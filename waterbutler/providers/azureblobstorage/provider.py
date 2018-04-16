import hashlib
import asyncio
import aiohttp
import functools
from urllib.parse import urlparse
from urllib.parse import quote as url_quote
import uuid

from azure.storage.blob import BlockBlobService
from azure.storage._serialization import _add_date_header
from azure.storage.blob._serialization import _get_path
from azure.storage.blob._deserialization import (
    _convert_xml_to_blob_list,
    _parse_blob
)
from azure.storage._constants import (
    X_MS_VERSION,
    USER_AGENT_STRING,
)

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.azureblobstorage.metadata import AzureBlobStorageFileMetadata
from waterbutler.providers.azureblobstorage.metadata import AzureBlobStorageFolderMetadata
from waterbutler.providers.azureblobstorage.metadata import AzureBlobStorageFileMetadataHeaders


class _Request(object):

    def __init__(self, method, url, params, headers):
        self.method = method
        urlo = urlparse(url)
        self.path = urlo.path
        self.host = urlo.netloc
        self.query = params
        self.headers = headers
        self.body = None


class _ResponseBody(object):

    def __init__(self, resp, body):
        self.headers = dict(map(lambda h: (h[0].lower(), h[1]), resp.headers.items()))
        self.body = body


def _update_request(request):
    # append addtional headers based on the service
    request.headers['x-ms-version'] = X_MS_VERSION
    request.headers['User-Agent'] = USER_AGENT_STRING
    request.headers['x-ms-client-request-id'] = str(uuid.uuid1())

    # If the host has a path component (ex local storage), move it
    path = request.host.split('/', 1)
    if len(path) == 2:
        request.host = path[0]
        request.path = '/{}{}'.format(path[1], request.path)

    # Encode and optionally add local storage prefix to path
    request.path = url_quote(request.path, '/()$=\',~')


class AzureBlobStorageProvider(provider.BaseProvider):
    """Provider for Azure Blob Storage cloud storage service.
    """
    NAME = 'azureblobstorage'

    def __init__(self, auth, credentials, settings):
        """
        :param dict auth: Not used
        :param dict credentials: Dict containing `username`, `password` and `tenant_name`
        :param dict settings: Dict containing `container`
        """
        super().__init__(auth, credentials, settings)

        self.connection = BlockBlobService(account_name=credentials['account_name'],
                                           account_key=credentials['account_key'])

        self.container = settings['container']

    def _get_host_locations(self, primary=True, secondary=False):
        locations = []
        if primary:
            locations.append(self.connection.primary_endpoint)
        if secondary:
            locations.append(self.connection.secondary_endpoint)
        return locations

    def generate_urls(self, blob_name=None, primary=True, secondary=False):
        if blob_name is None:
            path = _get_path(self.container)
        else:
            path = _get_path(self.container, blob_name)
        hosts = self._get_host_locations(primary, secondary)
        return list(map(lambda h: "https://" + h + path, hosts))

    @provider.throttle()
    async def make_signed_request(self, method, urls, *args, **kwargs):
        kwargs['headers'] = self.build_headers(**kwargs.get('headers', {}))
        retry = _retry = kwargs.pop('retry', 2)
        range = kwargs.pop('range', None)
        expects = kwargs.pop('expects', None)
        throws = kwargs.pop('throws', exceptions.ProviderError)
        if range:
            kwargs['headers']['Range'] = self._build_range_header(range)

        if callable(urls):
            urls = urls()
        httpreq = _Request(method, urls[0], kwargs.get('params', {}), kwargs['headers'])
        _update_request(httpreq)
        _add_date_header(httpreq)
        self.connection.authentication.sign_request(httpreq)

        target_url = 0
        while retry >= 0:
            try:
                response = await aiohttp.request(method, urls[target_url % len(urls)], *args, **kwargs)
                if expects and response.status not in expects:
                    raise (await exceptions.exception_from_response(response, error=throws, **kwargs))
                return response
            except throws as e:
                if retry <= 0 or e.code not in self._retry_on:
                    raise
                await asyncio.sleep((1 + _retry - retry) * 2)
                retry -= 1
                target_url += 1

    async def validate_v1_path(self, path, **kwargs):
        if path == '/':
            return WaterButlerPath(path)

        implicit_folder = path.endswith('/')

        assert path.startswith('/')
        if implicit_folder:
            resp = await self.make_signed_request(
                'GET',
                functools.partial(self.generate_urls, secondary=True),
                params={'restype': 'container', 'comp': 'list'},
                expects=(200, 404),
                throws=exceptions.MetadataError,
            )
            respbody = await resp.read()
            if resp.status == 404:
                raise exceptions.NotFoundError(str(path))
            objects = _convert_xml_to_blob_list(_ResponseBody(resp, respbody))
            if len(list(filter(lambda o: o.name.startswith(path[1:]),
                               objects))) == 0:
                raise exceptions.NotFoundError(str(path))
        else:
            resp = await self.make_signed_request(
                'HEAD',
                functools.partial(self.generate_urls, path[1:], secondary=True),
                expects=(200, 404),
                throws=exceptions.MetadataError,
            )
            await resp.release()
            if resp.status == 404:
                raise exceptions.NotFoundError(str(path))

        return WaterButlerPath(path)

    async def validate_path(self, path, **kwargs):
        return WaterButlerPath(path)

    def can_duplicate_names(self):
        return True

    def can_intra_copy(self, dest_provider, path=None):
        # Not supported
        return False

    def can_intra_move(self, dest_provider, path=None):
        # Not supported
        return False

    async def intra_copy(self, dest_provider, source_path, dest_path):
        # Not supported
        raise NotImplementedError()

    async def download(self, path, accept_url=False, version=None, range=None, **kwargs):
        """
        :param str path: Path to the key you want to download
        :param dict \*\*kwargs: Additional arguments that are ignored
        :rtype: :class:`waterbutler.core.streams.ResponseStreamReader`
        :raises: :class:`waterbutler.core.exceptions.DownloadError`
        """

        if not path.is_file:
            raise exceptions.DownloadError('No file specified for download', code=400)

        assert not path.path.startswith('/')
        urls = functools.partial(self.generate_urls, path.path, secondary=True)

        resp = await self.make_signed_request(
            'GET',
            urls,
            expects=(200, ),
            throws=exceptions.MetadataError,
        )

        return streams.ResponseStreamReader(resp)

    async def upload(self, stream, path, conflict='replace', **kwargs):
        """Uploads the given stream to Azure Blob Storage

        :param waterbutler.core.streams.RequestWrapper stream: The stream to put to Azure Blob Storage
        :param str path: The full path of the key to upload to/into

        :rtype: dict, bool
        """

        path, exists = await self.handle_name_conflict(path, conflict=conflict)
        stream.add_writer('md5', streams.HashStreamWriter(hashlib.md5))
        headers = {'Content-Length': str(stream.size), 'x-ms-blob-type': 'BlockBlob'}

        assert not path.path.startswith('/')

        resp = await self.make_signed_request(
            'PUT',
            functools.partial(self.generate_urls, path.path),
            data=stream,
            headers=headers,
            skip_auto_headers={'CONTENT-TYPE'},
            expects=(200, 201, 202, ),
            throws=exceptions.UploadError,
        )
        await resp.release()

        return (await self.metadata(path, **kwargs)), not exists

    async def delete(self, path, confirm_delete=0, **kwargs):
        """Deletes the key at the specified path

        :param str path: The path of the key to delete
        :param int confirm_delete: Must be 1 to confirm root folder delete
        """

        if path.is_root:
            if not confirm_delete == 1:
                raise exceptions.DeleteError(
                    'confirm_delete=1 is required for deleting root provider folder',
                    code=400
                )

        if path.is_file:
            assert not path.path.startswith('/')
            resp = await self.make_signed_request(
                'DELETE',
                functools.partial(self.generate_urls, path.path),
                expects=(200, 202, 204),
                throws=exceptions.MetadataError,
            )
            await resp.release()
        else:
            await self._delete_folder(path, **kwargs)

    async def _delete_folder(self, path, **kwargs):
        resp = await self.make_signed_request(
            'GET',
            functools.partial(self.generate_urls, secondary=True),
            params={'restype': 'container', 'comp': 'list'},
            expects=(200, ),
            throws=exceptions.MetadataError,
        )
        respbody = await resp.read()
        objects = _convert_xml_to_blob_list(_ResponseBody(resp, respbody))
        objects = list(map(lambda o: (o.name[len(path.path):], o),
                           filter(lambda o: o.name.startswith(path.path),
                                  objects)))
        if len(objects) == 0 and not path.is_root:
            raise exceptions.DeleteError('Not found', code=404)
        for name, blob in objects:
            resp = await self.make_signed_request(
                'DELETE',
                functools.partial(self.generate_urls, blob.name),
                expects=(200, 202, 204, 404),
                throws=exceptions.MetadataError,
            )
            await resp.release()

    async def revisions(self, path, **kwargs):
        """Get past versions of the requested key

        :param str path: The path to a key
        :rtype list:
        """
        return []

    async def metadata(self, path, revision=None, **kwargs):
        """Get Metadata about the requested file or folder

        :param WaterButlerPath path: The path to a key or folder
        :rtype: dict or list
        """
        if path.is_dir:
            return (await self._metadata_folder(path))

        return (await self._metadata_file(path, revision=revision))

    async def create_folder(self, path, folder_precheck=True, **kwargs):
        """
        :param str path: The path to create a folder at
        """

        WaterButlerPath.validate_folder(path)

        if folder_precheck:
            if (await self.exists(path)):
                raise exceptions.FolderNamingConflict(path.name)
            if (await self.exists(await self.validate_path('/' + path.path[:-1]))):
                raise exceptions.FolderNamingConflict(path.name)

        headers = {'x-ms-blob-type': 'BlockBlob'}
        resp = await self.make_signed_request(
            'PUT',
            functools.partial(self.generate_urls, path.path + '.osfkeep'),
            data='',
            headers=headers,
            skip_auto_headers={'CONTENT-TYPE'},
            expects=(200, 201, 202, ),
            throws=exceptions.CreateFolderError
        )
        await resp.release()

        return AzureBlobStorageFolderMetadata({'prefix': path.path})

    async def _metadata_file(self, path, revision=None):
        if revision == 'Latest':
            revision = None
        assert not path.path.startswith('/')
        resp = await self.make_signed_request(
            'HEAD',
            functools.partial(self.generate_urls, path.path, secondary=True),
            expects=(200, ),
            throws=exceptions.MetadataError,
        )
        await resp.release()
        blob = _parse_blob(_ResponseBody(resp, b''), path.path, None)
        return AzureBlobStorageFileMetadataHeaders(path.path, blob)

    async def _metadata_folder(self, path):
        resp = await self.make_signed_request(
            'GET',
            functools.partial(self.generate_urls, secondary=True),
            params={'restype': 'container', 'comp': 'list'},
            expects=(200, ),
            throws=exceptions.MetadataError,
        )
        respbody = await resp.read()
        objects = _convert_xml_to_blob_list(_ResponseBody(resp, respbody))
        objects = list(map(lambda o: (o.name[len(path.path):], o),
                           filter(lambda o: o.name.startswith(path.path),
                                  objects)))
        if len(objects) == 0 and not path.is_root:
            raise exceptions.MetadataError('Not found', code=404)

        contents = list(filter(lambda o: '/' not in o[0], objects))
        prefixes = sorted(set(map(lambda o: path.path + o[0][:o[0].index('/') + 1],
                                  filter(lambda o: '/' in o[0], objects))))

        items = [
            AzureBlobStorageFolderMetadata({'prefix': item})
            for item in prefixes
        ]

        for content_path, content in contents:
            if content_path == path.path:
                continue
            fmetadata = AzureBlobStorageFileMetadata(content)
            if fmetadata.name == '.osfkeep':
                continue
            items.append(fmetadata)

        return items
