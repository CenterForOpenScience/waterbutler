import hashlib
import functools

from swiftclient import Connection, quote
from swiftclient.utils import parse_api_response

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.swift.metadata import SwiftFileMetadata
from waterbutler.providers.swift.metadata import SwiftFolderMetadata
from waterbutler.providers.swift.metadata import SwiftFileMetadataHeaders
from waterbutler.providers.swift.metadata import resp_headers


class SwiftProvider(provider.BaseProvider):
    """Provider for Swift cloud storage service.
    """
    NAME = 'swift'

    def __init__(self, auth, credentials, settings):
        """
        :param dict auth: Not used
        :param dict credentials: Dict containing `username`, `password` and `tenant_name`
        :param dict settings: Dict containing `container`
        """
        super().__init__(auth, credentials, settings)

        auth_version = credentials['auth_version']
        if auth_version == '2':
            self.connection = Connection(auth_version='2',
                                         authurl=credentials['auth_url'],
                                         user=credentials['username'],
                                         key=credentials['password'],
                                         tenant_name=credentials['tenant_name'])
        elif auth_version == '3':
            os_options = {'user_domain_name': credentials['user_domain_name'],
                          'project_domain_name': credentials['project_domain_name'],
                          'project_name': credentials['tenant_name']}
            self.connection = Connection(auth_version='3',
                                         authurl=credentials['auth_url'],
                                         user=credentials['username'],
                                         key=credentials['password'],
                                         os_options=os_options)
        else:
            raise ValueError('Invalid auth version: {}'.format(auth_version))
        self.url = None
        self.token = None

        self.container = settings['container']

    @property
    def default_headers(self):
        if not self.url or not self.token:
            self.url, self.token = self.connection.get_auth()
        return {'X-Auth-Token': self.token}

    def generate_url(self, name=None):
        if not self.url or not self.token:
            self.url, self.token = self.connection.get_auth()
        if name is None:
            return '%s/%s' % (self.url, quote(self.container))
        else:
            return '%s/%s/%s' % (self.url, quote(self.container), quote(name))

    async def validate_v1_path(self, path, **kwargs):
        if path == '/':
            return WaterButlerPath(path)

        implicit_folder = path.endswith('/')

        assert path.startswith('/')
        if implicit_folder:
            resp = await self.make_request(
                'GET',
                self.generate_url,
                params={'format': 'json'},
                expects=(200, 404),
                throws=exceptions.MetadataError,
            )
            respbody = await resp.read()
            if resp.status == 404:
                raise exceptions.NotFoundError(str(path))
            objects = parse_api_response(resp_headers(resp.headers), respbody)
            if len(list(filter(lambda o: o['name'].startswith(path[1:]),
                               objects))) == 0:
                raise exceptions.NotFoundError(str(path))
        else:
            resp = await self.make_request(
                'HEAD',
                functools.partial(self.generate_url, path[1:]),
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
        url = functools.partial(self.generate_url, path.path)

        resp = await self.make_request(
            'GET',
            url,
            expects=(200, ),
            throws=exceptions.MetadataError,
        )

        return streams.ResponseStreamReader(resp)

    async def upload(self, stream, path, conflict='replace', **kwargs):
        """Uploads the given stream to Swift

        :param waterbutler.core.streams.RequestWrapper stream: The stream to put to Swift
        :param str path: The full path of the key to upload to/into

        :rtype: dict, bool
        """

        path, exists = await self.handle_name_conflict(path, conflict=conflict)
        stream.add_writer('md5', streams.HashStreamWriter(hashlib.md5))
        headers = {'Content-Length': str(stream.size)}

        assert not path.path.startswith('/')

        resp = await self.make_request(
            'PUT',
            functools.partial(self.generate_url, path.path),
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
            resp = await self.make_request(
                'DELETE',
                functools.partial(self.generate_url, path.path),
                expects=(200, 202, 204, 404),
                throws=exceptions.MetadataError,
            )
            await resp.release()
        else:
            await self._delete_folder(path, **kwargs)

    async def _delete_folder(self, path, **kwargs):
        resp = await self.make_request(
            'GET',
            self.generate_url,
            params={'format': 'json'},
            expects=(200, ),
            throws=exceptions.MetadataError,
        )
        respbody = await resp.read()
        objects = list(map(lambda o: (o['name'][len(path.path):], o),
                           filter(lambda o: o['name'].startswith(path.path),
                                  parse_api_response(resp_headers(resp.headers),
                                                     respbody))))
        if len(objects) == 0 and not path.is_root:
            raise exceptions.DeleteError('Not found', code=404)
        for name, obj in objects:
            resp = await self.make_request(
                'DELETE',
                functools.partial(self.generate_url, obj['name']),
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
                raise exceptions.FolderNamingConflict(str(path))
            if (await self.exists(await self.validate_path('/' + path.path[:-1]))):
                raise exceptions.FolderNamingConflict(str(path))

        resp = await self.make_request(
            'PUT',
            functools.partial(self.generate_url, path.path + '.osfkeep'),
            data='',
            skip_auto_headers={'CONTENT-TYPE'},
            expects=(200, 201, 202, ),
            throws=exceptions.CreateFolderError
        )
        await resp.release()

        return SwiftFolderMetadata({'prefix': path.path})

    async def _metadata_file(self, path, revision=None):
        if revision == 'Latest':
            revision = None
        assert not path.path.startswith('/')
        resp = await self.make_request(
            'HEAD',
            functools.partial(self.generate_url, path.path),
            expects=(200, ),
            throws=exceptions.MetadataError,
        )
        await resp.release()
        return SwiftFileMetadataHeaders(path.path, resp.headers)

    async def _metadata_folder(self, path):
        resp = await self.make_request(
            'GET',
            self.generate_url,
            params={'format': 'json'},
            expects=(200, ),
            throws=exceptions.MetadataError,
        )
        respbody = await resp.read()
        objects = list(map(lambda o: (o['name'][len(path.path):], o),
                           filter(lambda o: o['name'].startswith(path.path),
                                  parse_api_response(resp_headers(resp.headers),
                                                     respbody))))
        if len(objects) == 0 and not path.is_root:
            raise exceptions.MetadataError('Not found', code=404)

        contents = list(filter(lambda o: '/' not in o[0], objects))
        prefixes = sorted(set(map(lambda o: path.path + o[0][:o[0].index('/') + 1],
                                  filter(lambda o: '/' in o[0], objects))))

        items = [
            SwiftFolderMetadata({'prefix': item})
            for item in prefixes
        ]

        for content_path, content in contents:
            if content_path == path.path:
                continue

            fmetadata = SwiftFileMetadata(content)
            if fmetadata.name == '.osfkeep':
                continue
            items.append(fmetadata)

        return items
