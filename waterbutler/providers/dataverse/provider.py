import hashlib
import logging
import tempfile
from typing import Tuple
from http import HTTPStatus

from aiohttp.helpers import BasicAuth

from waterbutler.core.utils import AsyncIterator
from waterbutler.core.path import WaterButlerPath
from waterbutler.core import exceptions, provider, streams

from waterbutler.providers.dataverse import settings as pd_settings
from waterbutler.providers.dataverse.metadata import (DataverseRevision,
                                                      DataverseDatasetMetadata, )

logger = logging.getLogger(__name__)


class DataverseProvider(provider.BaseProvider):
    """Provider for Dataverse

    Implementation is based on API v4.5 Docs: http://guides.dataverse.org/en/4.5/api/. The latest
    API version is v4.16 as of Oct. 2019. API v4.16 Docs: http://guides.dataverse.org/en/4.16/api/.
    In addition, here is the latest Docs link: http://guides.dataverse.org/en/latest/api/.

    About Dataverse API Sets

    Dataverse provides several sets of APIs and surprisingly WB touches three different sets for
    different actions. Each set behave differently in various aspects; thus it is recommended to
    treat them separately. For example as mentioned below, auth options are different. Please refer
    to the API Docs for details.

    * UPLOAD and DELETE: Dataverse SWORD API

      * v4.5 API Docs: http://guides.dataverse.org/en/4.5/api/sword.html
      * v4.16 API Docs: http://guides.dataverse.org/en/4.16/api/sword.html

    * DOWNLOAD: Dataverse Data Access API

      * v4.5 API Docs: http://guides.dataverse.org/en/4.5/api/dataaccess.html
      * v4.16 API Docs: http://guides.dataverse.org/en/4.16/api/dataaccess.html

    * METADATA: Dataverse Native API

      * v4.5 API Docs: http://guides.dataverse.org/en/4.5/api/native-api.html
      * v4.16 API Docs: http://guides.dataverse.org/en/4.16/api/native-api.html

    About Dataverse API Tokens and Authentication

    * Before v4.7, both Data Access API and Native API use 1) session auth (not eligible for 3rd-
      party applications such as WB) and 2) API key based auth which is what WB uses now. However,
      the latter option can only be use as a query param "key=", which isn't safe at all. Starting
      v4.7, a new and more secure approach is available to 3rd-parties which uses a dedicated auth
      header "X-Dataverse-key" whose value should be set as the API key.

    * In comparision, Dataverse SWORD API only supports basic auth. To make it more interesting,
      it seems that this "support" was created specifically for using the API key as if it were
      username and password. The trick here is that the API key must be provided as the username
      and the password must be set to empty.

    * TODO: update DOWNLOAD and METADATA to use header auth instead of query param auth.

    About Dataverse DOWNLOAD

    * Dataverse doesn't respect Range header on downloads (as of v4.16, Oct. 2019)

    About Basic Auth

    * ``aiohttp-v0.18`` shows a deprecation warning that the ``auth`` parameter should be of type
      ``aiohttp.helpersBasicAuth`` when building the client request object. This is why we could
      pass a tuple ``auth=(self.token, )`` when calling ``self.make_request()``. See:
      https://github.com/aio-libs/aiohttp/blob/v0.18.4/aiohttp/client_reqrep.py#L249-L252

    * However, in ``aiohttp-v3.5`` (more specifically starting v0.22), the request initialization
      will throw an error if the ``auth`` parameter is not of type ``aiohttp.helpersBasicAuth``.
      Thus, must use the basic auth object ``auth=BasicAuth(self.token)`` instead. See:
      https://github.com/aio-libs/aiohttp/blob/3.5/aiohttp/client_reqrep.py#L468-L469

    * For more info about ``aiohttp.helpersBasicAuth`` as of version 3.5, see:
      https://github.com/aio-libs/aiohttp/blob/3.5/aiohttp/helpers.py#L116-L176
    """

    NAME = 'dataverse'

    def __init__(self, auth, credentials, settings, **kwargs):
        """
        :param dict auth: Not used
        :param dict credentials: Contains `token`
        :param dict settings: Contains `host`, `doi`, `id`, and `name` of a dataset. Hosts::

            - 'demo.dataverse.org': Harvard Demo Server
            - 'dataverse.harvard.edu': Dataverse Production Server **(NO TEST DATA)**
            - Other
        """
        super().__init__(auth, credentials, settings, **kwargs)
        self.BASE_URL = 'https://{0}'.format(self.settings['host'])

        self.token = self.credentials['token']
        self.doi = self.settings['doi']
        self._id = self.settings['id']
        self.name = self.settings['name']
        self.metrics.add('host', {
            'host': self.settings['host'],
            'doi': self.doi,
            'name': self.name,
            'id': self._id,
        })

        self._metadata_cache = {}

    def build_url(self, path, *segments, **query):
        # Need to split up the dataverse subpaths and push them into segments
        return super().build_url(*(tuple(path.split('/')) + segments), **query)

    def can_duplicate_names(self):
        return False

    async def validate_v1_path(self, path, **kwargs):
        if path != '/' and path.endswith('/'):
            raise exceptions.NotFoundError(str(path))

        return await self.validate_path(path, **kwargs)

    async def validate_path(self, path, revision=None, **kwargs):
        """Ensure path is in configured dataset

        :param str path: The path to a file
        :param list metadata: List of file metadata from _get_data
        """
        self.metrics.add('validate_path.revision', revision)
        if path == '/':
            wbpath = WaterButlerPath('/')
            wbpath.revision = revision
            return wbpath

        path = path.strip('/')

        wbpath = None
        for item in (await self._maybe_fetch_metadata(version=revision)):
            if path == item.extra['fileId']:
                wbpath = WaterButlerPath('/' + item.name, _ids=(None, item.extra['fileId']))
        wbpath = wbpath or WaterButlerPath('/' + path)

        wbpath.revision = revision
        return wbpath

    async def revalidate_path(self, base, path, folder=False, revision=None):
        path = path.strip('/')

        wbpath = None
        for item in (await self._maybe_fetch_metadata(version=revision)):
            if path == item.name:
                # Dataverse cant have folders
                wbpath = base.child(item.name, _id=item.extra['fileId'], folder=False)
        wbpath = wbpath or base.child(path, _id=None, folder=False)

        wbpath.revision = revision or base.revision
        return wbpath

    async def _maybe_fetch_metadata(self, version=None, refresh=False):
        if refresh or self._metadata_cache.get(version) is None:
            for v in ((version, ) or ('latest', 'latest-published')):
                self._metadata_cache[v] = await self._get_data(v)
        if version:
            return self._metadata_cache[version]
        return sum(self._metadata_cache.values(), [])

    async def download(self, path: WaterButlerPath, revision: str=None,  # type: ignore
                       range: Tuple[int, int] = None, **kwargs) -> streams.ResponseStreamReader:
        r"""Returns a ResponseWrapper (Stream) for the specified path
        raises FileNotFoundError if the status from Dataverse is not 200

        :param WaterButlerPath path: Path to the file you want to download
        :param str revision: Used to verify if file is in selected dataset
                - 'latest' to check draft files
                - 'latest-published' to check published files
                - None to check all data
        :param Tuple[int, int] range: the range header
        :param dict \*\*kwargs: Additional arguments that are ignored
        :rtype: :class:`waterbutler.core.streams.ResponseStreamReader`
        :raises: :class:`waterbutler.core.exceptions.DownloadError`
        """
        if path.identifier is None:
            raise exceptions.NotFoundError(str(path))

        logger.debug('request-range:: {}'.format(range))
        # TODO: use the auth header "X-Dataverse-key" instead of query param (1/2)
        resp = await self.make_request(
            'GET',
            self.build_url(pd_settings.DOWN_BASE_URL, path.identifier, key=self.token),
            range=range,
            expects=(200, 206),
            throws=exceptions.DownloadError,
        )
        return streams.ResponseStreamReader(resp)

    async def upload(self, stream, path, **kwargs):
        """Zips the given stream then uploads to Dataverse.
        This will delete existing draft files with the same name.

        :param waterbutler.core.streams.RequestWrapper stream: The stream to put to Dataverse
        :param str path: The filename prepended with '/'

        :rtype: dict, bool
        """

        stream.add_writer('md5', streams.HashStreamWriter(hashlib.md5))

        zip_stream = streams.ZipStreamReader(AsyncIterator([(path.name, stream)]))

        # Write stream to disk (Necessary to find zip file size)
        f = tempfile.TemporaryFile()
        chunk = await zip_stream.read()
        while chunk:
            f.write(chunk)
            chunk = await zip_stream.read()
        file_stream = streams.FileStreamReader(f)

        dv_headers = {
            "Content-Disposition": "filename=temp.zip",
            "Content-Type": "application/zip",
            "Packaging": "http://purl.org/net/sword/package/SimpleZip",
            "Content-Length": str(file_stream.size),
        }

        # Delete old file if it exists
        if path.identifier:
            await self.delete(path)

        resp = await self.make_request(
            'POST',
            self.build_url(pd_settings.EDIT_MEDIA_BASE_URL, 'study', self.doi),
            headers=dv_headers,
            auth=BasicAuth(self.token),
            data=file_stream,
            expects=(201, ),
            throws=exceptions.UploadError
        )
        await resp.release()

        # Find appropriate version of file
        metadata = await self._get_data('latest')
        files = metadata if isinstance(metadata, list) else []
        file_metadata = next(file for file in files if file.name == path.name)

        if stream.writers['md5'].hexdigest != file_metadata.extra['hashes']['md5']:
            raise exceptions.UploadChecksumMismatchError()

        return file_metadata, path.identifier is None

    async def delete(self, path, **kwargs):
        """Deletes the key at the specified path

        :param str path: The path of the key to delete
        """
        # Can only delete files in draft
        path = await self.validate_path('/' + path.identifier, version='latest', throw=True)

        resp = await self.make_request(
            'DELETE',
            self.build_url(pd_settings.EDIT_MEDIA_BASE_URL, 'file', path.identifier),
            auth=BasicAuth(self.token),
            expects=(204, ),
            throws=exceptions.DeleteError,
        )
        await resp.release()

    async def metadata(self, path, version=None, **kwargs):
        """
        :param str version:

            - 'latest' for draft files
            - 'latest-published' for published files
            - None for all data
        """
        version = version or path.revision

        if path.is_root:
            return (await self._maybe_fetch_metadata(version=version))

        try:
            return next(
                item
                for item in
                (await self._maybe_fetch_metadata(version=version))
                if item.extra['fileId'] == path.identifier
            )
        except StopIteration:
            raise exceptions.MetadataError(
                "Could not retrieve file '{}'".format(path),
                code=HTTPStatus.NOT_FOUND,
            )

    async def revisions(self, path, **kwargs):
        """Get past versions of the request file. Orders versions based on
        `_get_all_data()`

        :param str path: The path to a key
        :rtype list:
        """

        metadata = await self._get_data()
        return [
            DataverseRevision(item.extra['datasetVersion'])
            for item in metadata if item.extra['fileId'] == path.identifier
        ]

    async def _get_data(self, version=None):
        """Get list of file metadata for a given dataset version

        :param str version:

            - 'latest' for draft files
            - 'latest-published' for published files
            - None for all data
        """

        if not version:
            return (await self._get_all_data())

        # TODO: use the auth header "X-Dataverse-key" instead of query param (2/2)
        url = self.build_url(
            pd_settings.JSON_BASE_URL.format(self._id, version),
            key=self.token,
        )
        resp = await self.make_request(
            'GET',
            url,
            expects=(200, ),
            throws=exceptions.MetadataError
        )

        data = await resp.json()
        data = data['data']

        dataset_metadata = DataverseDatasetMetadata(
            data, self.name, self.doi, version,
        )

        return [item for item in dataset_metadata.contents]

    async def _get_all_data(self):
        """Get list of file metadata for all dataset versions"""
        try:
            published_data = await self._get_data('latest-published')
        except exceptions.MetadataError as e:
            if e.code != 404:
                raise
            published_data = []
        draft_data = await self._get_data('latest')

        # Prefer published to guarantee users get published version by default
        return published_data + draft_data
