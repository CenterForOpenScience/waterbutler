import http
import logging

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions

from waterbutler.providers.onedrive import settings
from waterbutler.providers.onedrive.path import OneDrivePath
from waterbutler.providers.onedrive.metadata import OneDriveFileMetadata
from waterbutler.providers.onedrive.metadata import OneDriveFolderMetadata
from waterbutler.providers.onedrive.metadata import OneDriveRevisionMetadata

logger = logging.getLogger(__name__)


class OneDriveProvider(provider.BaseProvider):
    """Provider for the Microsoft OneDrive cloud storage service.

    API docs: https://dev.onedrive.com/README.htm


    Special drives: https://dev.onedrive.com/resources/drive.htm#tasks-on-drive-resources


    API::

        Get folder contents:  If folder is root, api path is ``/drive/root/children``. If folder
        is not root, api path is  ``/drive/items/$item-id/children`.

    """
    NAME = 'onedrive'
    BASE_URL = settings.BASE_URL

    def __init__(self, auth, credentials, settings):
        super().__init__(auth, credentials, settings)
        self.token = self.credentials['token']
        self.folder = self.settings['folder']
        logger.info("__init__ credentials:{} settings:{}".format(
            repr(credentials), repr(settings)))

    @property
    def default_headers(self):
        """Set Authorization header with access token from auth provider.

        API docs: https://dev.onedrive.com/auth/msa_oauth.htm
        """
        return {'Authorization': 'bearer {}'.format(self.token)}

    async def validate_v1_path(self, path, **kwargs):
        if path == '/':
            return OneDrivePath(path, _ids=[self.folder])

        logger.info('validate_v1_path self::{} path::{}  url:{}'.format(
            repr(self), repr(path), self.build_url(path)))

        resp = await self.make_request(
            'GET', self.build_url(path),
            expects=(200, ),
            throws=exceptions.MetadataError
        )

        data = await resp.json()

        od_path = OneDrivePath(path)
        names = od_path.file_path(data)
        ids = od_path.ids(data)

        wb_path = OneDrivePath(names, _ids=ids, folder=path.endswith('/'))
        logger.info('wb_path::{}  IDs:{}'.format(repr(wb_path._parts), repr(ids)))
        return wb_path

    async def validate_path(self, path, **kwargs):
        logger.info('validate_path self::{} path::{}'.format(repr(self), path))
        return await self.validate_v1_path(path, **kwargs)

    async def revalidate_path(self, base, path, folder=None):
        logger.info('revalidate_path base::{} path::{}  base.id::{}'.format(
            base._prepend, path, base.identifier))
        logger.info('revalidate_path self::{} base::{} path::{}'.format(
            str(self), repr(base), repr(path)))
        logger.info('revalidate_path base::{} path::{}'.format(
            repr(base.full_path), repr(path)))

        od_path = OneDrivePath('/{}'.format(path))
        if (base.identifier is not None):
            url = self.build_url(base.identifier)
            resp = await self.make_request(
                'GET', url,
                expects=(200, ),
                throws=exceptions.MetadataError
            )
            data = await resp.json()
            folder_path = od_path.file_path(data)
            url = self._build_root_url("drive/root:", folder_path, str(path))
        elif (base._prepend is None):
            #  in a sub-folder, no need to get the root id
            url = self._build_root_url('drive/root:', base.full_path, str(path))
        else:
            #  root: get folder name and build path from it
            url = self.build_url(base._prepend)
            resp = await self.make_request(
                'GET', url,
                expects=(200, ),
                throws=exceptions.MetadataError
            )
            data = await resp.json()
            url = self._build_root_url("drive/root:", od_path.file_path(data), str(path))

        resp = await self.make_request(
            'GET',
            url,
            expects=(200, 404, ),
            throws=exceptions.ProviderError
        )

        if (resp.status == 404):
            ids = None
            folder = False
            await resp.release()
        else:
            data = await resp.json()
            ids = od_path.ids(data)[-1]
            folder = ('folder' in data.keys())

        return base.child(path, _id=ids, folder=folder)

    async def metadata(self, path, revision=None, **kwargs):
        """Fetch metadata for the file or folder identified by ``path``.

        API docs: https://dev.onedrive.com/items/get.htm

        :param OneDrivePath path:
        :param str revision: default ``None``
        :rtype: OneDriveMetadata
        :rtype: list(OneDriveFileMetadata|OneDriveFolderMetadata)
        :return: either a OneDriveFileMetada for a single file or an array of either
        ``OneDriveFileMetadata` or `OneDriveFolderMetadata` objects
        """
        logger.info('metadata identifier::{} path::{} revision::{}'.format(
            repr(path.identifier), repr(path), repr(revision)))

        if path.api_identifier is None:
            raise exceptions.NotFoundError(str(path))
        url = self.build_url(path.api_identifier, expand='children')

        logger.info("metadata url::{}".format(repr(url)))
        resp = await self.make_request(
            'GET',
            url,
            expects=(200, ),
            throws=exceptions.MetadataError
        )
        logger.info("metadata resp::{}".format(repr(resp)))

        data = await resp.json()
        logger.info("metadata data::{}".format(repr(data)))

        if data.get('deleted'):
            raise exceptions.MetadataError(
                "Could not retrieve {kind} '{path}'".format(
                    kind='folder' if data['folder'] else 'file',
                    path=path,
                ),
                code=http.client.NOT_FOUND,
            )

        return self._construct_metadata(data)

    async def revisions(self, path, **kwargs):
        """Get a list of revisions for the file identified by ``path``.

        API docs: https://dev.onedrive.com/items/view_delta.htm

        As of May 20, 2016: for files, the latest state is returned. There is not a list of changes
        for the file.

        :param OneDrivePath path: a `OneDrivePath` object representing the file to get revisions for
        :rtype: list(OneDriveRevisionMetadata)
        :return: a list of `OneDriveRevisionMetadata` objects
        """
        data = await self._revisions_json(path, **kwargs)
        logger.info('revisions: data::{}'.format(data['value']))

        return [
            OneDriveRevisionMetadata(item)
            for item in data['value']
            if not item.get('deleted')
        ]

    async def download(self, path, revision=None, range=None, **kwargs):
        """ OneDrive API Reference: https://dev.onedrive.com/items/download.htm """
        logger.info('folder:: {} revision::{} path.identifier:{} '
                    'path:{} path.parts:{}'.format(
                        self.folder, revision, path.identifier, repr(path), repr(path._parts)))

        if path.identifier is None:
            raise exceptions.DownloadError('"{}" not found'.format(str(path)), code=404)

        downloadUrl = None
        if revision:
            items = await self._revisions_json(path)
            for item in items['value']:
                if item['eTag'] == revision:
                    downloadUrl = item['@content.downloadUrl']
                    break
        else:
            url = self._build_content_url(path.identifier)
            logger.info('url::{}'.format(url))
            metaData = await self.make_request(
                'GET',
                url,
                expects=(200, ),
                throws=exceptions.MetadataError
            )
            data = await metaData.json()
            logger.info('data::{} downloadUrl::{}'.format(data, downloadUrl))
            downloadUrl = data['@content.downloadUrl']
        if downloadUrl is None:
            raise exceptions.NotFoundError(str(path))

        resp = await self.make_request(
            'GET',
            downloadUrl,
            range=range,
            expects=(200, 206),
            headers={'accept-encoding': ''},
            throws=exceptions.DownloadError,
        )

        return streams.ResponseStreamReader(resp)

    def can_duplicate_names(self):
        return False

    def can_intra_move(self, other, path=None):
        return False

    def can_intra_copy(self, other, path=None):
        return False

    async def upload(self, *args, **kwargs):
        raise exceptions.ReadOnlyProviderError(self.NAME)

    async def delete(self, *args, **kwargs):
        raise exceptions.ReadOnlyProviderError(self.NAME)

    async def move(self, *args, **kwargs):
        raise exceptions.ReadOnlyProviderError(self.NAME)

    # copy is okay if source is foobar and destination is not
    async def copy(self, dest_provider, *args, **kwargs):
        if dest_provider.NAME == self.NAME:
            raise exceptions.ReadOnlyProviderError(self.NAME)
        return await super().copy(dest_provider, *args, **kwargs)


    def _construct_metadata(self, data):
        if 'folder' in data.keys():
            ret = []
            if 'children' in data.keys():
                for item in data['children']:
                    if 'folder' in item.keys():
                        ret.append(OneDriveFolderMetadata(item, self.folder))
                    else:
                        ret.append(OneDriveFileMetadata(item, self.folder))
            return ret

        return OneDriveFileMetadata(data, self.folder)

    def _build_root_url(self, *segments, **query):
        return provider.build_url(settings.BASE_ROOT_URL, *segments, **query)

    def _build_content_url(self, *segments, **query):
        return provider.build_url(settings.BASE_CONTENT_URL, *segments, **query)

    async def _revisions_json(self, path, **kwargs):
        """

        API docs: https://dev.onedrive.com/items/view_delta.htm

        As of May 20, 2016: for files, the latest state is returned.
        There is not a list of changes for the file.

        :param OneDrivePath path: The path to create a folder at
        :rtype: `OneDriveFolderMetadata`
        :return: a `OneDriveFolderMetadata` object representing the new folder
        """
        if path.identifier is None:
            raise exceptions.NotFoundError(str(path))
        response = await self.make_request(
            'GET',
            self.build_url(path.identifier, 'view.delta', top=250),
            expects=(200, ),
            throws=exceptions.RevisionsError
        )
        data = await response.json()
        logger.info('revisions: data::{}'.format(data['value']))

        return data
