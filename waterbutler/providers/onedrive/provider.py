import http
import json
import asyncio
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
            expects=(200, 400),
            throws=exceptions.MetadataError
        )

        if resp.status == 400:
            await resp.release()
            return OneDrivePath(path, _ids=[self.folder, '' ])

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

    def can_intra_copy(self, dest_provider, path=None):
        return type(self) == type(dest_provider)

    def can_intra_move(self, dest_provider, path=None):
        return self == dest_provider

    async def upload(self, stream, path, conflict='replace', **kwargs):
        """ OneDrive API Reference: https://dev.onedrive.com/items/upload_put.htm
            Limited to 100MB file upload. """
        path, exists = await self.handle_name_conflict(path, conflict=conflict)

        logger.info("upload path:{} path.parent:{} path.path:{} self:{}".format(
            repr(path), path.parent, path.full_path, repr(self))
        )

        od_path = OneDrivePath(str(path))
        upload_url = self.build_url(
            od_path.one_drive_parent_folder(path), 'children', path.name, 'content'
        )

        logger.info("upload url:{} path:{} str(path):{} str(full_path):{} "
                    "self:{}".format(upload_url, repr(path), str(path),
                                     str(path), repr(self.folder)))

        resp = await self.make_request(
            'PUT',
            upload_url,
            headers={'Content-Length': str(stream.size)},
            data=stream,
            expects=(201, ),
            throws=exceptions.UploadError,
        )

        data = await resp.json()
        logger.info('upload:: data:{}'.format(data))
        return OneDriveFileMetadata(data, self.folder), not exists

    async def delete(self, path, **kwargs):
        """Delete a file or folder from OneDrive.

        API docs: https://dev.onedrive.com/items/delete.htm

        :param path OneDrivePath: a OneDrivePath representing the path to be deleted
        :return: None
        """
        resp = await self.make_request(
            'DELETE',
            self.build_url(path.identifier),
            data={},
            expects=(204, ),
            throws=exceptions.DeleteError,
        )
        await resp.release()

    # async def copy():  use default implementation

    # async def move():  use default implementation

    async def intra_copy(self, dest_provider, src_path, dest_path):
        """Copy a file or folder within a OneDrive provider.

        API docs: https://dev.onedrive.com/items/copy.htm
        """

        url = self.build_url(src_path.identifier, 'action.copy')

        payload = json.dumps({
            'name': dest_path.name,
            'parentReference': {'id': dest_path.parent_id()}
        })

        logger.info(
            'intra_copy dest_provider::{} src_path::{} dest_path::{} '
            'url::{} payload::{}'.format(
                repr(dest_provider), repr(src_path), repr(dest_path),
                repr(url), payload
            )
        )
        resp = await self.make_request(
            'POST',
            url,
            data=payload,
            headers={'content-type': 'application/json', 'Prefer': 'respond-async'},
            expects=(202, ),
            throws=exceptions.IntraCopyError,
        )
        if resp is None:
            await resp.release()
            raise exceptions.IntraCopyError
        logger.info('resp::{}'.format(repr(resp)))
        status_url = resp.headers['LOCATION']
        await resp.release()
        logger.info('status_url::{}'.format(repr(status_url)))

        i = 0
        status = None
        while (i < settings.ONEDRIVE_COPY_ITERATION_COUNT):
            logger.info('status::{}  i:{}'.format(repr(status), i))
            status = await self._copy_status(status_url)
            if (status is not None):
                break
            await asyncio.sleep(settings.ONEDRIVE_COPY_SLEEP_INTERVAL)
            i += 1

        if i >= settings.ONEDRIVE_COPY_ITERATION_COUNT:
            raise exceptions.CopyError('OneDrive API file copy has not responded in a timely '
                                       'manner.  Please wait for 1-2 minutes, then query for '
                                       'the file to see if the copy has completed',
                                       code=202)

        metadata = self._construct_metadata(status)
        return metadata, dest_path.identifier is None

    async def intra_move(self, dest_provider, src_path, dest_path):
        """Move a file or folder within a OneDrive container. Used when renaming a file or folder
        or moving a file/folder within a single drive.

        API docs: https://dev.onedrive.com/items/move.htm

        :param dest_provider:
        :param OneDrivePath src_path:
        :param OneDrivePath dest_path:
        :rtype: (`OneDriveFileMetadata`, bool)
        :rtype: (`OneDriveFolderMetadata`, bool)
        :return: (``OneDriveFileMetadata``|``OneDriveFolderMetadata``, <was created>)
        """

        parentReference = {'id': dest_path.parent_id()}
        url = self.build_url(src_path.identifier)

        payload = json.dumps({'name': dest_path.name,
                              'parentReference': parentReference})

        logger.info('intra_move dest_path::{} src_path::{} url::{} '
                    'payload:{}'.format(
                        str(dest_path.parent.identifier), repr(src_path),
                        url, payload))

        try:
            resp = await self.make_request(
                'PATCH',
                url,
                data=payload,
                headers={'content-type': 'application/json'},
                expects=(200, ),
                throws=exceptions.IntraMoveError,
            )
        except exceptions.IntraMoveError:
            raise

        data = await resp.json()

        logger.info('intra_move data:{}'.format(data))

        if 'folder' not in data.keys():
            return OneDriveFileMetadata(data, self.folder), True

        folder = OneDriveFolderMetadata(data, self.folder)

        return folder, dest_path.identifier is None

    async def create_folder(self, path, **kwargs):
        """Create the folder defined by ``path``.

        API docs: https://dev.onedrive.com/items/create.htm

        :param OneDrivePath path: The path to create a folder at
        :rtype: `OneDriveFolderMetadata`
        :return: a `OneDriveFolderMetadata` object representing the new folder
        """
        OneDrivePath.validate_folder(path)
        od_path = OneDrivePath(str(path))

        #  OneDrive's root folder has a different alias between create
        #  folder and at least upload; need to strip out :
        if path._prepend == '0':
            upload_url = self._build_root_url(
                od_path.one_drive_parent_folder(path).replace(':', ''),
                'children'
            )
        else:
            upload_url = self.build_url(
                od_path.one_drive_parent_folder(path),
                'children'
            )

        logger.info("upload url:{} path:{} folderName:{}".format(upload_url, repr(path), repr(path.name)))
        payload = {'name': path.name,
                   'folder': {},
                    "@name.conflictBehavior": "rename"}

        resp = await self.make_request(
            'POST',
            upload_url,
            data=json.dumps(payload),
            headers={'content-type': 'application/json'},
            expects=(201, ),
            throws=exceptions.CreateFolderError,
        )

        data = await resp.json()
        logger.info('upload:: data:{}'.format(data))
        return OneDriveFolderMetadata(data, self.folder)

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

    async def _copy_status(self, status_url):
        """ OneDrive API Reference: https://dev.onedrive.com/resources/asyncJobStatus.htm """
        resp = await self.make_request(
            'GET', status_url,
            expects=(200, 202),
            throws=exceptions.IntraCopyError,
        )
        data = await resp.json()
        status = data.get('status')
        logger.info('_copy_status  status::{} resp:{} data::{}'.format(repr(status), repr(resp), repr(data)))
        return data if resp.status == 200 else None
