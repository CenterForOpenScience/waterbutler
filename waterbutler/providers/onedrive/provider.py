import json
import http
import asyncio

import logging

from urllib.parse import urlparse

from itertools import repeat

from waterbutler.core import streams
from waterbutler.core import path
from waterbutler.core import provider
from waterbutler.core import exceptions

from waterbutler.providers.onedrive import settings
from waterbutler.providers.onedrive.metadata import OneDriveRevision
from waterbutler.providers.onedrive.metadata import OneDriveFileMetadata
from waterbutler.providers.onedrive.metadata import OneDriveFolderMetadata

logger = logging.getLogger(__name__)


class OneDrivePath(path.WaterButlerPath):
    """OneDrive specific WaterButlerPath class to handle some of the idiosyncrasies of
    file paths in OneDrive."""

    def update_from_response(self, data):
        """
        Update current path - identifier, path name
        :param dict data: OneDrive API response
        """
        if not self.is_root:
            self._parts[-1] = self.PART_CLASS(data['name'], _id=data['id'])

    @classmethod
    def from_response(cls, data, prepend=None):
        """
        Create OneDrivePath from OneDrive API response
        :param dict data: OneDrive API response
        :param str prepend: relative path to OneDrive root drive
        :return: OneDrivePath
        """
        raw_path = cls.file_path(data, prepend=prepend)
        if not raw_path:
            # system root path
            raw_path = '/'
            return cls(raw_path, _ids=(data['id'],), prepend=prepend)

        url_segment_count = len(urlparse(raw_path).path.split('/'))
        ids = [data['parentReference']['id'], data['id']]
        if len(ids) < url_segment_count:
            for x in repeat(None, url_segment_count - len(ids)):
                ids.insert(0, x)

        return cls(raw_path, _ids=ids, folder=bool(data.get('folder', None)), prepend=prepend)

    @classmethod
    def file_path(cls, data, prepend=None):
        parent_path = data['parentReference']['path'].replace('/drive/root:', '')
        # empty
        if not parent_path:
            path = '/{}'.format(data['name'])
        else:
            path = '{}/{}'.format(parent_path, data['name'])

        if prepend is not None:
            assert path.startswith(prepend)
            path = path.replace(prepend, '')

        return path


class OneDriveProvider(provider.BaseProvider):
    """Provider for the OneDrive cloud storage service.

    API docs: https://dev.onedrive.com/README.htm
    """
    NAME = 'onedrive'
    BASE_URL = settings.BASE_URL

    def __init__(self, auth, credentials, settings):
        super().__init__(auth, credentials, settings)
        self.token = self.credentials['token']
        self.folder = self.settings['folder']

    def has_real_root(self):
        return self.folder == '0'

    async def validate_v1_path(self, path, **kwargs):
        if path == '/' and self.has_real_root():
            return OneDrivePath(path, _ids=(self.folder.strip('/'),), folder=True)

        prepend = None
        if not self.has_real_root():
            async with self.request(
                    'GET',
                    self._build_content_url(self.folder.strip('/')),
                    expects=(200,),
                    throws=exceptions.MetadataError
            ) as resp:
                data = await resp.json()
                prepend = OneDrivePath.file_path(data)

        async with self.request(
                'GET',
                self._build_root_url('/drive/root:', prepend or '', path),
                expects=(200,),
                throws=exceptions.MetadataError
        ) as resp:

            data = await resp.json()

            od_path = OneDrivePath.from_response(data, prepend)
            logger.info('wb_path::{}  IDs:'.format(repr(od_path._parts)))
            return od_path

    async def validate_path(self, path, **kwargs):
        return await self.validate_v1_path(path, **kwargs)

    async def revalidate_path(self, base, path, folder=False):
        """Take a path and a base path and build a WaterButlerPath representing `/base/path`.  For
        id-based providers, this will need to lookup the id of the new child object.

        :param WaterButlerPath base: The base folder to look under
        :param str path: the path of a child of `base`, relative to `base`
        :param bool folder: whether the returned WaterButlerPath should represent a folder
        :rtype: WaterButlerPath
        """
        assert isinstance(base, OneDrivePath), 'Base path should be validated'
        assert base.identifier, 'Base path should be validated'
        async with self.request(
                'GET',
                self._build_root_url('/drive/root:', base.full_path, path),
                expects=(200, 404),
                throws=exceptions.MetadataError
        ) as resp:
            # @todo can't raise exception if path already exists
            path_id = None
            is_folder = folder
            if resp.status != 404:
                data = await resp.json()
                path_id = data['id']
                is_folder = bool(data.get('folder', None))

            return base.child(path, _id=path_id, folder=is_folder)

    @property
    def default_headers(self):
        return {
            'Authorization': 'Bearer {}'.format(self.token),
        }

    async def intra_copy(self, dest_provider, src_path, dest_path):
        """ OneDrive API Reference: https://dev.onedrive.com/items/copy.htm """

        dest_exist = dest_path.identifier is not None

        if dest_path.parent.is_root and self.has_real_root():
            parent_reference = {
                'path': '/drive/root:/'
            }
        else:
            parent_reference = {
                'id': dest_path.parent.identifier
            }

        payload = {'name': dest_path.name,
                   'parentReference': parent_reference}
        url = self.build_url(src_path.identifier, 'action.copy')

        logger.info(
            'intra_copy dest_provider::{} src_path::{} dest_path::{}  url::{} payload::{}'.format(repr(dest_provider),
                                                                                                  repr(src_path),
                                                                                                  repr(dest_path),
                                                                                                  repr(url), payload))
        async with self.request(
                'POST',
                url,
                data=json.dumps(payload),
                headers={'content-type': 'application/json', 'Prefer': 'respond-async'},
                expects=(202,),
                throws=exceptions.IntraCopyError,
        ) as resp:
            logger.info('resp::{}'.format(repr(resp)))
            status_url = resp.headers['LOCATION']
            logger.info('status_url::{}'.format(repr(status_url)))

            try:
                data = await asyncio.wait_for(self._wait_for_async_job(status_url,
                                                                       throws=exceptions.IntraCopyError),
                                              settings.ONEDRIVE_COPY_REQUEST_TIMEOUT or None
                                              )
            except asyncio.TimeoutError:
                raise exceptions.CopyError(
                    """OneDrive API file copy has not responded in a timely manner.  Please wait for 1-2 minutes,
                     then query for the file to see if the copy has completed""",
                    code=202)

            dest_path.update_from_response(data)

            return self._construct_metadata(data, dest_path), not dest_exist

    async def _wait_for_async_job(self, url, throws=exceptions.ProviderError):
        """
        OneDrive API Reference: https://dev.onedrive.com/resources/asyncJobStatus.htm
        :param str url: Status URL for Async Operation
        :param Exception throws: The exception to be raised from expects
        :return dict: Response json content
        """
        while True:
            async with self.request(
                'GET',
                url,
                expects=(200, 202),
                throws=throws
            ) as resp:
                if resp.status == 200:
                    return await resp.json()

            await asyncio.sleep(settings.ONEDRIVE_ASYNC_REQUEST_SLEEP_INTERVAL)

    async def intra_move(self, dest_provider, src_path, dest_path):
        """ OneDrive API Reference: https://dev.onedrive.com/items/move.htm
            Use Cases: file rename or file move or folder rename or folder move
        """

        dest_exist = dest_path.identifier is not None

        if dest_exist:
            await dest_provider.delete(dest_path)

        if dest_path.parent.is_root and self.has_real_root():
            parent_reference = {
                'path': '/drive/root:/'
            }
        else:
            parent_reference = {
                'id': dest_path.parent.identifier
            }

        url = self.build_url(src_path.identifier)
        payload = {'name': dest_path.name,
                   'parentReference': parent_reference}

        logger.info('intra_move dest_path::{} src_path::{} url::{} payload:{}'.format(str(dest_path.parent.identifier),
                                                                                      repr(src_path), url, payload))

        async with self.request(
                'PATCH',
                url,
                data=json.dumps(payload),
                headers={'content-type': 'application/json'},
                expects=(200,),
                throws=exceptions.IntraMoveError,
        ) as resp:

            data = await resp.json()
            dest_path.update_from_response(data)

            logger.info('intra_move data:{}'.format(data))

            return self._construct_metadata(data, dest_path), not dest_exist

    async def download(self, path, revision=None, range=None, **kwargs):
        """ OneDrive API Reference: https://dev.onedrive.com/items/download.htm """
        logger.info('folder:: {} revision::{} path.identifier:{} path:{} path.parts:{}'.format(self.folder, revision,
                                                                                               path.identifier,
                                                                                               repr(path),
                                                                                               repr(path._parts)))

        if path.identifier is None:
            raise exceptions.DownloadError('"{}" not found'.format(str(path)), code=404)
        download_url = None
        if revision:
            items = await self._revisions_json(path)
            for item in items['value']:
                if item['eTag'] == revision:
                    download_url = item['@content.downloadUrl']
                    break
        else:
            url = self._build_content_url(path.identifier)
            logger.info('url::{}'.format(url))
            metaData = await self.make_request('GET',
                                               url,
                                               expects=(200,),
                                               throws=exceptions.MetadataError)
            data = await metaData.json()
            logger.debug('data::{} downloadUrl::{}'.format(data, download_url))
            download_url = data['@content.downloadUrl']

        if download_url is None:
            raise exceptions.NotFoundError(str(path))

        resp = await self.make_request(
            'GET',
            download_url,
            range=range,
            expects=(200, 206),
            headers={'accept-encoding': ''},
            throws=exceptions.DownloadError,
        )

        return streams.ResponseStreamReader(resp)

    async def create_folder(self, path, **kwargs):
        """
        OneDrive API Reference: https://dev.onedrive.com/items/create.htm
        :param str path: The path to create a folder at
        :type path: OneDrivePath
        """
        OneDrivePath.validate_folder(path)

        if not path.parent.identifier:
            raise exceptions.CreateFolderError('Path parent wasn\'t validated', code=400)

        if path.parent.is_root and self.has_real_root():
            # OneDrive's root folder has a different alias between create folder and at least upload;
            url = self._build_root_url('drive', 'root', 'children')
        else:
            url = self._build_content_url(path.parent.identifier, 'children')

        logger.info("upload url:{} path:{} folderName:{}".format(url, repr(path), repr(path.name)))
        payload = {'name': path.name,
                   'folder': {},
                   "@name.conflictBehavior": "rename"}

        async with self.request(
                'POST',
                url,
                data=json.dumps(payload),
                headers={'content-type': 'application/json'},
                expects=(201,),
                throws=exceptions.CreateFolderError,
        ) as resp:
            data = await resp.json()
            path.update_from_response(data)
            logger.info('upload:: data:{}'.format(data))

            return OneDriveFolderMetadata(data, path)

    async def upload(self, stream, path, conflict='replace', **kwargs):
        """ OneDrive API Reference: https://dev.onedrive.com/items/upload_put.htm
            Limited to 100MB file upload. """
        path, exists = await self.handle_name_conflict(path, conflict=conflict)

        logger.info("upload path:{} path.parent:{} path.path:{} self:{}".format(repr(path), path.parent, path.full_path,
                                                                                repr(self)))

        upload_url = self._build_content_url(path.parent.identifier, 'children', '{}/content'.format(path.name))

        logger.info(
            "upload url:{} path:{} str(path):{} str(full_path):{} self:{}".format(upload_url, repr(path), str(path),
                                                                                  str(path), repr(self.folder)))

        async with self.request(
                'PUT',
                upload_url,
                headers={'Content-Length': str(stream.size)},
                data=stream,
                expects=(201,),
                throws=exceptions.UploadError,
        ) as resp:
            data = await resp.json()
            logger.info('upload:: data:{}'.format(data))
            path.update_from_response(data)
            return OneDriveFileMetadata(data, path), not exists

    async def delete(self, path, confirm_delete=0, **kwargs):
        """ OneDrive API Reference: https://dev.onedrive.com/items/delete.htm """
        # copy or move automatically try to delete all directories for 'conflict' = 'replace'
        if not path.identifier:
            raise exceptions.NotFoundError(str(path))

        if path.is_root:
            if confirm_delete != 1:
                raise exceptions.DeleteError(
                    'confirm_delete=1 is required for deleting root provider folder',
                    code=400
                )
            root_content = await self.metadata(path)
            if root_content:
                results, _ = (await asyncio.wait([self.delete(item._path) for item in root_content]))
                # try to raise first exception
                for fut in results:
                    fut.result()
            return

        async with self.request(
                'DELETE',
                self._build_content_url(path.identifier),
                data={},
                expects=(204,),
                throws=exceptions.DeleteError,
        ):
            return

    async def metadata(self, path, revision=None, **kwargs):
        """ OneDrive API Reference: https://dev.onedrive.com/items/get.htm """
        logger.info('metadata identifier::{} path::{} revision::{}'
                    .format(repr(path.identifier), repr(path), repr(revision)))

        if path.is_root and self.has_real_root():
            #  handle when OSF is linked to real root onedrive
            url = self._build_content_url('root', expand='children')
        else:
            # handles root/sub1, root/sub1/sub2
            if path.identifier is None:
                raise exceptions.NotFoundError(str(path))
            url = self._build_content_url(path.identifier, expand='children')

        logger.info("metadata url::{}".format(repr(url)))
        async with self.request(
                'GET', url,
                expects=(200,),
                throws=exceptions.MetadataError
        ) as resp:
            logger.debug("metadata resp::{}".format(repr(resp)))
            data = await resp.json()

            if data.get('deleted'):
                raise exceptions.MetadataError(
                    "Could not retrieve {kind} '{path}'".format(
                        kind='folder' if 'folder' in data else 'file',
                        path=path,
                    ),
                    code=http.client.NOT_FOUND,
                )
            path.update_from_response(data)
            return self._construct_metadata_response(data, path)

    async def revisions(self, path, **kwargs):
        """
        OneDrive API Reference: https://dev.onedrive.com/items/view_delta.htm
        As of May 20, 2016: for files, the latest state is returned. There is not a list of changes for the file.
        """
        data = await self._revisions_json(path, **kwargs)
        logger.info('revisions: data::{}'.format(data['value']))

        return [OneDriveRevision(item) for item in data['value'] if not item.get('deleted')]

    async def _revisions_json(self, path, **kwargs):
        """
        OneDrive API Reference: https://dev.onedrive.com/items/view_delta.htm
        As of May 20, 2016: for files, the latest state is returned.  There is not a list of changes for the file.
        """
        if path.identifier is None:
            raise exceptions.NotFoundError(str(path))
        async with self.request(
                'GET',
                self.build_url(path.identifier, 'view.delta', top=250),
                expects=(200,),
                throws=exceptions.RevisionsError
        ) as resp:
            data = await resp.json()
            logger.info('revisions: data::{}'.format(data['value']))

            return data

    def can_duplicate_names(self):
        return False

    def can_intra_copy(self, dest_provider, path=None):
        return type(self) == type(dest_provider)

    def can_intra_move(self, dest_provider, path=None):
        return self == dest_provider

    def path_from_metadata(self, parent_path, metadata):
        return metadata._path

    def _construct_metadata_response(self, data, path):
        if 'folder' in data.keys():
            ret = []
            if 'children' in data.keys():
                for item in data['children']:
                    item_path = path.child(item['name'], _id=item['id'], folder=bool(item.get('folder', None)))
                    ret.append(self._construct_metadata(item, item_path))
            return ret

        return self._construct_metadata(data, path)

    def _construct_metadata(self, item, path):
        if 'folder' in item.keys():
            metadata = OneDriveFolderMetadata(item, path)
        else:
            metadata = OneDriveFileMetadata(item, path)

        return metadata

    def _build_root_url(self, *segments, **query):
        return provider.build_url(settings.BASE_ROOT_URL, *segments, **query)

    def _build_content_url(self, *segments, **query):
        return provider.build_url(settings.BASE_CONTENT_URL, *segments, **query)
