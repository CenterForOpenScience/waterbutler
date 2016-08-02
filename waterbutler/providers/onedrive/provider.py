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
#  from waterbutler.tasks.core import backgroundify
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.onedrive import settings
from waterbutler.providers.onedrive.metadata import OneDriveRevision
from waterbutler.providers.onedrive.metadata import OneDriveFileMetadata
from waterbutler.providers.onedrive.metadata import OneDriveFolderMetadata

logger = logging.getLogger(__name__)


class OneDrivePath(path.WaterButlerPath):
    """OneDrive specific WaterButlerPath class to handle some of the idiosyncrasies of
    file paths in OneDrive."""
    def file_path(self, data):
        parent_path = data['parentReference']['path'].replace('/drive/root:', '')
        if (len(parent_path) == 0):
            names = '/{}'.format(data['name'])
        else:
            names = '{}/{}'.format(parent_path, data['name'])
        return names

    def ids(self, data):
        ids = [data['parentReference']['id'], data['id']]
        url_segment_count = len(urlparse(self.file_path(data)).path.split('/'))
        if (len(ids) < url_segment_count):
            for x in repeat(None, url_segment_count - len(ids)):
                ids.insert(0, x)
        return ids

    def one_drive_parent_folder(self, path):
        if path._prepend == '0':
            folder = 'drive/root:/'
        elif str(path.parent) == '/':
            folder = path._prepend
        else:
            folder = path.path.replace(path.name, '')
        return folder


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
        logger.debug("__init__ credentials:{} settings:{}".format(repr(credentials), repr(settings)))

    async def validate_v1_path(self, path, **kwargs):
        if path == '/':
            return WaterButlerPath(path, prepend=self.folder)

        logger.info('validate_v1_path self::{} path::{}  url:{}'.format(repr(self), repr(path), self.build_url(path)))

        resp = await self.make_request(
            'GET', self.build_url(path),
            expects=(200, 400),
            throws=exceptions.MetadataError
        )

        if resp.status == 400:
            await resp.release()
            return WaterButlerPath(path, prepend=self.folder)

        data = await resp.json()

        od_path = OneDrivePath(path)
        names = od_path.file_path(data)
        ids = od_path.ids(data)

        wb_path = WaterButlerPath(names, _ids=ids, folder=path.endswith('/'))
        logger.info('wb_path::{}  IDs:{}'.format(repr(wb_path._parts), repr(ids)))
        return wb_path

    async def validate_path(self, path, **kwargs):
        logger.info('validate_path self::{} path::{}'.format(repr(self), path))
        return await self.validate_v1_path(path, **kwargs)

    async def revalidate_path(self, base, path, folder=None):
        logger.info('revalidate_path base::{} path::{}  base.id::{}'.format(base._prepend, path, base.identifier))
        logger.info('revalidate_path self::{} base::{} path::{}'.format(str(self), repr(base), repr(path)))
        logger.info('revalidate_path base::{} path::{}'.format(repr(base.full_path), repr(path)))
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

    @property
    def default_headers(self):
        return {
            'Authorization': 'Bearer {}'.format(self.token),
        }

    async def intra_copy(self, dest_provider, src_path, dest_path):
        """ OneDrive API Reference: https://dev.onedrive.com/items/copy.htm """

        url = self.build_url(src_path.identifier, 'action.copy')
        payload = json.dumps({'name': dest_path.name,
                              'parentReference': {'id': dest_path.parent.full_path.strip('/') if dest_path.parent.identifier is None else dest_path.parent.identifier}})  # TODO: this feels like a hack.  parent.identifier is None in some cases.

        logger.info('intra_copy dest_provider::{} src_path::{} dest_path::{}  url::{} payload::{}'.format(repr(dest_provider), repr(src_path), repr(dest_path), repr(url), payload))
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
        metadata = self._construct_metadata(status)
        return metadata, dest_path.identifier is None

    async def _copy_status(self, status_url):
        """ OneDrive API Reference: https://dev.onedrive.com/resources/asyncJobStatus.htm """
        status = 'notStarted'
        resp = await self.make_request(
            'GET', status_url,
            expects=(200, 202),
            throws=exceptions.IntraCopyError,
        )
        data = await resp.json()
        status = data.get('status')
        logger.info('_copy_status  status::{} resp:{} data::{}'.format(repr(status), repr(resp), repr(data)))
        return data if resp.status == 200 else None

    async def intra_move(self, dest_provider, src_path, dest_path):
        """ OneDrive API Reference: https://dev.onedrive.com/items/move.htm
            Use Cases: file rename or file move or folder rename or folder move
        """

        parentReference = {'id': dest_path.parent.full_path.strip('/') if dest_path.parent.identifier is None else dest_path.parent.identifier}
        url = self.build_url(src_path.identifier)
        payload = json.dumps({'name': dest_path.name,
                              'parentReference': parentReference})

        logger.info('intra_move dest_path::{} src_path::{} url::{} payload:{}'.format(str(dest_path.parent.identifier), repr(src_path), url, payload))

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

    async def download(self, path, revision=None, range=None, **kwargs):
        """ OneDrive API Reference: https://dev.onedrive.com/items/download.htm """
        logger.info('folder:: {} revision::{} path.identifier:{} path:{} path.parts:{}'.format(self.folder, revision, path.identifier, repr(path), repr(path._parts)))

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
            metaData = await self.make_request('GET',
                                                    url,
                                                    expects=(200, ),
                                                    throws=exceptions.MetadataError)
            data = await metaData.json()
            logger.debug('data::{} downloadUrl::{}'.format(data, downloadUrl))
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

    async def create_folder(self, path, **kwargs):
        """
        OneDrive API Reference: https://dev.onedrive.com/items/create.htm
        :param str path: The path to create a folder at
        """
        WaterButlerPath.validate_folder(path)
        od_path = OneDrivePath(str(path))

        if path._prepend == '0':
            #  OneDrive's root folder has a different alias between create folder and at least upload; need to strip out :
            upload_url = self._build_root_url(od_path.one_drive_parent_folder(path).replace(':', ''), 'children')
        else:
            upload_url = self.build_url(od_path.one_drive_parent_folder(path), 'children')

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

    async def upload(self, stream, path, conflict='replace', **kwargs):
        """ OneDrive API Reference: https://dev.onedrive.com/items/upload_put.htm
            Limited to 100MB file upload. """
        path, exists = await self.handle_name_conflict(path, conflict=conflict)

        logger.info("upload path:{} path.parent:{} path.path:{} self:{}".format(repr(path), path.parent, path.full_path, repr(self)))

        od_path = OneDrivePath(str(path))
        if path._prepend == '0':
            upload_url = self._build_root_url(od_path.one_drive_parent_folder(path), '{}:/content'.format(path.name))
        else:
            upload_url = self.build_url(od_path.one_drive_parent_folder(path), 'children', path.name, "content")

        logger.info("upload url:{} path:{} str(path):{} str(full_path):{} self:{}".format(upload_url, repr(path), str(path), str(path), repr(self.folder)))

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
        """ OneDrive API Reference: https://dev.onedrive.com/items/delete.htm """
        resp = await self.make_request(
            'DELETE',
            self.build_url(path.identifier),
            data={},
            expects=(204, ),
            throws=exceptions.DeleteError,
        )
        await resp.release()

    async def metadata(self, path, revision=None, **kwargs):
        """ OneDrive API Reference: https://dev.onedrive.com/items/get.htm """
        logger.info('metadata identifier::{} path::{} revision::{}'.format(repr(path.identifier), repr(path), repr(revision)))

        if (path.full_path == '0/'):
            #  handle when OSF is linked to root onedrive
            url = self.build_url('root', expand='children')
        elif str(path) == '/':
            #  OSF lined to sub folder
            url = self.build_url(path.full_path, expand='children')
        else:
            #  handles root/sub1, root/sub1/sub2
            if path.identifier is None:
                raise exceptions.NotFoundError(str(path))
            url = self.build_url(path.identifier, expand='children')

        logger.info("metadata url::{}".format(repr(url)))
        resp = await self.make_request(
            'GET', url,
            expects=(200, ),
            throws=exceptions.MetadataError
        )
        logger.debug("metadata resp::{}".format(repr(resp)))

        data = await resp.json()
        #  logger.info("metadata data::{}".format(repr(data)))

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
        """ OneDrive API Reference: https://dev.onedrive.com/items/view_delta.htm
            As of May 20, 2016: for files, the latest state is returned.  There is not a list of changes for the file.
        """
        data = await self._revisions_json(path, **kwargs)
        logger.info('revisions: data::{}'.format(data['value']))

        return [
            OneDriveRevision(item)
            for item in data['value']
            if not item.get('deleted')
        ]

    async def _revisions_json(self, path, **kwargs):
        """ OneDrive API Reference: https://dev.onedrive.com/items/view_delta.htm
            As of May 20, 2016: for files, the latest state is returned.  There is not a list of changes for the file.
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

    def can_duplicate_names(self):
        return False

    def can_intra_copy(self, dest_provider, path=None):
        return type(self) == type(dest_provider)

    def can_intra_move(self, dest_provider, path=None):
        return self == dest_provider

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
