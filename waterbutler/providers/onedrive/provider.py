import json
import http
import asyncio

import logging

from urllib.parse import urlparse

from itertools import repeat

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
#  from waterbutler.tasks.core import backgroundify
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.onedrive import settings
from waterbutler.providers.onedrive.metadata import OneDriveRevision
from waterbutler.providers.onedrive.metadata import OneDriveFileMetadata
from waterbutler.providers.onedrive.metadata import OneDriveFolderMetadata

logger = logging.getLogger(__name__)


class OneDriveProvider(provider.BaseProvider):
    NAME = 'onedrive'
    BASE_URL = settings.BASE_URL

    def __init__(self, auth, credentials, settings):
        super().__init__(auth, credentials, settings)
        self.token = self.credentials['token']
        self.folder = self.settings['folder']
        logger.debug("__init__ credentials:{} settings:{}".format(repr(credentials), repr(settings)))

    @asyncio.coroutine
    def validate_v1_path(self, path, **kwargs):
        if path == '/':
            return WaterButlerPath(path, prepend=self.folder)

        logger.info('validate_v1_path self::{} path::{}  url:{}'.format(repr(self), repr(path), self.build_url(path)))

        resp = yield from self.make_request(
            'GET', self.build_url(path),
            expects=(200, 400),
            throws=exceptions.MetadataError
        )

        if resp.status == 400:
            return WaterButlerPath(path, prepend=self.folder)

        data = yield from resp.json()

        names = self._get_names(data)
        ids = self._get_ids(data)

        wb_path = WaterButlerPath(names, _ids=ids, folder=path.endswith('/'))
        logger.info('wb_path::{}  IDs:{}'.format(repr(wb_path._parts), repr(ids)))
        return wb_path

    @asyncio.coroutine
    def validate_path(self, path, **kwargs):
        logger.info('validate_path self::{} path::{}'.format(repr(self), path))
        return self.validate_v1_path(path, **kwargs)

    @asyncio.coroutine
    def revalidate_path(self, base, path, folder=None):
        logger.info('revalidate_path base::{} path::{}  base.id::{}'.format(base._prepend, path, base.identifier))
        logger.info('revalidate_path self::{} base::{} path::{}'.format(str(self), repr(base), repr(path)))
        logger.info('revalidate_path base::{} path::{}'.format(repr(base.full_path), repr(path)))

        if (base.identifier is not None):
            url = self.build_url(base.identifier)
            resp = yield from self.make_request(
                'GET', url,
                expects=(200, ),
                throws=exceptions.MetadataError
            )
            data = yield from resp.json()
            folder_path = self._get_names(data)
            url = self._build_root_url("drive/root:", folder_path, str(path))
        elif (base._prepend is None):
            #  in a sub-folder, no need to get the root id
            url = self._build_root_url('drive/root:', base.full_path, str(path))
        else:
            #  root: get folder name and build path from it
            url = self.build_url(base._prepend)
            resp = yield from self.make_request(
                'GET', url,
                expects=(200, ),
                throws=exceptions.MetadataError
            )
            data = yield from resp.json()
            url = self._build_root_url("drive/root:", self._get_names(data), str(path))

        resp = yield from self.make_request(
            'GET',
            url,
            expects=(200, 404, ),
            throws=exceptions.ProviderError
        )

        if (resp.status == 404):
            ids = None
            folder = False
        else:
            data = yield from resp.json()
            ids = self._get_ids(data)[-1]
            folder = ('folder' in data.keys())

        return base.child(path, _id=ids, folder=folder)

    @property
    def default_headers(self):
        return {
            'Authorization': 'Bearer {}'.format(self.token),
        }

    @asyncio.coroutine
    def intra_copy(self, dest_provider, src_path, dest_path):
        #  https://dev.onedrive.com/items/copy.htm

        url = self.build_url(src_path.identifier, 'action.copy')
        payload = json.dumps({'name': dest_path.name,
                              'parentReference': {'id': dest_path.parent.full_path.strip('/') if dest_path.parent.identifier is None else dest_path.parent.identifier}})  # TODO: this feels like a hack.  parent.identifier is None in some cases.

        logger.info('intra_copy dest_provider::{} src_path::{} dest_path::{}  url::{} payload::{}'.format(repr(dest_provider), repr(src_path), repr(dest_path), repr(url), payload))
        resp = yield from self.make_request(
            'POST',
            url,
            data=payload,
            headers={'content-type': 'application/json', 'Prefer': 'respond-async'},
            expects=(202, ),
            throws=exceptions.IntraCopyError,
        )
        if resp is None:
            raise exceptions.IntraCopyError
        logger.info('resp::{}'.format(repr(resp)))
        status_url = resp.headers['LOCATION']
        logger.info('status_url::{}'.format(repr(status_url)))
        i = 0
        status = False
        while (i < 100):
            logger.info('status::{}  i:{}'.format(repr(status), i))
            status = yield from self._copy_status(status_url)
            #  status = backgroundify(self._copy_status(status_url))  #  TODO: determine how best to call status check without blocking and without returning to OSF
            if (status):
                break
            i += 1

        data = yield from self.metadata(src_path, None)  # TODO: validate_v1_path to get destination ID/Name pair and then return self.metadata so OSF has new Destination path IDs.
        return data, True

    @asyncio.coroutine
    def _copy_status(self, status_url):
        #  docs: https://dev.onedrive.com/resources/asyncJobStatus.htm
        status = 'notStarted'
        resp = yield from self.make_request(
            'GET', status_url,
            expects=(200, 202),
            throws=exceptions.IntraCopyError,
        )
        data = yield from resp.json()
        status = data.get('status')
        logger.info('_copy_status  status::{} resp:{} data::{}'.format(repr(status), repr(resp), repr(data)))
        return True if resp.status == 200 else False

    @asyncio.coroutine
    def intra_move(self, dest_provider, src_path, dest_path):
        #  https://dev.onedrive.com/items/move.htm

        #  PATCH /drive/items/{item-id}
        #  use cases: file rename or file move or folder rename or folder move

        url = self.build_url(src_path.identifier)
        payload = json.dumps({'name': dest_path.name,
                              'parentReference': {'id': dest_path.parent.full_path.strip('/') if dest_path.parent.identifier is None else dest_path.parent.identifier}})  # TODO: this feels like a hack.  parent.identifier is None

        logger.info('intra_move dest_path::{} src_path::{} url::{} payload:{}'.format(str(dest_path.parent.identifier), repr(src_path), url, payload))

        try:
            resp = yield from self.make_request(
                'PATCH',
                url,
                data=payload,
                headers={'content-type': 'application/json'},
                expects=(200, ),
                throws=exceptions.IntraMoveError,
            )
        except exceptions.IntraMoveError as e:
            if e.code != 403:
                raise

        data = yield from resp.json()

        logger.info('intra_move data:{}'.format(data))

        if 'folder' not in data.keys():
            return OneDriveFileMetadata(data, self.folder), True

        folder = OneDriveFolderMetadata(data, self.folder)

        return folder, True

    @asyncio.coroutine
    def download(self, path, revision=None, range=None, **kwargs):

        logger.info('folder:: {} revision::{} path.identifier:{} path:{} path.parts:{}'.format(self.folder, revision, path.identifier, repr(path), repr(path._parts)))

        if path.identifier is None:
            raise exceptions.DownloadError('"{}" not found'.format(str(path)), code=404)
        downloadUrl = None
        if revision:
            items = yield from self._revisions_json(path)
            for item in items['value']:
                if item['eTag'] == revision:
                    downloadUrl = item['@content.downloadUrl']
                    break
        else:
            url = self._build_content_url(path.identifier)
            logger.info('url::{}'.format(url))
            metaData = yield from self.make_request('GET',
                                                    url,
                                                    expects=(200, ),
                                                    throws=exceptions.MetadataError
                                                    )
            data = yield from metaData.json()
            logger.debug('data::{} downloadUrl::{}'.format(data, downloadUrl))
            downloadUrl = data['@content.downloadUrl']
        if downloadUrl is None:
            raise exceptions.NotFoundError(str(path))

        resp = yield from self.make_request(
            'GET',
            downloadUrl,
            range=range,
            expects=(200, 206),
            throws=exceptions.DownloadError,
        )

        return streams.ResponseStreamReader(resp)

    @asyncio.coroutine
    def upload(self, stream, path, conflict='replace', **kwargs):
        path, exists = yield from self.handle_name_conflict(path, conflict=conflict)
        #  PUT /drive/items/{parent-id}/children/{filename}/content

        fileName = self._get_one_drive_id(path)
        path = self._get_sub_folder_path(path, fileName)
        upload_url = self.build_url(path, 'children', fileName, "content")

        logger.info("upload url:{} path:{} str(path):{} str(full_path):{} self:{}".format(upload_url, repr(path), str(path), str(path), repr(self.folder)))

        resp = yield from self.make_request(
            'PUT',
            upload_url,
            headers={'Content-Length': str(stream.size)},
            data=stream,
            expects=(201, ),
            throws=exceptions.UploadError,
        )

        data = yield from resp.json()
        logger.info('upload:: data:{}'.format(data))
        return OneDriveFileMetadata(data, self.folder), not exists

    @asyncio.coroutine
    def delete(self, path, **kwargs):
        yield from self.make_request(
            'DELETE',
            self.build_url(path.identifier),
            data={},
            expects=(204, ),
            throws=exceptions.DeleteError,
        )

    @asyncio.coroutine
    def metadata(self, path, revision=None, **kwargs):
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
        resp = yield from self.make_request(
            'GET', url,
            expects=(200, ),
            throws=exceptions.MetadataError
        )
        logger.debug("metadata resp::{}".format(repr(resp)))

        data = yield from resp.json()
        #  logger.info("metadata data::{}".format(repr(data)))

        if data.get('deleted'):
            raise exceptions.MetadataError(
                "Could not retrieve {kind} '{path}'".format(
                    kind='folder' if data['folder'] else 'file',
                    path=path,
                ),
                code=http.client.NOT_FOUND,
            )

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

    @asyncio.coroutine
    def revisions(self, path, **kwargs):
        #  https://dev.onedrive.com/items/view_delta.htm
        data = yield from self._revisions_json(path, **kwargs)
        logger.info('revisions: data::{}'.format(data['value']))

        return [
            OneDriveRevision(item)
            for item in data['value']
            if not item.get('deleted')
        ]

    @asyncio.coroutine
    def create_folder(self, path, **kwargs):
        """
        :param str path: The path to create a folder at
        """
        #  https://dev.onedrive.com/items/create.htm
        #  PUT /drive/items/{parent-id}:/{name}
        #  In the request body, supply a JSON representation of a Folder Item, as shown below.
        WaterButlerPath.validate_folder(path)

        folderName = path.full_path.split('/')[-2]
        parentFolder = path.full_path.split('/')[-3]
        upload_url = self.build_url(parentFolder, 'children')

        logger.info("upload url:{} path:{} parentFolder:{} folderName:{}".format(upload_url, repr(path), str(parentFolder), repr(folderName)))
        payload = {'name': folderName,
                   'folder': {},
                    "@name.conflictBehavior": "rename"}

        resp = yield from self.make_request(
            'POST',
            upload_url,
            data=json.dumps(payload),
            headers={'content-type': 'application/json'},
            expects=(201, ),
            throws=exceptions.CreateFolderError,
        )

        data = yield from resp.json()
        logger.info('upload:: data:{}'.format(data))
        return OneDriveFolderMetadata(data, self.folder)

    @asyncio.coroutine
    def _revisions_json(self, path, **kwargs):
        #  https://dev.onedrive.com/items/view_delta.htm
        #  TODO: 2015-11-29 - onedrive only appears to return the last delta for a token, period.  Not sure if there is a work around, from the docs: "The delta feed shows the latest state for each item, not each change. If an item were renamed twice, it would only show up once, with its latest name."
        if path.identifier is None:
                raise exceptions.NotFoundError(str(path))
        response = yield from self.make_request(
            'GET',
            self.build_url(path.identifier, 'view.delta', top=250),
            expects=(200, ),
            throws=exceptions.RevisionsError
        )
        data = yield from response.json()
        logger.info('revisions: data::{}'.format(data['value']))

        return data

    def can_duplicate_names(self):
        return False

    def can_intra_copy(self, dest_provider, path=None):
        return type(self) == type(dest_provider)

    def can_intra_move(self, dest_provider, path=None):
        return self == dest_provider

    def _build_root_url(self, *segments, **query):
        return provider.build_url(settings.BASE_ROOT_URL, *segments, **query)

    def _build_content_url(self, *segments, **query):
        return provider.build_url(settings.BASE_CONTENT_URL, *segments, **query)

#      def _is_folder(self, path):
#          return True if str(path).endswith('/') else False

    def _get_one_drive_id(self, path):
        return path.full_path[path.full_path.rindex('/') + 1:]

    def _get_names(self, data):
        parent_path = data['parentReference']['path'].replace('/drive/root:', '')
        if (len(parent_path) == 0):
            names = '/{}'.format(data['name'])
        else:
            names = '{}/{}'.format(parent_path, data['name'])
        return names

    def _get_ids(self, data):
        ids = [data['parentReference']['id'], data['id']]
        url_segment_count = len(urlparse(self._get_names(data)).path.split('/'))
        if (len(ids) < url_segment_count):
            for x in repeat(None, url_segment_count - len(ids)):
                ids.insert(0, x)
        return ids

    def _get_sub_folder_path(self, path, fileName):
        return urlparse(path.full_path.replace(fileName, '')).path.split('/')[-2]
