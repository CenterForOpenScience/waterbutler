import json
import http
import asyncio

import logging


from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
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
        logger.debug('__init__')        
        super().__init__(auth, credentials, settings)
        logger.debug('token::' + repr(self.credentials))        
        self.token = self.credentials['token']
        self.folder = self.settings['folder']
        

    @asyncio.coroutine
    def validate_v1_path(self, path, **kwargs):
        if path == '/':
            return WaterButlerPath(path, prepend=self.folder)

        implicit_folder = path.endswith('/')

        resp = yield from self.make_request(
            'GET', self.build_url('metadata', 'auto', self.folder + path),
            expects=(200,),
            throws=exceptions.MetadataError
        )

        data = yield from resp.json()
        explicit_folder = data['is_dir']
        if explicit_folder != implicit_folder:
            raise exceptions.NotFoundError(str(path))

        return WaterButlerPath(path, prepend=self.folder)

    @asyncio.coroutine
    def validate_path(self, path, **kwargs):
        return WaterButlerPath(path, prepend=self.folder)

    @property
    def default_headers(self):
        return {
            'Authorization': 'Bearer {}'.format(self.token),
        }

    @asyncio.coroutine
    def intra_copy(self, dest_provider, src_path, dest_path):
        try:
            if self == dest_provider:
                resp = yield from self.make_request(
                    'POST',
                    self.build_url('fileops', 'copy'),
                    data={
                        'root': 'auto',
                        'from_path': src_path.full_path,
                        'to_path': dest_path.full_path,
                    },
                    expects=(200, 201),
                    throws=exceptions.IntraCopyError,
                )
            else:
                from_ref_resp = yield from self.make_request(
                    'GET',
                    self.build_url('copy_ref', 'auto', src_path.full_path),
                )
                from_ref_data = yield from from_ref_resp.json()
                resp = yield from self.make_request(
                    'POST',
                    self.build_url('fileops', 'copy'),
                    data={
                        'root': 'auto',
                        'from_copy_ref': from_ref_data['copy_ref'],
                        'to_path': dest_path,
                    },
                    headers=dest_provider.default_headers,
                    expects=(200, 201),
                    throws=exceptions.IntraCopyError,
                )
        except exceptions.IntraCopyError as e:
            if e.code != 403:
                raise

            yield from dest_provider.delete(dest_path)
            resp, _ = yield from self.intra_copy(dest_provider, src_path, dest_path)
            return resp, False

        # TODO Refactor into a function
        data = yield from resp.json()

        if not data['is_dir']:
            return OneDriveFileMetadata(data, self.folder), True

        folder = OneDriveFolderMetadata(data, self.folder)

        folder.children = []
        for item in data['contents']:
            if item['is_dir']:
                folder.children.append(OneDriveFolderMetadata(item, self.folder))
            else:
                folder.children.append(OneDriveFileMetadata(item, self.folder))

        return folder, True

    @asyncio.coroutine
    def intra_move(self, dest_provider, src_path, dest_path):
        if dest_path.full_path.lower() == src_path.full_path.lower():
            # OneDrive does not support changing the casing in a file name
            raise exceptions.InvalidPathError('In OneDrive to change case, add or subtract other characters.')

        try:
            resp = yield from self.make_request(
                'POST',
                self.build_url('fileops', 'move'),
                data={
                    'root': 'auto',
                    'to_path': dest_path.full_path,
                    'from_path': src_path.full_path,
                },
                expects=(200, ),
                throws=exceptions.IntraMoveError,
            )
        except exceptions.IntraMoveError as e:
            if e.code != 403:
                raise

            yield from dest_provider.delete(dest_path)
            resp, _ = yield from self.intra_move(dest_provider, src_path, dest_path)
            return resp, False

        data = yield from resp.json()

        if not data['is_dir']:
            return OneDriveFileMetadata(data, self.folder), True

        folder = OneDriveFolderMetadata(data, self.folder)

        folder.children = []
        for item in data['contents']:
            if item['is_dir']:
                folder.children.append(OneDriveFolderMetadata(item, self.folder))
            else:
                folder.children.append(OneDriveFileMetadata(item, self.folder))

        return folder, True

    @asyncio.coroutine
    def download(self, path, revision=None, range=None, **kwargs):
        if revision:
            url = self._build_content_url('files', 'auto', path.full_path, rev=revision)
        else:
            # Dont add unused query parameters
            url = self._build_content_url('files', 'auto', path.full_path)

        resp = yield from self.make_request(
            'GET',
            url,
            range=range,
            expects=(200, 206),
            throws=exceptions.DownloadError,
        )

        if 'Content-Length' not in resp.headers:
            size = json.loads(resp.headers['X-DROPBOX-METADATA'])['bytes']
        else:
            size = None

        return streams.ResponseStreamReader(resp, size=size)

    @asyncio.coroutine
    def upload(self, stream, path, conflict='replace', **kwargs):
        path, exists = yield from self.handle_name_conflict(path, conflict=conflict)

        resp = yield from self.make_request(
            'PUT',
            self._build_content_url('files_put', 'auto', path.full_path),
            headers={'Content-Length': str(stream.size)},
            data=stream,
            expects=(200, ),
            throws=exceptions.UploadError,
        )

        data = yield from resp.json()
        return OneDriveFileMetadata(data, self.folder), not exists

    @asyncio.coroutine
    def delete(self, path, **kwargs):
        yield from self.make_request(
            'POST',
            self.build_url('fileops', 'delete'),
            data={'root': 'auto', 'path': path.full_path},
            expects=(200, ),
            throws=exceptions.DeleteError,
        )

    @asyncio.coroutine
    def metadata(self, path, revision=None, **kwargs):
        if revision:
            url = self.build_url('revisions', 'auto', path.full_path, rev_limit=250)

        else:
            url = self.build_url('/drive/items/', path.full_path)
            
        logger.debug('metadata::{}'.format(repr(url)))
        
        resp = yield from self.make_request(
            'GET', url,
            expects=(200, ),
            throws=exceptions.MetadataError
        )

        data = yield from resp.json()
        logger.debug("data::{}".format(repr(data)))

        if revision:
            try:
                data = next(v for v in (yield from resp.json()) if v['rev'] == revision)
            except StopIteration:
                raise exceptions.NotFoundError(str(path))

        # OneDrive will match a file or folder by name within the requested path
#         if path.is_file and data['is_dir']:
#             raise exceptions.MetadataError(
#                 "Could not retrieve file '{}'".format(path),
#                 code=http.client.NOT_FOUND,
#             )

        if data.get('deleted'):
            raise exceptions.MetadataError(
                "Could not retrieve {kind} '{path}'".format(
                    kind='folder' if data['folder'] else 'file',
                    path=path,
                ),
                code=http.client.NOT_FOUND,
            )

        logger.debug('data::{}'.format(repr(data)))

        if 'folder' in data.keys() and 'children' in data.keys(): # and data['folder']['childCount'] > 0:            
            ret = []
            for item in data['children']:
                if 'folder' in item.keys():
                    ret.append(OneDriveFolderMetadata(item, self.folder))
                else:
                    ret.append(OneDriveFileMetadata(item, self.folder))
            return ret

        return OneDriveFileMetadata(data, self.folder)

    @asyncio.coroutine
    def revisions(self, path, **kwargs):
        response = yield from self.make_request(
            'GET',
            self.build_url('revisions', 'auto', path.full_path, rev_limit=250),
            expects=(200, ),
            throws=exceptions.RevisionsError
        )
        data = yield from response.json()

        return [
            OneDriveRevision(item)
            for item in data
            if not item.get('is_deleted')
        ]

    @asyncio.coroutine
    def create_folder(self, path, **kwargs):
        """
        :param str path: The path to create a folder at
        """
        WaterButlerPath.validate_folder(path)

        response = yield from self.make_request(
            'POST',
            self.build_url('fileops', 'create_folder'),
            params={
                'root': 'auto',
                'path': path.full_path
            },
            expects=(200, 403),
            throws=exceptions.CreateFolderError
        )

        data = yield from response.json()

        if response.status == 403:
            if 'because a file or folder already exists at path' in data.get('error'):
                raise exceptions.FolderNamingConflict(str(path))
            raise exceptions.CreateFolderError(data, code=403)

        return OneDriveFolderMetadata(data, self.folder)

    def can_intra_copy(self, dest_provider, path=None):
        return type(self) == type(dest_provider)

    def can_intra_move(self, dest_provider, path=None):
        return self == dest_provider

    def _build_content_url(self, *segments, **query):
        return provider.build_url(settings.BASE_CONTENT_URL, *segments, **query)
