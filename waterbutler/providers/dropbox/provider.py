import json
import http

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.dropbox import settings
from waterbutler.providers.dropbox.metadata import DropboxRevision
from waterbutler.providers.dropbox.metadata import DropboxFileMetadata
from waterbutler.providers.dropbox.metadata import DropboxFolderMetadata


class DropboxProvider(provider.BaseProvider):
    NAME = 'dropbox'
    BASE_URL = settings.BASE_URL

    def __init__(self, auth, credentials, settings):
        super().__init__(auth, credentials, settings)
        self.token = self.credentials['token']
        self.folder = self.settings['folder']

    async def validate_v1_path(self, path, **kwargs):
        if path == '/':
            return WaterButlerPath(path, prepend=self.folder)

        implicit_folder = path.endswith('/')

        resp = await self.make_request(
            'GET', self.build_url('metadata', 'auto', self.folder + path),
            expects=(200,),
            throws=exceptions.MetadataError
        )

        data = await resp.json()
        explicit_folder = data['is_dir']
        if explicit_folder != implicit_folder:
            raise exceptions.NotFoundError(str(path))

        return WaterButlerPath(path, prepend=self.folder)

    async def validate_path(self, path, **kwargs):
        return WaterButlerPath(path, prepend=self.folder)

    def can_duplicate_names(self):
        return False

    @property
    def default_headers(self):
        return {
            'Authorization': 'Bearer {}'.format(self.token),
        }

    async def intra_copy(self, dest_provider, src_path, dest_path):
        dest_folder = dest_provider.folder

        try:
            if self == dest_provider:
                resp = await self.make_request(
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
                from_ref_resp = await self.make_request(
                    'GET',
                    self.build_url('copy_ref', 'auto', src_path.full_path),
                )
                from_ref_data = await from_ref_resp.json()
                resp = await self.make_request(
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

            await dest_provider.delete(dest_path)
            resp, _ = await self.intra_copy(dest_provider, src_path, dest_path)
            return resp, False

        # TODO Refactor into a function
        data = await resp.json()

        if not data['is_dir']:
            return DropboxFileMetadata(data, dest_folder), True

        folder = DropboxFolderMetadata(data, dest_folder)

        folder.children = []
        for item in data['contents']:
            if item['is_dir']:
                folder.children.append(DropboxFolderMetadata(item, dest_folder))
            else:
                folder.children.append(DropboxFileMetadata(item, dest_folder))

        return folder, True

    async def intra_move(self, dest_provider, src_path, dest_path):
        if dest_path.full_path.lower() == src_path.full_path.lower():
            # Dropbox does not support changing the casing in a file name
            raise exceptions.InvalidPathError('In Dropbox to change case, add or subtract other characters.')

        dest_folder = dest_provider.folder

        try:
            resp = await self.make_request(
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

            await dest_provider.delete(dest_path)
            resp, _ = await self.intra_move(dest_provider, src_path, dest_path)
            return resp, False

        data = await resp.json()

        if not data['is_dir']:
            return DropboxFileMetadata(data, dest_folder), True

        folder = DropboxFolderMetadata(data, dest_folder)

        folder.children = []
        for item in data['contents']:
            if item['is_dir']:
                folder.children.append(DropboxFolderMetadata(item, dest_folder))
            else:
                folder.children.append(DropboxFileMetadata(item, dest_folder))

        return folder, True

    async def download(self, path, revision=None, range=None, **kwargs):
        if revision:
            url = self._build_content_url('files', 'auto', path.full_path, rev=revision)
        else:
            # Dont add unused query parameters
            url = self._build_content_url('files', 'auto', path.full_path)

        resp = await self.make_request(
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

    async def upload(self, stream, path, conflict='replace', **kwargs):
        path, exists = await self.handle_name_conflict(path, conflict=conflict)

        resp = await self.make_request(
            'PUT',
            self._build_content_url('files_put', 'auto', path.full_path),
            headers={'Content-Length': str(stream.size)},
            data=stream,
            expects=(200, ),
            throws=exceptions.UploadError,
        )

        data = await resp.json()
        return DropboxFileMetadata(data, self.folder), not exists

    async def delete(self, path, **kwargs):
        await self.make_request(
            'POST',
            self.build_url('fileops', 'delete'),
            data={'root': 'auto', 'path': path.full_path},
            expects=(200, ),
            throws=exceptions.DeleteError,
        )

    async def metadata(self, path, revision=None, **kwargs):
        if revision:
            url = self.build_url('revisions', 'auto', path.full_path, rev_limit=250)

        else:
            url = self.build_url('metadata', 'auto', path.full_path)
        resp = await self.make_request(
            'GET', url,
            expects=(200, ),
            throws=exceptions.MetadataError
        )

        data = await resp.json()

        if revision:
            try:
                data = next(v for v in (await resp.json()) if v['rev'] == revision)
            except StopIteration:
                raise exceptions.NotFoundError(str(path))

        # Dropbox will match a file or folder by name within the requested path
        if path.is_file and data['is_dir']:
            raise exceptions.MetadataError(
                "Could not retrieve file '{}'".format(path),
                code=http.client.NOT_FOUND,
            )

        if data.get('is_deleted'):
            raise exceptions.MetadataError(
                "Could not retrieve {kind} '{path}'".format(
                    kind='folder' if data['is_dir'] else 'file',
                    path=path,
                ),
                code=http.client.NOT_FOUND,
            )

        if data['is_dir']:
            ret = []
            for item in data['contents']:
                if item['is_dir']:
                    ret.append(DropboxFolderMetadata(item, self.folder))
                else:
                    ret.append(DropboxFileMetadata(item, self.folder))
            return ret

        return DropboxFileMetadata(data, self.folder)

    async def revisions(self, path, **kwargs):
        response = await self.make_request(
            'GET',
            self.build_url('revisions', 'auto', path.full_path, rev_limit=250),
            expects=(200, ),
            throws=exceptions.RevisionsError
        )
        data = await response.json()

        return [
            DropboxRevision(item)
            for item in data
            if not item.get('is_deleted')
        ]

    async def create_folder(self, path, **kwargs):
        """
        :param str path: The path to create a folder at
        """
        WaterButlerPath.validate_folder(path)

        response = await self.make_request(
            'POST',
            self.build_url('fileops', 'create_folder'),
            params={
                'root': 'auto',
                'path': path.full_path
            },
            expects=(200, 403),
            throws=exceptions.CreateFolderError
        )

        data = await response.json()

        if response.status == 403:
            if 'because a file or folder already exists at path' in data.get('error'):
                raise exceptions.FolderNamingConflict(str(path))
            raise exceptions.CreateFolderError(data, code=403)

        return DropboxFolderMetadata(data, self.folder)

    def can_intra_copy(self, dest_provider, path=None):
        return type(self) == type(dest_provider)

    def can_intra_move(self, dest_provider, path=None):
        return self == dest_provider

    def _build_content_url(self, *segments, **query):
        return provider.build_url(settings.BASE_CONTENT_URL, *segments, **query)
