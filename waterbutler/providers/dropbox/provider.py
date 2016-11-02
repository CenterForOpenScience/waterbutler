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
from waterbutler.providers.dropbox.exceptions import DropboxNamingConflictError
from waterbutler.providers.dropbox.exceptions import DropboxUnhandledConflictError


class DropboxProvider(provider.BaseProvider):
    """Provider for the Dropbox.com cloud storage service.

    This provider uses the v2 Dropbox API. Files and folders are assigned IDs and some API calls can use IDs in place of paths.
    But this has not yet been implemented.

    API docs: https://www.dropbox.com/developers/documentation/http/documentation

    Quirks:

    * Dropbox is case-insensitive.
    """
    NAME = 'dropbox'
    BASE_URL = settings.BASE_URL

    def __init__(self, auth, credentials, settings):
        super().__init__(auth, credentials, settings)
        self.token = self.credentials['token']
        self.folder = self.settings['folder']

    async def dropbox_request(self, url, body, throws,
                              expects=(200, 409,), *args, **kwargs):
        if 'path' in body:
            error_path = '/{}'.format(body['path'].lstrip(self.folder))
        else:
            error_path = ''
        try:
            async with self.request(
                'POST',
                url,
                headers={},
                data=json.dumps(body),
                expects=expects,
                throws=throws
            ) as resp:
                data = await resp.json()
                if resp.status == 409:
                    self.dropbox_conflict_error_handler(data, error_path)
        except:
            raise
        return data

    def dropbox_conflict_error_handler(self, data, error_path=''):
        if 'error' in data:
            if data['error']['.tag'] in data['error']:
                error_tag = data['error'][data['error']['.tag']]
                if error_tag['.tag'] == 'not_found':
                    raise exceptions.NotFoundError(error_path)
                if 'conflict' in error_tag:
                    raise DropboxNamingConflictError(data['error_summary'])
            if 'conflict' in data['error']['reason']['.tag']:
                raise DropboxNamingConflictError('{} for path {}'.format(data['error_summary'], error_path))
        raise DropboxUnhandledConflictError(str(data))

    async def validate_v1_path(self, path, **kwargs):
        if path == '/':
            return WaterButlerPath(path, prepend=self.folder)
        implicit_folder = path.endswith('/')
        data = await self.dropbox_request(self.build_url('files',
                                                         'get_metadata'),
                                          {'path': self.folder.rstrip('/') + path.rstrip('/')},
                                          exceptions.MetadataError)
        explicit_folder = data['.tag'] == 'folder'
        if explicit_folder != implicit_folder:
            raise exceptions.NotFoundError(str(path))
        return WaterButlerPath(path, prepend=self.folder)

    async def validate_path(self, path, **kwargs):
        return WaterButlerPath(path, prepend=self.folder)

    def can_duplicate_names(self):
        return False

    def shares_storage_root(self, other):
        """Dropbox settings only include the root folder. If a cross-resource move occurs
        between two dropbox providers that are on different accounts but have the same folder
        base name, the parent method could incorrectly think the action is a self-overwrite.
        Comparing credentials means that this is unique per connected account."""
        return super().shares_storage_root(other) and self.credentials == other.credentials

    @property
    def default_headers(self):
        return {'Authorization': 'Bearer {}'.format(self.token),
                'Content-Type': 'application/json'}

    async def intra_copy(self, dest_provider, src_path, dest_path):
        dest_folder = dest_provider.folder
        throws = exceptions.IntraCopyError
        try:
            if self == dest_provider:
                data = await self.dropbox_request(self.build_url('files', 'copy'),
                                                  {'from_path': src_path.full_path.rstrip('/'),
                                                   'to_path': dest_path.full_path.rstrip('/')},
                                                  throws,
                                                  expects=(200, 201, 409))
            else:
                from_ref_data = await self.dropbox_request(self.build_url('files', 'copy_reference', 'get'),
                                                           {'path': src_path.full_path.rstrip('/')},
                                                           throws)
                from_ref = from_ref_data['copy_reference']

                data = await dest_provider.dropbox_request(self.build_url('files', 'copy_reference', 'save'),
                                                           {'copy_reference': from_ref,
                                                            'path': dest_path.full_path.rstrip('/')},
                                                           throws,
                                                           expects=(200, 201, 409))
                data = data['metadata']
        except DropboxNamingConflictError:
            await dest_provider.delete(dest_path)
            resp, _ = await self.intra_copy(dest_provider, src_path, dest_path)
            return resp, False

        if data['.tag'] == 'file':
            return DropboxFileMetadata(data, dest_folder), True
        folder = DropboxFolderMetadata(data, dest_folder)
        folder.children = [item for item in await dest_provider.metadata(dest_path)]
        return folder, True

    async def intra_move(self, dest_provider, src_path, dest_path):
        if dest_path.full_path.lower() == src_path.full_path.lower():
            # Dropbox does not support changing the casing in a file name
            raise exceptions.InvalidPathError('In Dropbox to change case, add or subtract other characters.')

        try:
            data = await self.dropbox_request(self.build_url('files', 'move'),
                                              {'from_path': src_path.full_path.rstrip('/'),
                                               'to_path': dest_path.full_path.rstrip('/')},
                                              exceptions.IntraMoveError)
        except DropboxNamingConflictError:
            await dest_provider.delete(dest_path)
            resp, _ = await self.intra_move(dest_provider, src_path, dest_path)
            return resp, False

        dest_folder = dest_provider.folder
        if data['.tag'] == 'file':
            return DropboxFileMetadata(data, dest_folder), True
        folder = DropboxFolderMetadata(data, dest_folder)
        folder.children = [item for item in await dest_provider.metadata(dest_path)]
        return folder, True

    async def download(self, path, revision=None, range=None, **kwargs):
        if revision:
            path_arg = json.dumps({"path": "rev:" + revision})
        else:
            path_arg = json.dumps({"path": path.full_path})
        url = self._build_content_url('files', 'download')
        resp = await self.make_request(
            'POST',
            url,
            headers={'Dropbox-API-Arg': path_arg,
                     'Content-Type': ''},
            range=range,
            expects=(200, 206, 409,),
            throws=exceptions.DownloadError,
        )
        if resp.status == 409:
            data = await resp.json()
            self.dropbox_conflict_error_handler(data)
        if 'Content-Length' not in resp.headers:
            size = json.loads(resp.headers['dropbox-api-result'])['size']
        else:
            size = None
        return streams.ResponseStreamReader(resp, size=size)

    async def upload(self, stream, path, conflict='replace', **kwargs):
        path, exists = await self.handle_name_conflict(path, conflict=conflict)
        url = self._build_content_url('files', 'upload')
        path_arg = json.dumps({"path": path.full_path})
        if conflict == 'replace':
            path_arg = json.dumps({"path": path.full_path, "mode": "overwrite"})
        headers = {'Content-Type': 'application/octet-stream',
                   'Dropbox-API-Arg': path_arg,
                   'Content-Length': str(stream.size)}
        resp = await self.make_request(
            'POST',
            url,
            headers=headers,
            data=stream,
            expects=(200, 409,),
            throws=exceptions.UploadError,
        )
        data = await resp.json()
        if resp.status == 409:
            self.dropbox_conflict_error_handler(data, path.path)
        return DropboxFileMetadata(data, self.folder), not exists

    async def delete(self, path, confirm_delete=0, **kwargs):
        """Delete file, folder, or provider root contents

        :param DropboxPath path: DropboxPath path object for folder
        :param int confirm_delete: Must be 1 to confirm root folder delete
        """
        if path.is_root:
            if confirm_delete == 1:
                await self._delete_folder_contents(path)
                return
            else:
                raise exceptions.DeleteError(
                    'confirm_delete=1 is required for deleting root provider folder',
                    code=400
                )
        await self.dropbox_request(self.build_url('files', 'delete'),
                                   {'path': self.folder.rstrip('/') + '/' + path.path.rstrip('/')},
                                   exceptions.DeleteError)

    async def metadata(self, path, revision=None, **kwargs):
        full_path = path.full_path.rstrip('/')
        throws = exceptions.MetadataError,
        url = self.build_url('files', 'get_metadata')
        body = {'path': full_path}
        if revision:
            body = {'path': 'rev:' + revision}
        elif path.is_folder:
            url = self.build_url('files', 'list_folder')

        if path.is_folder:
            ret = []
            has_more = True
            while has_more:
                data = await self.dropbox_request(url, body, throws)
                for entry in data['entries']:
                    if entry['.tag'] == 'folder':
                        ret.append(DropboxFolderMetadata(entry, self.folder))
                    else:
                        ret.append(DropboxFileMetadata(entry, self.folder))
                if not data['has_more']:
                    has_more = False
                else:
                    url = self.build_url('files', 'list_folder', 'continue')
                    body = {'cursor': data['cursor']}
            return ret

        data = await self.dropbox_request(url, body, throws)
        # Dropbox v2 API will not indicate file/folder if path "deleted"
        if data['.tag'] == 'deleted':
            raise exceptions.MetadataError(
                "Could not retrieve '{}'".format(path),
                code=http.client.NOT_FOUND,
            )

        # Dropbox will match a file or folder by name within the requested path
        if path.is_file and data['.tag'] == 'folder':
            raise exceptions.MetadataError(
                "Could not retrieve file '{}'".format(path),
                code=http.client.NOT_FOUND,
            )

        return DropboxFileMetadata(data, self.folder)

    async def revisions(self, path, **kwargs):
        # Dropbox v2 API limits the number of revisions returned to a maximum
        # of 100, default 10. Previously we had set the limit to 250.

        data = await self.dropbox_request(self.build_url('files', 'list_revisions'),
                                          {'path': path.full_path.rstrip('/'),
                                           'limit': 100},
                                          exceptions.RevisionsError)
        if data['is_deleted'] is True:
            raise exceptions.RevisionsError(
                "Could not retrieve '{}'".format(path),
                code=http.client.NOT_FOUND,
            )
        if data['is_deleted']:
            return []
        return [DropboxRevision(item) for item in data['entries']]

    async def create_folder(self, path, **kwargs):
        """
        :param str path: The path to create a folder at
        """
        WaterButlerPath.validate_folder(path)
        data = await self.dropbox_request(self.build_url('files', 'create_folder'),
                                          {'path': path.full_path.rstrip('/')},
                                          exceptions.CreateFolderError)
        return DropboxFolderMetadata(data, self.folder)

    def can_intra_copy(self, dest_provider, path=None):
        return type(self) == type(dest_provider)

    def can_intra_move(self, dest_provider, path=None):
        return self == dest_provider

    def _build_content_url(self, *segments, **query):
        return provider.build_url(settings.BASE_CONTENT_URL, *segments, **query)

    async def _delete_folder_contents(self, path, **kwargs):
        """Delete the contents of a folder. For use against provider root.

        :param DropboxPath path: DropboxPath path object for folder
        """
        meta = (await self.metadata(path))
        for child in meta:
            drop_box_path = await self.validate_path(child.path)
            await self.delete(drop_box_path)
