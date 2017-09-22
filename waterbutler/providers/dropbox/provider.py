import json
import typing
from http import HTTPStatus

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.dropbox import settings
from waterbutler.providers.dropbox.metadata import (BaseDropboxMetadata,
                                                    DropboxFileMetadata,
                                                    DropboxFolderMetadata,
                                                    DropboxRevision)
from waterbutler.providers.dropbox.exceptions import (DropboxNamingConflictError,
                                                      DropboxUnhandledConflictError)


class DropboxProvider(provider.BaseProvider):
    """Provider for the Dropbox.com cloud storage service.

    This provider uses the v2 Dropbox API. The v2 API assigns IDs to files and folders, but not all
    endpoints currently support IDs. Dropbox WaterButlerPath objects will continue to use string
    paths until they do. As of Nov. 3, 2016, endpoint ID support is classified as follows.

    Can use ID as path::

        /files/get_metadata
        /files/copy_reference/get
        /files/download
        /files/list_revisions

    Cannot use ID as path::

        /files/copy
        /files/copy_reference/save
        /files/move
        /files/upload
        /files/delete
        /files/list_folder
        /files/create_folder

    Does not use path::

        /files/list_folder/continue

    API docs: https://www.dropbox.com/developers/documentation/http/documentation

    Quirks:

    * Dropbox paths are case-insensitive.
    """
    NAME = 'dropbox'
    BASE_URL = settings.BASE_URL

    def __init__(self, auth, credentials, settings):
        super().__init__(auth, credentials, settings)
        self.token = self.credentials['token']
        self.folder = self.settings['folder']
        self.metrics.add('folder_is_root', self.folder == '/')

    async def dropbox_request(self,
                              url: str,
                              body: dict,
                              expects: typing.Tuple=(200, 409,),
                              *args,
                              **kwargs) -> dict:
        """Convenience wrapper around ``BaseProvider.request`` for simple Dropbox API calls. Sets
        the method to ``POST``, jsonifies the ``body`` param, and provides default error handling
        for Dropbox's standard 409 error response structure.

        :param str url: the url of the endpoint to POST to
        :param dict body: the data to send in the request body, will be jsonified
        :param tuple expects: expected error codes, defaults to 200 (success) and 409 (error)
        :param tuple \*args: passed through to BaseProvider.request()
        :param dict \*\*kwargs: passed through to BaseProvider.request()
        """
        async with self.request(
            'POST',
            url,
            data=json.dumps(body),
            expects=expects,
            *args,
            **kwargs,
        ) as resp:
            data = await resp.json()
            if resp.status == 409:
                self.dropbox_conflict_error_handler(data, body.get('path', ''))
            return data

    def dropbox_conflict_error_handler(self, data: dict, error_path: str='') -> None:
        """Takes a standard Dropbox error response and an optional path and tries to throw a
        meaningful error based on it.

        :param dict data: the error received from Dropbox
        :param str error_path: the path where the error occurred. Base folder will be stripped.
        """

        if error_path.startswith(self.folder):
            error_path = error_path[len(self.folder):]
        if not error_path.startswith('/'):
            error_path = '/{}'.format(error_path)

        if 'error' in data:
            error_class = data['error']['.tag']
            if error_class in data['error']:
                error_type = data['error'][error_class]
                if error_type['.tag'] == 'not_found':
                    raise exceptions.NotFoundError(error_path)
                if 'conflict' in error_type:
                    raise DropboxNamingConflictError(error_path)
            if data['error'].get('reason', False) and 'conflict' in data['error']['reason']['.tag']:
                raise DropboxNamingConflictError(error_path)
        raise DropboxUnhandledConflictError(str(data))

    async def validate_v1_path(self, path: str, **kwargs) -> WaterButlerPath:
        if path == '/':
            return WaterButlerPath(path, prepend=self.folder)
        implicit_folder = path.endswith('/')
        data = await self.dropbox_request(
            self.build_url('files', 'get_metadata'),
            {'path': self.folder.rstrip('/') + path.rstrip('/')},
            throws=exceptions.MetadataError,
        )
        explicit_folder = data['.tag'] == 'folder'
        if explicit_folder != implicit_folder:
            raise exceptions.NotFoundError(str(path))
        return WaterButlerPath(path, prepend=self.folder)

    async def validate_path(self, path: str, **kwargs) -> WaterButlerPath:
        return WaterButlerPath(path, prepend=self.folder)

    def can_duplicate_names(self) -> bool:
        return False

    def shares_storage_root(self, other: provider.BaseProvider) -> bool:
        """Dropbox settings only include the root folder. If a cross-resource move occurs
        between two dropbox providers that are on different accounts but have the same folder
        base name, the parent method could incorrectly think the action is a self-overwrite.
        Comparing credentials means that this is unique per connected account."""
        return super().shares_storage_root(other) and self.credentials == other.credentials

    @property
    def default_headers(self) -> dict:
        return {'Authorization': 'Bearer {}'.format(self.token),
                'Content-Type': 'application/json'}

    async def intra_copy(self,  # type: ignore
                         dest_provider: 'DropboxProvider',
                         src_path: WaterButlerPath,
                         dest_path: WaterButlerPath) \
            -> typing.Tuple[typing.Union[DropboxFileMetadata, DropboxFolderMetadata], bool]:
        dest_folder = dest_provider.folder
        try:
            if self == dest_provider:
                data = await self.dropbox_request(
                    self.build_url('files', 'copy'),
                    {
                        'from_path': src_path.full_path.rstrip('/'),
                        'to_path': dest_path.full_path.rstrip('/'),
                    },
                    expects=(200, 201, 409),
                    throws=exceptions.IntraCopyError,
                )
            else:
                from_ref_data = await self.dropbox_request(
                    self.build_url('files', 'copy_reference', 'get'),
                    {'path': src_path.full_path.rstrip('/')},
                    throws=exceptions.IntraCopyError,
                )
                from_ref = from_ref_data['copy_reference']

                data = await dest_provider.dropbox_request(
                    self.build_url('files', 'copy_reference', 'save'),
                    {'copy_reference': from_ref, 'path': dest_path.full_path.rstrip('/')},
                    expects=(200, 201, 409),
                    throws=exceptions.IntraCopyError,
                )
                data = data['metadata']
        except DropboxNamingConflictError:
            await dest_provider.delete(dest_path)
            resp, _ = await self.intra_copy(dest_provider, src_path, dest_path)
            return resp, False

        if data['.tag'] == 'file':
            return DropboxFileMetadata(data, dest_folder), True
        folder = DropboxFolderMetadata(data, dest_folder)
        folder.children = [item for item in await dest_provider.metadata(dest_path)]  # type: ignore
        return folder, True

    async def intra_move(self,  # type: ignore
                         dest_provider: 'DropboxProvider',
                         src_path: WaterButlerPath,
                         dest_path: WaterButlerPath) -> typing.Tuple[BaseDropboxMetadata, bool]:
        if dest_path.full_path.lower() == src_path.full_path.lower():
            # Dropbox does not support changing the casing in a file name
            raise exceptions.InvalidPathError(
                'In Dropbox to change case, add or subtract other characters.')

        try:
            data = await self.dropbox_request(
                self.build_url('files', 'move'),
                {
                    'from_path': src_path.full_path.rstrip('/'),
                    'to_path': dest_path.full_path.rstrip('/'),
                },
                expects=(200, 201, 409),
                throws=exceptions.IntraMoveError,
            )
        except DropboxNamingConflictError:
            await dest_provider.delete(dest_path)
            resp, _ = await self.intra_move(dest_provider, src_path, dest_path)
            return resp, False

        dest_folder = dest_provider.folder
        if data['.tag'] == 'file':
            return DropboxFileMetadata(data, dest_folder), True
        folder = DropboxFolderMetadata(data, dest_folder)
        folder.children = [item for item in await dest_provider.metadata(dest_path)]  # type: ignore
        return folder, True

    async def download(self,  # type: ignore
                       path: WaterButlerPath,
                       revision: str=None,
                       range: typing.Tuple[int, int]=None,
                       **kwargs) -> streams.ResponseStreamReader:
        path_arg = {"path": ("rev:" + revision if revision else path.full_path)}
        resp = await self.make_request(
            'POST',
            self._build_content_url('files', 'download'),
            headers={'Dropbox-API-Arg': json.dumps(path_arg), 'Content-Type': ''},
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
            size = None  # ResponseStreamReader will extract it from the resp
        return streams.ResponseStreamReader(resp, size=size)

    async def upload(self,  # type: ignore
                     stream: streams.BaseStream,
                     path: WaterButlerPath,
                     conflict: str='replace',
                     **kwargs) -> typing.Tuple[DropboxFileMetadata, bool]:
        path, exists = await self.handle_name_conflict(path, conflict=conflict)
        path_arg = {"path": path.full_path}
        if conflict == 'replace':
            path_arg['mode'] = 'overwrite'

        resp = await self.make_request(
            'POST',
            self._build_content_url('files', 'upload'),
            headers={
                'Content-Type': 'application/octet-stream',
                'Dropbox-API-Arg': json.dumps(path_arg),
                'Content-Length': str(stream.size),
            },
            data=stream,
            expects=(200, 409,),
            throws=exceptions.UploadError,
        )
        data = await resp.json()
        if resp.status == 409:
            self.dropbox_conflict_error_handler(data, path.path)
        return DropboxFileMetadata(data, self.folder), not exists

    async def delete(self, path: WaterButlerPath, confirm_delete: int=0,  # type: ignore
                     **kwargs) -> None:  # type: ignore
        """Delete file, folder, or provider root contents

        :param WaterButlerPath path: WaterButlerPath path object for folder
        :param int confirm_delete: Must be 1 to confirm root folder delete
        """
        if path.is_root:
            if confirm_delete == 1:
                return await self._delete_folder_contents(path)
            else:
                raise exceptions.DeleteError(
                    'confirm_delete=1 is required for deleting root provider folder',
                    code=400
                )
        await self.dropbox_request(
            self.build_url('files', 'delete'),
            {'path': self.folder.rstrip('/') + '/' + path.path.rstrip('/')},
            throws=exceptions.DeleteError,
        )

    async def metadata(self,  # type: ignore
                       path: WaterButlerPath,
                       revision: str=None,
                       **kwargs) \
                       -> typing.Union[BaseDropboxMetadata, typing.List[BaseDropboxMetadata]]:
        full_path = path.full_path.rstrip('/')
        url = self.build_url('files', 'get_metadata')
        body = {'path': full_path}
        if revision:
            body = {'path': 'rev:' + revision}
        elif path.is_folder:
            url = self.build_url('files', 'list_folder')

        if path.is_folder:
            ret = []  # type: typing.List[BaseDropboxMetadata]
            has_more = True
            page_count = 0
            while has_more:
                page_count += 1
                data = await self.dropbox_request(url, body, throws=exceptions.MetadataError)
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
            self.metrics.add('metadata.folder.pages', page_count)
            return ret

        data = await self.dropbox_request(url, body, throws=exceptions.MetadataError)
        # Dropbox v2 API will not indicate file/folder if path "deleted"
        if data['.tag'] == 'deleted':
            raise exceptions.MetadataError(
                "Could not retrieve '{}'".format(path),
                code=HTTPStatus.NOT_FOUND,
            )

        # Dropbox will match a file or folder by name within the requested path
        if path.is_file and data['.tag'] == 'folder':
            raise exceptions.MetadataError(
                "Could not retrieve file '{}'".format(path),
                code=HTTPStatus.NOT_FOUND,
            )

        return DropboxFileMetadata(data, self.folder)

    async def revisions(self, path: WaterButlerPath, **kwargs) -> typing.List[DropboxRevision]:
        # Dropbox v2 API limits the number of revisions returned to a maximum
        # of 100, default 10. Previously we had set the limit to 250.

        data = await self.dropbox_request(
            self.build_url('files', 'list_revisions'),
            {'path': path.full_path.rstrip('/'), 'limit': 100},
            throws=exceptions.RevisionsError,
        )
        if data['is_deleted'] is True:
            raise exceptions.RevisionsError(
                "Could not retrieve '{}'".format(path),
                code=HTTPStatus.NOT_FOUND,
            )
        if data['is_deleted']:
            return []
        return [DropboxRevision(item) for item in data['entries']]

    async def create_folder(self, path: WaterButlerPath, **kwargs) -> DropboxFolderMetadata:
        """
        :param str path: The path to create a folder at
        """
        WaterButlerPath.validate_folder(path)
        data = await self.dropbox_request(
            self.build_url('files', 'create_folder'),
            {'path': path.full_path.rstrip('/')},
            throws=exceptions.CreateFolderError,
        )
        return DropboxFolderMetadata(data, self.folder)

    def can_intra_copy(self, dest_provider: provider.BaseProvider,
                       path: WaterButlerPath=None) -> bool:
        return type(self) == type(dest_provider)

    def can_intra_move(self, dest_provider: provider.BaseProvider,
                       path: WaterButlerPath=None) -> bool:
        return self == dest_provider  # dropbox can only intra move on same account

    def _build_content_url(self, *segments, **query):
        return provider.build_url(settings.BASE_CONTENT_URL, *segments, **query)

    async def _delete_folder_contents(self, path: WaterButlerPath, **kwargs) -> None:
        """Delete the contents of a folder. For use against provider root.

        :param WaterButlerPath path: WaterButlerPath path object for folder
        """
        meta = (await self.metadata(path))
        for child in meta:  # type: ignore
            dropbox_path = await self.validate_path(child.path)
            await self.delete(dropbox_path)
