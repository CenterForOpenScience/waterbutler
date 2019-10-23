import json
import typing
import logging
from http import HTTPStatus

from waterbutler.core import provider, streams
from waterbutler.core.path import WaterButlerPath
from waterbutler.core import exceptions as core_exceptions

from waterbutler.providers.dropbox import settings as pd_settings
from waterbutler.providers.dropbox import exceptions as pd_exceptions
from waterbutler.providers.dropbox.metadata import (DropboxRevision,
                                                    BaseDropboxMetadata,
                                                    DropboxFileMetadata,
                                                    DropboxFolderMetadata, )

logger = logging.getLogger(__name__)


class DropboxProvider(provider.BaseProvider):
    """Provider for the Dropbox.com cloud storage service.

    This provider uses the v2 Dropbox API. The v2 API assigns IDs to files and folders, but not all
    endpoints currently support IDs. Dropbox WaterButlerPath objects will continue to use string
    paths until they do. As of Jan. 2, 2018, endpoint ID support is classified as follows.

    Can use ID as path::

        /files/get_metadata
        /files/copy_reference/get
        /files/download
        /files/list_revisions
        /files/copy_v2
        /files/move_v2
        /files/delete_v2
        /files/list_folder

    Cannot use ID as path::

        /files/upload
        /files/copy_reference/save
        /files/create_folder_v2

    Does not use path::

        /files/list_folder/continue

    Deprecated API Update (as of Dec. 26th, 2017)::

        /files/copy             --->    /files/copy_v2
        /files/move             --->    /files/move_v2
        /files/delete           --->    /files/delete_v2
        /files/create_folder    --->    /files/create_folder_v2

    API docs: https://www.dropbox.com/developers/documentation/http/documentation

    Quirks: Dropbox paths are case-insensitive.
    """
    NAME = 'dropbox'
    BASE_URL = pd_settings.BASE_URL
    CONTIGUOUS_UPLOAD_SIZE_LIMIT = pd_settings.CONTIGUOUS_UPLOAD_SIZE_LIMIT
    CHUNK_SIZE = pd_settings.CHUNK_SIZE

    def __init__(self, auth, credentials, settings, **kwargs):
        super().__init__(auth, credentials, settings, **kwargs)
        self.token = self.credentials['token']
        self.folder = self.settings['folder']
        self.metrics.add('folder_is_root', self.folder == '/')

    async def dropbox_request(self,
                              url: str,
                              body: dict,
                              expects: typing.Tuple=(200, 409,),
                              *args,
                              **kwargs) -> dict:
        r"""Convenience wrapper around ``BaseProvider.make_request`` for simple Dropbox API calls.
        Sets the method to ``POST``, jsonifies the ``body`` param, and provides default error
        handling for Dropbox's standard 409 error response structure.

        Note: This ``.dropbox_request()`` doesn't fit all requests. For example, download doesn't
              expect a JSON response.  In addition, this wrapper shouldn't be applied to requests
              that clearly don't have the 409 problem even if they have a JSON response.

        TODO: review all ``.dropbox_request()`` and ``.make_request()`` usages in the provider and
              make sure each uses either one properly.  Make changes if necessary.

        :param str url: the url of the endpoint to POST to
        :param dict body: the data to send in the request body, will be jsonified
        :param tuple expects: expected error codes, defaults to 200 (success) and 409 (error)
        :param tuple \*args: passed through to BaseProvider.make_request()
        :param dict \*\*kwargs: passed through to BaseProvider.make_request()
        """
        resp = await self.make_request(
            'POST',
            url,
            data=json.dumps(body),
            expects=expects,
            *args,
            **kwargs,
        )
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
                    raise core_exceptions.NotFoundError(error_path)
                if 'conflict' in error_type:
                    raise pd_exceptions.DropboxNamingConflictError(error_path)
            if data['error'].get('reason', False) and 'conflict' in data['error']['reason']['.tag']:
                raise pd_exceptions.DropboxNamingConflictError(error_path)
        raise pd_exceptions.DropboxUnhandledConflictError(str(data))

    async def validate_v1_path(self, path: str, **kwargs) -> WaterButlerPath:
        if path == '/':
            return WaterButlerPath(path, prepend=self.folder)
        implicit_folder = path.endswith('/')
        data = await self.dropbox_request(
            self.build_url('files', 'get_metadata'),
            {'path': self.folder.rstrip('/') + path.rstrip('/')},
            throws=core_exceptions.MetadataError,
        )
        explicit_folder = data['.tag'] == 'folder'
        if explicit_folder != implicit_folder:
            raise core_exceptions.NotFoundError(str(path))
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
                    self.build_url('files', 'copy_v2'),
                    {
                        'from_path': src_path.full_path.rstrip('/'),
                        'to_path': dest_path.full_path.rstrip('/'),
                    },
                    expects=(200, 201, 409),
                    throws=core_exceptions.IntraCopyError,
                )
            else:
                from_ref_data = await self.dropbox_request(
                    self.build_url('files', 'copy_reference', 'get'),
                    {'path': src_path.full_path.rstrip('/')},
                    throws=core_exceptions.IntraCopyError,
                )
                from_ref = from_ref_data['copy_reference']

                data = await dest_provider.dropbox_request(
                    self.build_url('files', 'copy_reference', 'save'),
                    {'copy_reference': from_ref, 'path': dest_path.full_path.rstrip('/')},
                    expects=(200, 201, 409),
                    throws=core_exceptions.IntraCopyError,
                )
            data = data['metadata']
        except pd_exceptions.DropboxNamingConflictError:
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
            raise core_exceptions.InvalidPathError(
                'In Dropbox to change case, add or subtract other characters.')

        try:
            data = await self.dropbox_request(
                self.build_url('files', 'move_v2'),
                {
                    'from_path': src_path.full_path.rstrip('/'),
                    'to_path': dest_path.full_path.rstrip('/'),
                },
                expects=(200, 201, 409),
                throws=core_exceptions.IntraMoveError,
            )
            data = data['metadata']
        except pd_exceptions.DropboxNamingConflictError:
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
        """
        Dropbox V2 API Files Download
        https://www.dropbox.com/developers/documentation/http/documentation#files-download

        Request and Response Format: Content-download endpoints
        https://www.dropbox.com/developers/documentation/http/documentation#formats

        According to Dropbox's API docs for files download and content-download endpoints, the file
        content is contained in the response body and the result (metadata about the file) appears
        as JSON in the "Dropbox-API-Result" response header.  As far as the WB Dropbox provider is
        concerned, the header contains the size (in bytes) of the file that ``ResponseStreamReader``
        needs if the "Content-Length" header is not provided.
        """
        path_arg = {"path": ("rev:" + revision if revision else path.full_path)}
        resp = await self.make_request(
            'POST',
            self._build_content_url('files', 'download'),
            headers={'Dropbox-API-Arg': json.dumps(path_arg), 'Content-Type': ''},
            range=range,
            expects=(200, 206, 409,),
            throws=core_exceptions.DownloadError,
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
        """Upload file stream to Dropbox.  If file exceeds `CONTIGUOUS_UPLOAD_SIZE_LIMIT`, Dropbox's
        multipart upload endpoints will be used.
        """
        path, exists = await self.handle_name_conflict(path, conflict=conflict)

        if stream.size > self.CONTIGUOUS_UPLOAD_SIZE_LIMIT:
            data = await self._chunked_upload(stream, path, conflict=conflict)
        else:
            data = await self._contiguous_upload(stream, path, conflict=conflict)

        return DropboxFileMetadata(data, self.folder), not exists

    async def _contiguous_upload(self,
                                 stream: streams.BaseStream,
                                 path: WaterButlerPath,
                                 conflict: str='replace') -> dict:
        """Upload file in a single request.

        API Docs: https://www.dropbox.com/developers/documentation/http/documentation#files-upload

        :param stream: the stream to upload
        :param path: the WB path of the file
        :param conflict: whether to replace upon conflict
        :rtype: `dict`
        :return: A dictionary of the metadata about the file just uploaded
        """

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
            throws=core_exceptions.UploadError,
        )

        data = await resp.json()
        if resp.status == 409:
            self.dropbox_conflict_error_handler(data, path.path)
        return data

    async def _chunked_upload(self, stream: streams.BaseStream, path: WaterButlerPath,
                              conflict: str='replace') -> dict:
        """Chunked uploading is a 3-step process using Dropbox's "Upload Session".

        First, start a new upload session and receive an upload session ID.
        API Docs: https://www.dropbox.com/developers/documentation/http/documentation#files-upload_session-start

        Then, split the file into multiple chunks and upload them across multiple requests.
        API Docs: https://www.dropbox.com/developers/documentation/http/documentation#files-upload_session-append

        Finally, when all of the parts have finished uploading, send a complete session request to
        let Dropbox combine the uploaded data and save it to the given file path.
        API Docs: https://www.dropbox.com/developers/documentation/http/documentation#files-upload_session-finish

        Quirks:
        1. A single request should not upload more than 150 MB.
        2. The maximum size of a file one can upload to an upload session is 350 GB.
        3. An upload session can be used for a maximum of 48 hours.
        """

        # 1. Create an upload session and retrieves the session id to upload parts.
        session_id = await self._create_upload_session()

        # 2. Upload all parts in the session
        await self._upload_parts(stream, session_id)

        # 3. Complete the session and return the uploaded file's metadata.
        return await self._complete_session(stream, session_id, path, conflict=conflict)

    async def _create_upload_session(self) -> str:
        """Create an upload session for chunked upload.

        "Upload sessions allow you to upload a single file in one or more requests, for example
        where the size of the file is greater than 150 MB. This call starts a new upload session
        with the given data."

        API Docs: https://www.dropbox.com/developers/documentation/http/documentation#files-upload_session-start

        :rtype: str
        :return: session identifier
        """

        resp = await self.make_request(
            'POST',
            self._build_content_url('files', 'upload_session', 'start'),
            headers={
                'Content-Type': 'application/octet-stream',
                'Dropbox-API-Arg': json.dumps({'close': False}),
            },
            expects=(200, ),
            throws=core_exceptions.UploadError
        )
        data = await resp.json()
        return data['session_id']

    async def _upload_parts(self, stream: streams.BaseStream, session_id: str) -> None:
        """Determines the necessary partitioning of the stream (based on max chunk size), and
        calls `_upload_part` for each partition.
        """

        upload_args = {
            'close': False,
            'cursor': {'session_id': session_id, 'offset': 0, }
        }

        parts = [self.CHUNK_SIZE for _ in range(0, stream.size // self.CHUNK_SIZE)]
        if stream.size % self.CHUNK_SIZE:
            parts.append(stream.size - (len(parts) * self.CHUNK_SIZE))
        logger.debug('Chunked upload segment sizes: {}'.format(parts))

        last_chunk_size = 0
        for chunk_id, chunk_size in enumerate(parts):
            # Calculates the the ``offset`` that is required for ``/chunked_upload`` and that
            # represents the number of bytes transferred so far. If the offset does not match the
            # expected offset on the server, the server will ignore the request and respond with a
            # 400 error that includes the current offset.
            upload_args['cursor']['offset'] += last_chunk_size  # type: ignore
            logger.debug('  uploading part {} with size {} starting at offset '
                         '{}'.format(chunk_id + 1, chunk_size,
                                     upload_args['cursor']['offset']))  # type: ignore
            await self._upload_part(stream, chunk_size, upload_args)
            last_chunk_size = chunk_size

    async def _upload_part(self, stream: streams.BaseStream,
                           chunk_size: int, upload_args: dict) -> None:
        """Upload one part/chunk of the given stream to Dropbox

        "Append more data to an upload session. When the parameter close is set, this call will
        close the session. A single request should not upload more than 150 MB. ..."

        API Docs: https://www.dropbox.com/developers/documentation/http/documentation#files-upload_session-append
        """

        cutoff_stream = streams.CutoffStream(stream, cutoff=chunk_size)

        resp = await self.make_request(
            'POST',
            self._build_content_url('files', 'upload_session', 'append_v2'),
            headers={
                # ``Content-Length`` is required for ``asyncio`` to use inner chunked stream read
                'Content-Length': str(chunk_size),
                'Content-Type': 'application/octet-stream',
                'Dropbox-API-Arg': json.dumps(upload_args),
            },
            data=cutoff_stream,
            expects=(200, ),
            throws=core_exceptions.UploadError
        )

        await resp.release()

    async def _complete_session(self, stream: streams.BaseStream, session_id: str,
                                path: WaterButlerPath, conflict: str='replace') -> dict:
        """Complete the chunked upload session.

        "Finish an upload session and save the uploaded data to the given file path. ... The maximum
        size of a file one can upload to an upload session is 350 GB."

        API Docs: https://www.dropbox.com/developers/documentation/http/documentation#files-upload_session-finish

        :param stream: the stream to upload
        :param session_id: the ID of the chunked upload session
        :param path: the WB path of the file
        :param conflict: whether to replace upon conflict
        :rtype: `dict`
        :return: A dictionary of the metadata about the file just uploaded
        """

        upload_args = {
            'cursor': {'session_id': session_id, 'offset': stream.size, },
            'commit': {"path": path.full_path, },
        }
        if conflict == 'replace':
            upload_args['commit']['mode'] = 'overwrite'

        resp = await self.make_request(
            'POST',
            self._build_content_url('files', 'upload_session', 'finish'),
            headers={
                'Content-Type': 'application/octet-stream',
                'Dropbox-API-Arg': json.dumps(upload_args),
            },
            expects=(200, ),
            throws=core_exceptions.UploadError,
        )

        return await resp.json()

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
                raise core_exceptions.DeleteError(
                    'confirm_delete=1 is required for deleting root provider folder',
                    code=400
                )
        await self.dropbox_request(
            self.build_url('files', 'delete_v2'),
            {'path': self.folder.rstrip('/') + '/' + path.path.rstrip('/')},
            throws=core_exceptions.DeleteError,
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
                data = await self.dropbox_request(url, body, throws=core_exceptions.MetadataError)
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

        data = await self.dropbox_request(url, body, throws=core_exceptions.MetadataError)
        # Dropbox v2 API will not indicate file/folder if path "deleted"
        if data['.tag'] == 'deleted':
            raise core_exceptions.MetadataError(
                "Could not retrieve '{}'".format(path),
                code=HTTPStatus.NOT_FOUND,
            )

        # Dropbox will match a file or folder by name within the requested path
        if path.is_file and data['.tag'] == 'folder':
            raise core_exceptions.MetadataError(
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
            throws=core_exceptions.RevisionsError,
        )
        if data['is_deleted'] is True:
            raise core_exceptions.RevisionsError(
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
            self.build_url('files', 'create_folder_v2'),
            {'path': path.full_path.rstrip('/')},
            throws=core_exceptions.CreateFolderError,
        )
        return DropboxFolderMetadata(data['metadata'], self.folder)

    def can_intra_copy(self, dest_provider: provider.BaseProvider,
                       path: WaterButlerPath=None) -> bool:
        return type(self) == type(dest_provider)

    def can_intra_move(self, dest_provider: provider.BaseProvider,
                       path: WaterButlerPath=None) -> bool:
        return self == dest_provider  # dropbox can only intra move on same account

    def _build_content_url(self, *segments, **query):
        return provider.build_url(pd_settings.BASE_CONTENT_URL, *segments, **query)

    async def _delete_folder_contents(self, path: WaterButlerPath, **kwargs) -> None:
        """Delete the contents of a folder. For use against provider root.

        :param WaterButlerPath path: WaterButlerPath path object for folder
        """
        meta = (await self.metadata(path))
        for child in meta:  # type: ignore
            dropbox_path = await self.validate_path(child.path)
            await self.delete(dropbox_path)
