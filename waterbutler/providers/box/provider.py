import json
import base64
import hashlib
import logging
import tempfile
from asyncio import sleep
from http import HTTPStatus
from typing import List, Tuple, Union

import aiohttp

from waterbutler.core.path import WaterButlerPath
from waterbutler.core import exceptions, streams, provider
from waterbutler.core.exceptions import RetryChunkedUploadCommit

from waterbutler.providers.box import settings as pd_settings
from waterbutler.providers.box.metadata import (BaseBoxMetadata, BoxRevision,
                                                BoxFileMetadata, BoxFolderMetadata, )

logger = logging.getLogger(__name__)


class BoxProvider(provider.BaseProvider):
    """Provider for the Box.com cloud storage service.

    API docs: https://box-content.readme.io/reference

    """

    NAME = 'box'
    BASE_URL = pd_settings.BASE_URL
    NONCHUNKED_UPLOAD_LIMIT = pd_settings.NONCHUNKED_UPLOAD_LIMIT  # 50MB default
    TEMP_CHUNK_SIZE = pd_settings.TEMP_CHUNK_SIZE  # 32KiB default
    UPLOAD_COMMIT_RETRIES = pd_settings.UPLOAD_COMMIT_RETRIES

    def __init__(self, auth, credentials, settings, **kwargs):
        """Initialize a `BoxProvider` instance
        Credentials::

            * ``token``: api access token

        Settings::

            * ``folder``: id of the folder to use as root.  Box account root is always 0.

        """
        super().__init__(auth, credentials, settings, **kwargs)
        self.token = self.credentials['token']  # type: str
        self.folder = self.settings['folder']  # type: str

    async def validate_v1_path(self, path: str, **kwargs) -> WaterButlerPath:
        if path == '/':
            return WaterButlerPath('/', _ids=[self.folder])

        obj_id = path.strip('/')
        files_or_folders = 'folders' if path.endswith('/') else 'files'

        # Box file ids must be a valid base10 number
        if not obj_id.isdecimal():
            raise exceptions.NotFoundError(str(path))

        response = await self.make_request(
            'get',
            self.build_url(files_or_folders, obj_id, fields='id,name,path_collection'),
            expects=(200, 404,),
            throws=exceptions.MetadataError,
        )

        if response.status == 404:
            await response.release()
            raise exceptions.NotFoundError(str(path))

        data = await response.json()

        if self.folder != '0':  # don't allow files outside project root
            path_ids = [entry['id'] for entry in data['path_collection']['entries']]
            if self.folder not in path_ids:
                raise exceptions.NotFoundError(path)

        names, ids = zip(*[
            (x['name'], x['id'])
            for x in
            data['path_collection']['entries'] + [data]
        ])
        names, ids = ('',) + names[ids.index(self.folder) + 1:], ids[ids.index(self.folder):]

        return WaterButlerPath('/'.join(names), _ids=ids, folder=path.endswith('/'))

    async def validate_path(self, path: str, **kwargs) -> WaterButlerPath:
        if path == '/':
            return WaterButlerPath('/', _ids=[self.folder])

        try:
            obj_id, new_name = path.strip('/').split('/')
        except ValueError:
            obj_id, new_name = path.strip('/'), None

        if path.endswith('/') or new_name is not None:
            files_or_folders = 'folders'
        else:
            files_or_folders = 'files'

        # Box file ids must be a valid base10 number
        response = None
        if obj_id.isdecimal():
            response = await self.make_request(
                'get',
                self.build_url(files_or_folders, obj_id, fields='id,name,path_collection'),
                expects=(200, 404, 405),
                throws=exceptions.MetadataError,
            )
            if response.status in (404, 405):
                await response.release()
                response = None

        if response is None:
            if new_name is not None:
                raise exceptions.MetadataError('Could not find {}'.format(path), code=404)

            return await self.revalidate_path(
                WaterButlerPath('/', _ids=[self.folder]),
                obj_id,
                folder=path.endswith('/')
            )
        else:
            data = await response.json()  # .json releases the response

            if self.folder != '0':  # don't allow files outside project root
                path_ids = [entry['id'] for entry in data['path_collection']['entries']]
                if self.folder not in path_ids:
                    raise exceptions.NotFoundError(path)

            names, ids = zip(*[
                (x['name'], x['id'])
                for x in
                data['path_collection']['entries'] + [data]
            ])

            try:
                names, ids = ('',) + names[ids.index(self.folder) + 1:], ids[ids.index(self.folder):]
            except ValueError:
                raise Exception  # TODO

        is_folder = path.endswith('/')

        ret = WaterButlerPath('/'.join(names), _ids=ids, folder=is_folder)

        if new_name is not None:
            return await self.revalidate_path(ret, new_name, folder=is_folder)

        return ret

    async def revalidate_path(self, base: WaterButlerPath, path: str,
                              folder: bool=None) -> WaterButlerPath:
        # TODO Research the search api endpoint
        response = await self.make_request(
            'GET',
            self.build_url('folders', base.identifier, 'items',
                           fields='id,name,type', limit=1000),
            expects=(200,),
            throws=exceptions.ProviderError,
        )
        data = await response.json()
        lower_name = path.lower()

        try:
            item = next(
                x for x in data['entries']
                if x['name'].lower() == lower_name and (
                    folder is None or
                    (x['type'] == 'folder') == folder
                )
            )
            name = path  # Use path over x['name'] because of casing issues
            _id = item['id']
            folder = item['type'] == 'folder'
        except StopIteration:
            _id = None
            name = path

        return base.child(name, _id=_id, folder=folder)

    def can_duplicate_names(self)-> bool:
        return False

    def shares_storage_root(self, other: provider.BaseProvider) -> bool:
        """Box settings include the root folder id, which is unique across projects for subfolders.
        But the root folder of a Box account always has an ID of 0.  This means that the root
        folders of two separate Box accounts would incorrectly test as being the same storage root.
        Add a comparison of credentials to avoid this."""
        return super().shares_storage_root(other) and self.credentials == other.credentials

    def can_intra_move(self, other: provider.BaseProvider, path: WaterButlerPath=None) -> bool:
        return self == other

    def can_intra_copy(self, other: provider.BaseProvider, path: WaterButlerPath=None) -> bool:
        return self == other

    async def intra_copy(self,  # type: ignore
                         dest_provider: provider.BaseProvider, src_path: WaterButlerPath,
                         dest_path: WaterButlerPath) -> Tuple[BaseBoxMetadata, bool]:
        if dest_path.identifier is not None:
            await dest_provider.delete(dest_path)

        response = await self.make_request(
            'POST',
            self.build_url(
                'files' if src_path.is_file else 'folders',
                src_path.identifier,
                'copy'
            ),
            data={
                'name': dest_path.name,
                'parent': {
                    'id': dest_path.parent.identifier
                }
            },
            headers={'Content-Type': 'application/json'},
            expects=(200, 201),
            throws=exceptions.IntraCopyError,
        )
        data = await response.json()

        return await self._intra_move_copy_metadata(dest_path, data)

    async def intra_move(self,  # type: ignore
                         dest_provider: provider.BaseProvider, src_path: WaterButlerPath,
                         dest_path: WaterButlerPath) -> Tuple[BaseBoxMetadata, bool]:
        if dest_path.identifier is not None and str(dest_path).lower() != str(src_path).lower():
            await dest_provider.delete(dest_path)

        response = await self.make_request(
            'PUT',
            self.build_url(
                'files' if src_path.is_file else 'folders',
                src_path.identifier,
            ),
            data={
                'name': dest_path.name,
                'parent': {
                    'id': dest_path.parent.identifier
                }
            },
            headers={'Content-Type': 'application/json'},
            expects=(200, 201),
            throws=exceptions.IntraCopyError,
        )
        data = await response.json()

        return await self._intra_move_copy_metadata(dest_path, data)

    @property
    def default_headers(self) -> dict:
        return {
            'Authorization': 'Bearer {}'.format(self.token),
        }

    async def make_request(self, method: str, url: str, *args, **kwargs) -> aiohttp.ClientResponse:
        if isinstance(kwargs.get('data'), dict):
            kwargs['data'] = json.dumps(kwargs['data'])
        return await super().make_request(method, url, *args, **kwargs)

    async def download(self,  # type: ignore
                       path: WaterButlerPath, revision: str=None, range: Tuple[int, int]=None,
                       **kwargs) -> streams.ResponseStreamReader:
        if path.identifier is None:
            raise exceptions.DownloadError('"{}" not found'.format(str(path)), code=404)

        query = {}
        if revision and revision != path.identifier:
            query['version'] = revision

        logger.debug('request-range:: {}'.format(range))
        resp = await self.make_request(
            'GET',
            self.build_url('files', path.identifier, 'content', **query),
            headers={'Accept-Encoding': ''},
            range=range,
            expects=(200, 206),
            throws=exceptions.DownloadError,
        )
        logger.debug('download-headers:: {}'.format([(x, resp.headers[x]) for x in resp.headers]))

        return streams.ResponseStreamReader(resp)

    async def upload(self,  # type: ignore
                     stream: streams.BaseStream, path: WaterButlerPath, conflict: str='replace',
                     **kwargs) -> Tuple[BoxFileMetadata, bool]:
        """Upload a file to Box.  If the file is less than ``NONCHUNKED_UPLOAD_LIMIT``, upload in
        a single request.  Otherwise, use Box's chunked upload interface to send it across multiple
        requests.
        """

        if path.identifier and conflict == 'keep':
            path, _ = await self.handle_name_conflict(path, conflict=conflict, kind='folder')
            path._parts[-1]._id = None

        if stream.size > self.NONCHUNKED_UPLOAD_LIMIT:
            entry = await self._chunked_upload(path, stream)
        else:
            entry = await self._contiguous_upload(path, stream)

        created = path.identifier is None
        path._parts[-1]._id = entry['id']

        return BoxFileMetadata(entry, path), created

    async def delete(self,  # type: ignore
                     path: WaterButlerPath, confirm_delete: int=0, **kwargs) -> None:
        """Delete file, folder, or provider root contents

        :param BoxPath path: BoxPath path object for folder
        :param int confirm_delete: Must be 1 to confirm root folder delete
        """
        if not path.identifier:  # TODO This should be abstracted
            raise exceptions.NotFoundError(str(path))

        if path.is_root:
            if confirm_delete == 1:
                await self._delete_folder_contents(path)
                return
            else:
                raise exceptions.DeleteError(
                    'confirm_delete=1 is required for deleting root provider folder',
                    code=400
                )

        if path.is_file:
            url = self.build_url('files', path.identifier)
        else:
            url = self.build_url('folders', path.identifier, recursive=True)

        response = await self.make_request(
            'DELETE',
            url,
            expects=(204, ),
            throws=exceptions.DeleteError,
        )
        await response.release()

        return  # Ensures the response is properly released

    async def metadata(self,  # type: ignore
                       path: WaterButlerPath, raw: bool=False, folder=False, revision=None,
                       **kwargs) -> Union[dict, BoxFileMetadata, List[BoxFolderMetadata]]:
        if path.identifier is None:
            raise exceptions.NotFoundError(str(path))

        if path.is_file:
            return await self._get_file_meta(path, revision=revision, raw=raw)
        return await self._get_folder_meta(path, raw=raw, folder=folder)

    async def revisions(self, path: WaterButlerPath, **kwargs) -> List[BoxRevision]:
        # from https://developers.box.com/docs/#files-view-versions-of-a-file :
        # Alert: Versions are only tracked for Box users with premium accounts.
        # Few users will have a premium account, return only current if not
        curr = await self.metadata(path, raw=True)
        response = await self.make_request(
            'GET',
            self.build_url('files', path.identifier, 'versions'),
            expects=(200, 403),
            throws=exceptions.RevisionsError,
        )
        data = await response.json()

        revisions = data['entries'] if response.status == HTTPStatus.OK else []

        return [BoxRevision(each) for each in [curr] + revisions]

    async def create_folder(self, path: WaterButlerPath, folder_precheck: bool=True,
                            **kwargs) -> BoxFolderMetadata:
        WaterButlerPath.validate_folder(path)

        if folder_precheck:
            if path.identifier is not None:
                raise exceptions.FolderNamingConflict(path.name)

        response = await self.make_request(
            'POST',
            self.build_url('folders'),
            data={
                'name': path.name,
                'parent': {'id': path.parent.identifier}
            },
            expects=(201, 409),
            throws=exceptions.CreateFolderError,
        )
        # Catch 409s to avoid race conditions
        if response.status == 409:
            raise exceptions.FolderNamingConflict(path.name)
        resp_json = await response.json()
        # save new folder's id into the WaterButlerPath object. logs will need it later.
        path._parts[-1]._id = resp_json['id']
        return BoxFolderMetadata(resp_json, path)

    async def _get_file_meta(self, path: WaterButlerPath, raw: bool=False,
                             revision: str=None) -> Union[dict, BoxFileMetadata]:
        if revision:
            url = self.build_url('files', path.identifier, 'versions')
        else:
            url = self.build_url('files', path.identifier)

        response = await self.make_request(
            'GET',
            url,
            expects=(200, ),
            throws=exceptions.MetadataError,
        )
        data = await response.json()

        if revision:
            try:
                data = next(x for x in data['entries'] if x['id'] == revision)
            except StopIteration:
                raise exceptions.NotFoundError(str(path))

        if not data:
            raise exceptions.NotFoundError(str(path))

        return data if raw else BoxFileMetadata(data, path)

    async def _get_folder_meta(self, path: WaterButlerPath, raw: bool=False,
                               folder: bool=False) -> Union[dict, List[BoxFolderMetadata]]:
        if folder:
            response = await self.make_request(
                'GET',
                self.build_url('folders', path.identifier),
                expects=(200, ),
                throws=exceptions.MetadataError,
            )
            data = await response.json()
            return data if raw else self._serialize_item(data, path)

        # Box maximum limit is 1000
        page_count, page_total, limit = 0, None, 1000
        full_resp = {} if raw else []  # type: ignore
        while page_total is None or page_count < page_total:
            url = self.build_url('folders', path.identifier, 'items',
                                 fields='id,name,size,modified_at,etag,total_count',
                                 offset=(page_count * limit),
                                 limit=limit)
            response = await self.make_request(
                'GET',
                url,
                expects=(200, ),
                throws=exceptions.MetadataError,
            )
            resp_json = await response.json()
            if raw:
                full_resp.update(resp_json)  # type: ignore
            else:
                full_resp.extend([  # type: ignore
                    self._serialize_item(
                        each, path.child(each['name'], folder=(each['type'] == 'folder'))
                    )
                    for each in resp_json['entries']
                ])

            page_count += 1
            if page_total is None:
                page_total = ((resp_json['total_count'] - 1) // limit) + 1  # ceiling div
        self.metrics.add('metadata.folder.pages', page_total)
        return full_resp

    def _serialize_item(self, item: dict,
                        path: WaterButlerPath) -> Union[BoxFileMetadata, BoxFolderMetadata]:
        if item['type'] == 'folder':
            serializer = BoxFolderMetadata  # type: ignore
        else:
            serializer = BoxFileMetadata  # type: ignore
        return serializer(item, path)

    def _build_upload_url(self, *segments, **query):
        return provider.build_url(pd_settings.BASE_UPLOAD_URL, *segments, **query)

    async def _delete_folder_contents(self, path: WaterButlerPath, **kwargs) -> None:
        """Delete the contents of a folder. For use against provider root.

        :param BoxPath path: BoxPath path object for folder
        """
        meta = await self.metadata(path)
        for child in meta:  # type: ignore
            box_path = await self.validate_path(child.path)
            await self.delete(box_path)

    async def _intra_move_copy_metadata(self, path, data: dict) -> Tuple[BaseBoxMetadata, bool]:
        """Return appropriate metadata from intra_copy/intra_move actions. If `data` represents
        a folder, will fetch and include `data`'s children.
        """
        created = path.identifier is None
        path.parts[-1]._id = data['id']
        if data['type'] == 'file':
            return self._serialize_item(data, path), created
        else:
            folder = self._serialize_item(data, path)
            folder._children = await self._get_folder_meta(path)  # type: ignore
            return folder, created

    async def _contiguous_upload(self, path: WaterButlerPath, stream: streams.BaseStream) -> dict:
        """Upload a file to Box using a single request. This will only be called if the file is
        smaller than the ``NONCHUNKED_UPLOAD_LIMIT``.

        API Docs: https://developer.box.com/reference#upload-a-file
        """
        assert stream.size <= self.NONCHUNKED_UPLOAD_LIMIT
        stream.add_writer('sha1', streams.HashStreamWriter(hashlib.sha1))

        data_stream = streams.FormDataStream(
            attributes=json.dumps({
                'name': path.name,
                'parent': {'id': path.parent.identifier}
            })
        )
        data_stream.add_file('file', stream, path.name, disposition='form-data')

        if path.identifier is not None:
            segments = ['files', path.identifier, 'content']
        else:
            segments = ['files', 'content']

        response = await self.make_request(
            'POST',
            self._build_upload_url(*segments),
            data=data_stream,
            headers=data_stream.headers,
            expects=(201, ),
            throws=exceptions.UploadError,
        )
        data = await response.json()

        entry = data['entries'][0]
        if stream.writers['sha1'].hexdigest != entry['sha1']:
            raise exceptions.UploadChecksumMismatchError()

        return entry

    async def _chunked_upload(self, path: WaterButlerPath, stream: streams.BaseStream) -> dict:
        """Upload a large file to Box over multiple requests. This method will be used if the
        file to upload is larger than ``NONCHUNKED_UPLOAD_LIMIT``.  Checksum verification is built
        into this process, so manual verification is not needed.

        API Docs: https://developer.box.com/reference#chunked-upload
        """

        # Step 1: Add a sha1 calculator. The final sha1 will be needed to complete the session
        stream.add_writer('sha1', streams.HashStreamWriter(hashlib.sha1))

        # Step 2: Create an upload session with Box and recieve session id.
        session_data = await self._create_chunked_upload_session(path, stream)
        logger.debug('chunked upload session data: {}'.format(json.dumps(session_data)))

        metadata = None
        try:
            # Step 3. Split the data into parts and upload them to box.
            parts_manifest = await self._upload_parts(stream, session_data)
            logger.debug('chunked upload parts manifest: {}'.format(json.dumps(parts_manifest)))
            data_sha = base64.standard_b64encode(stream.writers['sha1'].digest).decode()
            # Step 4. Complete the session and return the uploaded file's metadata.
            retry = self.UPLOAD_COMMIT_RETRIES
            while retry > 0:
                retry -= 1
                try:
                    metadata = await self._complete_chunked_upload_session(session_data,
                                                                           parts_manifest, data_sha)
                    break
                except RetryChunkedUploadCommit:
                    continue
        except Exception as err:
            msg = 'An unexpected error has occurred during the multi-part upload.'
            logger.error('{} upload_id={} error={!r}'.format(msg, session_data, err))
            aborted = await self._abort_chunked_upload(session_data, data_sha)
            if not aborted:
                msg += '  The abort action failed to clean up the temporary file parts generated ' \
                    'during the upload process.  Please manually remove them.'
            raise exceptions.UploadError(msg)
        return metadata

    async def _create_chunked_upload_session(self, path: WaterButlerPath,
                                             stream: streams.BaseStream) -> dict:
        """Create an upload session to use with a chunked upload.

        The upload session metadata contains a session identifier, the partitioning scheme, and
        urls to the chunked upload endpoints. When the upload has completed the session will need
        to be closed.

        API Docs: https://developer.box.com/reference#create-session-new-file
        """

        # During chunked upload session creation, WB should EITHER provide the ``path.identifier``
        # (file ID) in the URL if the file already exists OR provide the ``path.parent.identifier``
        # (parent folder ID) in the data payload.  In addition, providing both will get a 400 with
        # error "multiple_destinations".  Moreover, providing the parent folder ID when the file
        # exists will get a 409 with error "item_name_in_use".  Finally, chunked upload never asks
        # user to confirm file conflicts and just overwrites.
        data = {}
        if path.identifier is not None:
            segments = ['files', path.identifier, 'upload_sessions']
        else:
            segments = ['files', 'upload_sessions']
            data['folder_id'] = path.parent.identifier
        data.update({
            'file_size': stream.size,
            'file_name': path.name,
        })

        response = await self.make_request(
            'POST',
            self._build_upload_url(*segments),
            data=json.dumps(data, sort_keys=True),
            headers={'Content-Type': 'application/json'},
            expects=(201, ),
            throws=exceptions.UploadError,
        )
        return await response.json()

    async def _upload_parts(self, stream: streams.BaseStream, session_data: dict) -> list:
        """Calculate the partitioning scheme and upload the parts of the stream.  Returns a list
        of metadata objects for each part, as reported by Box.  This list will be used to finialize
        the upload.
        """

        part_max_size = session_data['part_size']
        parts = [part_max_size for _ in range(0, stream.size // part_max_size)]
        if stream.size % part_max_size:
            parts.append(stream.size - (len(parts) * part_max_size))
        logger.debug('Stream will be partitioned into {} with the following '
                     'sizes: {}'.format(len(parts), parts))

        start_offset, manifest = 0, []
        for part_id, part_size in enumerate(parts):
            logging.debug('Uploading part {}, with size {} bytes, starting '
                          'at offset {}'.format(part_id, part_size, start_offset))
            part_metadata = await self._upload_part(stream, str(part_id), part_size, start_offset,
                                                    session_data['id'])
            manifest.append(part_metadata)
            start_offset += part_size

        return manifest

    async def _upload_part(self, stream: streams.BaseStream, part_id: str, part_size: int,
                           start_offset: int, session_id: str) -> dict:
        """Upload one part/chunk of the given stream to Box.

        Box requires that the sha of the part be sent along in the headers of the request.  To do
        this WB must write the stream segment to disk before uploading.  The part sha is calculated
        as the tempfile is written.

        API Docs: https://developer.box.com/reference#upload-part
        """

        cutoff_stream = streams.CutoffStream(stream, cutoff=part_size)
        part_hasher_name = 'part-{}-sha1'.format(part_id)
        stream.add_writer(part_hasher_name, streams.HashStreamWriter(hashlib.sha1))

        f = tempfile.TemporaryFile()
        chunk = await cutoff_stream.read(self.TEMP_CHUNK_SIZE)
        while chunk:
            f.write(chunk)
            chunk = await cutoff_stream.read(self.TEMP_CHUNK_SIZE)
        file_stream = streams.FileStreamReader(f)

        part_sha = stream.writers[part_hasher_name].digest
        part_sha_b64 = base64.standard_b64encode(part_sha).decode()
        stream.remove_writer(part_hasher_name)

        byte_range = self._build_range_header((start_offset, start_offset + part_size - 1))
        content_range = str(byte_range).replace('=', ' ') + '/{}'.format(stream.size)

        response = await self.make_request(
            'PUT',
            self._build_upload_url('files', 'upload_sessions', session_id),
            headers={
                # ``Content-Length`` is required for ``asyncio`` to use inner chunked stream read
                'Content-Length': str(part_size),
                'Content-Range': content_range,
                'Content-Type:': 'application/octet-stream',
                'Digest': 'sha={}'.format(part_sha_b64)
            },
            data=file_stream,
            expects=(201, 200),
            throws=exceptions.UploadError,
        )
        data = await response.json()

        f.close()
        return data['part']

    async def _complete_chunked_upload_session(self, session_data: dict, parts_manifest: list,
                                               data_sha: str) -> dict:
        """Completes the chunked upload session.  Lets Box know that the parts have all been
        uploaded, and that it can reconstruct the file from its individual parts.

        https://developer.box.com/reference#commit-upload
        """
        response = await self.make_request(
            'POST',
            self._build_upload_url('files', 'upload_sessions', session_data['id'], 'commit'),
            data={'parts': parts_manifest},
            headers={
                'Content-Type:': 'application/json',
                'Digest': 'sha={}'.format(data_sha)
            },
            expects=(201, 202),
            throws=exceptions.UploadError,
        )
        if response.status == HTTPStatus.ACCEPTED:
            await response.release()
            await sleep(response.headers['Retry-After'])
            raise RetryChunkedUploadCommit('Failed to commit chunked upload')
        data = await response.json()
        entry = data['entries'][0]
        return entry

    async def _abort_chunked_upload(self, session_data: dict, data_sha: str) -> bool:
        """Aborts a chunked upload session. This discards all data uploaded during the session.
        This operation cannot be undone.

        API Docs: https://developer.box.com/reference#abort

        :rtype: bool
        :return: `True` if abort request succeeded, `False` otherwise.
        """
        response = await self.make_request(
            'DELETE',
            self._build_upload_url('files', 'upload_sessions', session_data['id']),
            headers={
                'Content-Type:': 'application/json',
                'Digest': 'sha={}'.format(data_sha)
            },
        )
        await response.release()
        return response.status == HTTPStatus.NO_CONTENT
