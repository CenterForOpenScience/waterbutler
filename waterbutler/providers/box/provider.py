import json
import typing
import aiohttp
import hashlib
from http import HTTPStatus

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
from waterbutler.core import path as wb_path
from waterbutler.providers.box import settings
from waterbutler.providers.box.metadata import (BaseBoxMetadata,
                                                BoxFileMetadata,
                                                BoxFolderMetadata,
                                                BoxRevision)


class BoxProvider(provider.BaseProvider):
    """Provider for the Box.com cloud storage service.

    API docs: https://box-content.readme.io/reference

    """

    NAME = 'box'
    BASE_URL = settings.BASE_URL

    def __init__(self, auth, credentials, settings):
        """
        Credentials::

            * ``token``: api access token

        Settings::

            * ``folder``: id of the folder to use as root.  Box account root is always 0.

        """
        super().__init__(auth, credentials, settings)
        self.token = self.credentials['token']  # type: str
        self.folder = self.settings['folder']  # type: str

    async def validate_v1_path(self, path: str, **kwargs) -> wb_path.WaterButlerPath:
        if path == '/':
            return wb_path.WaterButlerPath('/', _ids=[self.folder])

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

        return wb_path.WaterButlerPath('/'.join(names), _ids=ids, folder=path.endswith('/'))

    async def validate_path(self, path: str, **kwargs) -> wb_path.WaterButlerPath:
        if path == '/':
            return wb_path.WaterButlerPath('/', _ids=[self.folder])

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
                wb_path.WaterButlerPath('/', _ids=[self.folder]),
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

        ret = wb_path.WaterButlerPath('/'.join(names), _ids=ids, folder=is_folder)

        if new_name is not None:
            return await self.revalidate_path(ret, new_name, folder=is_folder)

        return ret

    async def revalidate_path(self, base: wb_path.WaterButlerPath, path: str,
                              folder: bool=None) -> wb_path.WaterButlerPath:
        # TODO Research the search api endpoint
        async with self.request(
            'GET',
            self.build_url('folders', base.identifier, 'items',
                           fields='id,name,type', limit=1000),
            expects=(200,),
            throws=exceptions.ProviderError
        ) as resp:
            data = await resp.json()
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

    def can_intra_move(self, other: provider.BaseProvider,
                       path: wb_path.WaterButlerPath=None) -> bool:
        return self == other

    def can_intra_copy(self, other: provider.BaseProvider,
                       path: wb_path.WaterButlerPath=None) -> bool:
        return self == other

    async def intra_copy(self,  # type: ignore
                         dest_provider: provider.BaseProvider,
                         src_path: wb_path.WaterButlerPath,
                         dest_path: wb_path.WaterButlerPath) \
            -> typing.Tuple[typing.Union[BoxFileMetadata, BoxFolderMetadata], bool]:
        if dest_path.identifier is not None:
            await dest_provider.delete(dest_path)

        async with self.request(
            'POST',
            self.build_url(
                'files' if src_path.is_file else 'folders',
                src_path.identifier,
                'copy'
            ),
            data=json.dumps({
                'name': dest_path.name,
                'parent': {
                    'id': dest_path.parent.identifier
                }
            }),
            headers={'Content-Type': 'application/json'},
            expects=(200, 201),
            throws=exceptions.IntraCopyError
        ) as resp:
            data = await resp.json()

        return await self._intra_move_copy_metadata(dest_path, data)

    async def intra_move(self,  # type: ignore
                         dest_provider: provider.BaseProvider,
                         src_path: wb_path.WaterButlerPath,
                         dest_path: wb_path.WaterButlerPath) -> typing.Tuple[BaseBoxMetadata, bool]:
        if dest_path.identifier is not None and str(dest_path).lower() != str(src_path).lower():
            await dest_provider.delete(dest_path)

        async with self.request(
            'PUT',
            self.build_url(
                'files' if src_path.is_file else 'folders',
                src_path.identifier,
            ),
            data=json.dumps({
                'name': dest_path.name,
                'parent': {
                    'id': dest_path.parent.identifier
                }
            }),
            headers={'Content-Type': 'application/json'},
            expects=(200, 201),
            throws=exceptions.IntraCopyError
        ) as resp:
            data = await resp.json()

        return await self._intra_move_copy_metadata(dest_path, data)

    @property
    def default_headers(self) -> dict:
        return {
            'Authorization': 'Bearer {}'.format(self.token),
        }

    async def make_request(self, *args, **kwargs) -> aiohttp.client.ClientResponse:
        if isinstance(kwargs.get('data'), dict):
            kwargs['data'] = json.dumps(kwargs['data'])

        return await super().make_request(*args, **kwargs)

    async def download(self,  # type: ignore
                       path: wb_path.WaterButlerPath,
                       revision: str=None,
                       range: typing.Tuple[int, int]=None,
                       **kwargs) -> streams.ResponseStreamReader:
        if path.identifier is None:
            raise exceptions.DownloadError('"{}" not found'.format(str(path)), code=404)

        query = {}
        if revision and revision != path.identifier:
            query['version'] = revision

        resp = await self.make_request(
            'GET',
            self.build_url('files', path.identifier, 'content', **query),
            headers={'Accept-Encoding': ''},
            range=range,
            expects=(200, 206),
            throws=exceptions.DownloadError,
        )

        return streams.ResponseStreamReader(resp)

    async def upload(self,  # type: ignore
                     stream: streams.BaseStream,
                     path: wb_path.WaterButlerPath,
                     conflict: str='replace',
                     **kwargs) -> typing.Tuple[BoxFileMetadata, bool]:
        if path.identifier and conflict == 'keep':
            path, _ = await self.handle_name_conflict(path, conflict=conflict, kind='folder')
            path._parts[-1]._id = None

        stream.add_writer('sha1', streams.HashStreamWriter(hashlib.sha1))

        data_stream = streams.FormDataStream(
            attributes=json.dumps({
                'name': path.name,
                'parent': {
                    'id': path.parent.identifier
                }
            })
        )
        data_stream.add_file('file', stream, path.name, disposition='form-data')

        async with self.request(
            'POST',
            self._build_upload_url(
                *filter(lambda x: x is not None, ('files', path.identifier, 'content'))),
            data=data_stream,
            headers=data_stream.headers,
            expects=(201,),
            throws=exceptions.UploadError,
        ) as resp:
            data = await resp.json()

        entry = data['entries'][0]
        if stream.writers['sha1'].hexdigest != entry['sha1']:
            raise exceptions.UploadChecksumMismatchError()

        created = path.identifier is None
        path._parts[-1]._id = entry['id']
        return BoxFileMetadata(entry, path), created

    async def delete(self,  # type: ignore
                     path: wb_path.WaterButlerPath,
                     confirm_delete: int=0,
                     **kwargs) -> None:
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

        async with self.request(
            'DELETE', url,
            expects=(204, ),
            throws=exceptions.DeleteError,
        ):
            return  # Ensures the response is properly released

    async def metadata(self,  # type: ignore
                       path: wb_path.WaterButlerPath,
                       raw: bool=False, folder=False, revision=None, **kwargs) \
                       -> typing.Union[dict, BoxFileMetadata, typing.List[BoxFolderMetadata]]:
        if path.identifier is None:
            raise exceptions.NotFoundError(str(path))

        if path.is_file:
            return await self._get_file_meta(path, revision=revision, raw=raw)
        return await self._get_folder_meta(path, raw=raw, folder=folder)

    async def revisions(self, path: wb_path.WaterButlerPath, **kwargs) -> typing.List[BoxRevision]:
        # from https://developers.box.com/docs/#files-view-versions-of-a-file :
        # Alert: Versions are only tracked for Box users with premium accounts.
        # Few users will have a premium account, return only current if not
        curr = await self.metadata(path, raw=True)
        async with self.request(
            'GET',
            self.build_url('files', path.identifier, 'versions'),
            expects=(200, 403),
            throws=exceptions.RevisionsError,
        ) as response:
            data = await response.json()

            revisions = data['entries'] if response.status == HTTPStatus.OK else []

        return [BoxRevision(each) for each in [curr] + revisions]

    async def create_folder(self, path: wb_path.WaterButlerPath, folder_precheck: bool=True,
                            **kwargs) -> BoxFolderMetadata:
        wb_path.WaterButlerPath.validate_folder(path)

        if folder_precheck:
            if path.identifier is not None:
                raise exceptions.FolderNamingConflict(path.name)

        async with self.request(
            'POST',
            self.build_url('folders'),
            data={
                'name': path.name,
                'parent': {
                    'id': path.parent.identifier
                }
            },
            expects=(201, 409),
            throws=exceptions.CreateFolderError,
        ) as resp:
            # Catch 409s to avoid race conditions
            if resp.status == 409:
                raise exceptions.FolderNamingConflict(path.name)
            resp_json = await resp.json()
        # save new folder's id into the WaterButlerPath object. logs will need it later.
        path._parts[-1]._id = resp_json['id']
        return BoxFolderMetadata(resp_json, path)

    async def _get_file_meta(self,
                             path: wb_path.WaterButlerPath,
                             raw: bool=False,
                             revision: str=None) -> typing.Union[dict, BoxFileMetadata]:
        if revision:
            url = self.build_url('files', path.identifier, 'versions')
        else:
            url = self.build_url('files', path.identifier)

        async with self.request(
            'GET', url,
            expects=(200, ),
            throws=exceptions.MetadataError,
        ) as resp:
            data = await resp.json()

        if revision:
            try:
                data = next(x for x in data['entries'] if x['id'] == revision)
            except StopIteration:
                raise exceptions.NotFoundError(str(path))

        if not data:
            raise exceptions.NotFoundError(str(path))

        return data if raw else BoxFileMetadata(data, path)

    async def _get_folder_meta(self,
                               path: wb_path.WaterButlerPath,
                               raw: bool=False,
                               folder: bool=False) \
                               -> typing.Union[dict, typing.List[BoxFolderMetadata]]:
        if folder:
            async with self.request(
                'GET', self.build_url('folders', path.identifier),
                expects=(200, ), throws=exceptions.MetadataError,
            ) as resp:
                data = await resp.json()
                # FIXME: Usage does not match function call signature!  Dead code or bug?
                return data if raw else self._serialize_item(data)

        # Box maximum limit is 1000
        page_count, page_total, limit = 0, None, 1000
        full_resp = {} if raw else []  # type: ignore
        while page_total is None or page_count < page_total:
            url = self.build_url('folders', path.identifier, 'items',
                                 fields='id,name,size,modified_at,etag,total_count',
                                 offset=(page_count * limit),
                                 limit=limit)
            async with self.request('GET', url, expects=(200, ),
                                    throws=exceptions.MetadataError) as response:
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
                        path: wb_path.WaterButlerPath) \
                        -> typing.Union[BoxFileMetadata, BoxFolderMetadata]:
        if item['type'] == 'folder':
            serializer = BoxFolderMetadata  # type: ignore
        else:
            serializer = BoxFileMetadata  # type: ignore
        return serializer(item, path)

    def _build_upload_url(self, *segments, **query):
        return provider.build_url(settings.BASE_UPLOAD_URL, *segments, **query)

    async def _delete_folder_contents(self, path: wb_path.WaterButlerPath, **kwargs) -> None:
        """Delete the contents of a folder. For use against provider root.

        :param BoxPath path: BoxPath path object for folder
        """
        meta = await self.metadata(path)
        for child in meta:  # type: ignore
            box_path = await self.validate_path(child.path)
            await self.delete(box_path)

    async def _intra_move_copy_metadata(self, path, data: dict) -> BaseBoxMetadata:
        """Return appropriate metadata from intra_copy/intra_move actions. If `data` respresents
        a folder, will fetch and include `data`'s children.
        """
        created = path.identifier is None
        path.parts[-1]._id = data['id']
        if data['type'] == 'file':
            return self._serialize_item(data, path), created
        else:
            folder = self._serialize_item(data, path)
            folder._children = await self._get_folder_meta(path)
            return folder, created
