import os
import http
import json

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.box import settings
from waterbutler.providers.box.metadata import BoxRevision
from waterbutler.providers.box.metadata import BoxFileMetadata
from waterbutler.providers.box.metadata import BoxFolderMetadata


class BoxProvider(provider.BaseProvider):
    NAME = 'box'
    BASE_URL = settings.BASE_URL

    def __init__(self, auth, credentials, settings):
        super().__init__(auth, credentials, settings)
        self.token = self.credentials['token']
        self.folder = self.settings['folder']

    async def validate_v1_path(self, path, **kwargs):
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
            raise exceptions.NotFoundError(str(path))

        data = await response.json()

        names, ids = zip(*[
            (x['name'], x['id'])
            for x in
            data['path_collection']['entries'] + [data]
        ])
        names, ids = ('',) + names[ids.index(self.folder) + 1:], ids[ids.index(self.folder):]

        return WaterButlerPath('/'.join(names), _ids=ids, folder=path.endswith('/'))

    async def validate_path(self, path, **kwargs):
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
        if obj_id.isdecimal():
            response = await self.make_request(
                'get',
                self.build_url(files_or_folders, obj_id, fields='id,name,path_collection'),
                expects=(200, 404, 405),
                throws=exceptions.MetadataError,
            )
        else:
            response = None  # Ugly but easiest

        if response is None or response.status in (404, 405):
            if new_name is not None:
                raise exceptions.MetadataError('Could not find {}'.format(path), code=404)

            return await self.revalidate_path(
                WaterButlerPath('/', _ids=[self.folder]),
                obj_id,
                folder=path.endswith('/')
            )
        else:
            data = await response.json()  # .json releases the response
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

    async def revalidate_path(self, base, path, folder=None):
        # TODO Research the search api endpoint
        async with self.request(
            'GET',
            self.build_url('folders', base.identifier, 'items', fields='id,name,type'),
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

    def can_duplicate_names(self):
        return False

    def can_intra_move(self, other, path=None):
        return self == other

    def can_intra_copy(self, other, path=None):
        return self == other

    async def intra_copy(self, dest_provider, src_path, dest_path):
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

        return self._serialize_item(data, dest_path), dest_path.identifier is None

    async def intra_move(self, dest_provider, src_path, dest_path):
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

        return self._serialize_item(data, dest_path), dest_path.identifier is None

    @property
    def default_headers(self):
        return {
            'Authorization': 'Bearer {}'.format(self.token),
        }

    async def make_request(self, *args, **kwargs):
        if isinstance(kwargs.get('data'), dict):
            kwargs['data'] = json.dumps(kwargs['data'])

        return await super().make_request(*args, **kwargs)

    async def download(self, path, revision=None, range=None, **kwargs):
        if path.identifier is None:
            raise exceptions.DownloadError('"{}" not found'.format(str(path)), code=404)

        query = {}
        if revision and revision != path.identifier:
            query['version'] = revision

        resp = await self.make_request(
            'GET',
            self.build_url('files', path.identifier, 'content', **query),
            range=range,
            expects=(200, 206),
            throws=exceptions.DownloadError,
        )

        return streams.ResponseStreamReader(resp)

    async def upload(self, stream, path, conflict='replace', **kwargs):
        if path.identifier and conflict == 'keep':
            path, _ = await self.handle_name_conflict(path, conflict=conflict, kind='folder')
            path._parts[-1]._id = None

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
            self._build_upload_url(*filter(lambda x: x is not None, ('files', path.identifier, 'content'))),
            data=data_stream,
            headers=data_stream.headers,
            expects=(201,),
            throws=exceptions.UploadError,
        ) as resp:
            data = await resp.json()

        created = path.identifier is None
        path._parts[-1]._id = data['entries'][0]['id']
        return BoxFileMetadata(data['entries'][0], path), created

    async def delete(self, path, **kwargs):
        if not path.identifier:  # TODO This should be abstracted
            raise exceptions.NotFoundError(str(path))

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

    async def metadata(self, path, raw=False, folder=False, revision=None, **kwargs):
        if path.identifier is None:
            raise exceptions.NotFoundError(str(path))

        if path.is_file:
            return await self._get_file_meta(path, revision=revision, raw=raw)
        return await self._get_folder_meta(path, raw=raw, folder=folder)

    async def revisions(self, path, **kwargs):
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

            revisions = data['entries'] if response.status == http.client.OK else []

        return [BoxRevision(each) for each in [curr] + revisions]

    async def create_folder(self, path, **kwargs):
        WaterButlerPath.validate_folder(path)

        if path.identifier is not None:
            raise exceptions.FolderNamingConflict(str(path))

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
                raise exceptions.FolderNamingConflict(str(path))
            resp_json = await resp.json()
        # save new folder's id into the WaterButlerPath object. logs will need it later.
        path._parts[-1]._id = resp_json['id']
        return BoxFolderMetadata(resp_json, path)

    def _assert_child(self, paths, target=None):
        if self.folder == 0:
            return True
        if target == self.folder:
            return True
        if not paths:
            raise exceptions.MetadataError('Not found', code=http.client.NOT_FOUND)
        if paths[0]['id'] == self.folder:
            return True
        return self._assert_child(paths[1:])

    async def _assert_child_folder(self, path):
        async with self.request(
            'GET',
            self.build_url('folders', path._id),
            expects=(200, ),
            throws=exceptions.MetadataError,
        ) as response:
            data = await response.json()
        self._assert_child(data['path_collection']['entries'], target=data['id'])

    async def _get_file_meta(self, path, raw=False, revision=None):
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

    async def _get_folder_meta(self, path, raw=False, folder=False):
        if folder:
            url = self.build_url('folders', path.identifier)
        else:
            url = self.build_url('folders', path.identifier, 'items', fields='id,name,size,modified_at,etag')

        async with self.request(
            'GET',
            url,
            expects=(200, ),
            throws=exceptions.MetadataError,
        ) as response:
            data = await response.json()

        if raw:
            return data

        if folder:
            return self._serialize_item(data)

        return [
            self._serialize_item(each, path.child(each['name'], folder=(each['type'] == 'folder')))
            for each in data['entries']
        ]

    def _serialize_item(self, item, path):
        if item['type'] == 'folder':
            serializer = BoxFolderMetadata
        else:
            serializer = BoxFileMetadata
        return serializer(item, path)

    def _build_upload_url(self, *segments, **query):
        return provider.build_url(settings.BASE_UPLOAD_URL, *segments, **query)

    def _build_full_path(self, entries, filename):
        path = []
        for entry in reversed(entries):
            if self.folder == entry['id']:
                break
            path.append(entry['name'])

        path = '/'.join(reversed(path))
        return '/' + os.path.join(path, filename)
