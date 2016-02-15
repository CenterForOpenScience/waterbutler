import os
import json
import uuid
import shutil
import hashlib

from waterbutler.core import utils
from waterbutler.core import signing
from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath
from waterbutler.core.utils import RequestHandlerContext

from waterbutler.providers.osfstorage import settings
from waterbutler.providers.osfstorage.tasks import backup
from waterbutler.providers.osfstorage.tasks import parity
from waterbutler.providers.osfstorage.metadata import OsfStorageFileMetadata
from waterbutler.providers.osfstorage.metadata import OsfStorageFolderMetadata
from waterbutler.providers.osfstorage.metadata import OsfStorageRevisionMetadata


QUERY_METHODS = ('GET', 'DELETE')


class OSFStorageProvider(provider.BaseProvider):
    __version__ = '0.0.1'

    NAME = 'osfstorage'

    def __init__(self, auth, credentials, settings):
        super().__init__(auth, credentials, settings)
        self.nid = settings['nid']
        self.root_id = settings['rootId']
        self.BASE_URL = settings['baseUrl']
        self.provider_name = settings['storage'].get('provider')

        self.parity_settings = settings.get('parity')
        self.parity_credentials = credentials.get('parity')

        self.archive_settings = settings.get('archive')
        self.archive_credentials = credentials.get('archive')

    async def validate_v1_path(self, path, **kwargs):
        if path == '/':
            return WaterButlerPath('/', _ids=[self.root_id], folder=True)

        implicit_folder = path.endswith('/')
        obj_id = path.strip('/')

        resp = await self.make_signed_request(
            'GET',
            self.build_url(obj_id, 'lineage'),
            expects=(200,)
        )

        data = await resp.json()
        explicit_folder = data['data'][0]['kind'] == 'folder'
        if explicit_folder != implicit_folder:
            raise exceptions.NotFoundError(str(path))

        names, ids = zip(*[(x['name'], x['id']) for x in reversed(data['data'])])

        return WaterButlerPath('/'.join(names), _ids=ids, folder=explicit_folder)

    async def validate_path(self, path, **kwargs):
        if path == '/':
            return WaterButlerPath('/', _ids=[self.root_id], folder=True)

        ends_with_slash = path.endswith('/')

        try:
            path, name = path.strip('/').split('/')
        except ValueError:
            path, name = path, None

        async with self.signed_request(
            'GET',
            self.build_url(path, 'lineage'),
            expects=(200, 404)
        ) as resp:

            if resp.status == 404:
                return WaterButlerPath(path, _ids=(self.root_id, None), folder=path.endswith('/'))

            data = await resp.json()

        is_folder = data['data'][0]['kind'] == 'folder'
        names, ids = zip(*[(x['name'], x['id']) for x in reversed(data['data'])])
        if name is not None:
            ids += (None, )
            names += (name, )
            is_folder = ends_with_slash

        return WaterButlerPath('/'.join(names), _ids=ids, folder=is_folder)

    async def revalidate_path(self, base, path, folder=False):
        assert base.is_dir

        try:
            data = next(
                x for x in
                await self.metadata(base)
                if x.name == path and
                x.kind == ('folder' if folder else 'file')
            )

            return base.child(data.name, _id=data.path.strip('/'), folder=folder)
        except StopIteration:
            return base.child(path, folder=folder)

    def make_provider(self, settings):
        """Requests on different files may need to use different providers,
        instances, e.g. when different files lives in different containers
        within a provider. This helper creates a single-use provider instance
        that optionally overrides the settings.

        :param dict settings: Overridden settings
        """
        if not getattr(self, '_inner_provider', None):
            self._inner_provider = utils.make_provider(
                self.provider_name,
                self.auth,
                self.credentials['storage'],
                self.settings['storage'],
            )
        return self._inner_provider

    def can_duplicate_names(self):
        return True

    def can_intra_copy(self, other, path=None):
        return isinstance(other, self.__class__)

    def can_intra_move(self, other, path=None):
        return isinstance(other, self.__class__)

    async def intra_move(self, dest_provider, src_path, dest_path):
        if dest_path.identifier:
            await dest_provider.delete(dest_path)

        async with self.signed_request(
            'POST',
            self.build_url('hooks', 'move'),
            data=json.dumps({
                'user': self.auth['id'],
                'source': src_path.identifier,
                'destination': {
                    'name': dest_path.name,
                    'node': dest_provider.nid,
                    'parent': dest_path.parent.identifier
                }
            }),
            headers={'Content-Type': 'application/json'},
            expects=(200, 201)
        ) as resp:
            data = await resp.json()

        if data['kind'] == 'file':
            return OsfStorageFileMetadata(data, str(dest_path)), dest_path.identifier is None

        return OsfStorageFolderMetadata(data, str(dest_path)), dest_path.identifier is None

    async def intra_copy(self, dest_provider, src_path, dest_path):
        if dest_path.identifier:
            await dest_provider.delete(dest_path)

        async with self.signed_request(
            'POST',
            self.build_url('hooks', 'copy'),
            data=json.dumps({
                'user': self.auth['id'],
                'source': src_path.identifier,
                'destination': {
                    'name': dest_path.name,
                    'node': dest_provider.nid,
                    'parent': dest_path.parent.identifier
                }
            }),
            headers={'Content-Type': 'application/json'},
            expects=(200, 201)
        ) as resp:
            data = await resp.json()

        if data['kind'] == 'file':
            return OsfStorageFileMetadata(data, str(dest_path)), dest_path.identifier is None

        return OsfStorageFolderMetadata(data, str(dest_path)), dest_path.identifier is None

    def build_signed_url(self, method, url, data=None, params=None, ttl=100, **kwargs):
        signer = signing.Signer(settings.HMAC_SECRET, settings.HMAC_ALGORITHM)
        if method.upper() in QUERY_METHODS:
            signed = signing.sign_data(signer, params or {}, ttl=ttl)
            params = signed
        else:
            signed = signing.sign_data(signer, json.loads(data or {}), ttl=ttl)
            data = json.dumps(signed)

        # Ensure url ends with a /
        if not url.endswith('/'):
            if '?' not in url:
                url += '/'
            elif url[url.rfind('?') - 1] != '/':
                url = url.replace('?', '/?')

        return url, data, params

    async def make_signed_request(self, method, url, data=None, params=None, ttl=100, **kwargs):
        url, data, params = self.build_signed_url(method, url, data=data, params=params, ttl=ttl, **kwargs)
        return await self.make_request(method, url, data=data, params=params, **kwargs)

    def signed_request(self, *args, **kwargs):
        return RequestHandlerContext(self.make_signed_request(*args, **kwargs))

    async def download(self, path, version=None, revision=None, mode=None, **kwargs):
        if not path.identifier:
            raise exceptions.NotFoundError(str(path))

        if version is None:
            # TODO Clean this up
            # version could be 0 here
            version = revision

        # osf storage metadata will return a virtual path within the provider
        async with self.signed_request(
            'GET',
            self.build_url(path.identifier, 'download', version=version, mode=mode),
            expects=(200, ),
            throws=exceptions.DownloadError,
        ) as resp:
            data = await resp.json()

        provider = self.make_provider(data['settings'])
        name = data['data'].pop('name')
        data['data']['path'] = await provider.validate_path('/' + data['data']['path'])
        download_kwargs = {}
        download_kwargs.update(kwargs)
        download_kwargs.update(data['data'])
        download_kwargs['displayName'] = kwargs.get('displayName', name)
        return await provider.download(**download_kwargs)

    async def upload(self, stream, path, **kwargs):
        self._create_paths()

        pending_name = str(uuid.uuid4())
        provider = self.make_provider(self.settings)
        local_pending_path = os.path.join(settings.FILE_PATH_PENDING, pending_name)
        remote_pending_path = await provider.validate_path('/' + pending_name)

        stream.add_writer('md5', streams.HashStreamWriter(hashlib.md5))
        stream.add_writer('sha1', streams.HashStreamWriter(hashlib.sha1))
        stream.add_writer('sha256', streams.HashStreamWriter(hashlib.sha256))

        with open(local_pending_path, 'wb') as file_pointer:
            stream.add_writer('file', file_pointer)
            await provider.upload(stream, remote_pending_path, check_created=False, fetch_metadata=False, **kwargs)

        complete_name = stream.writers['sha256'].hexdigest
        local_complete_path = os.path.join(settings.FILE_PATH_COMPLETE, complete_name)
        remote_complete_path = await provider.validate_path('/' + complete_name)

        try:
            metadata = await provider.metadata(remote_complete_path)
        except exceptions.MetadataError as e:
            if e.code != 404:
                raise
            metadata, _ = await provider.move(provider, remote_pending_path, remote_complete_path)
        else:
            await provider.delete(remote_pending_path)
        finally:
            metadata = metadata.serialized()

        # Due to cross volume movement in unix we leverage shutil.move which properly handles this case.
        # http://bytes.com/topic/python/answers/41652-errno-18-invalid-cross-device-link-using-os-rename#post157964
        shutil.move(local_pending_path, local_complete_path)

        async with self.signed_request(
            'POST',
            self.build_url(path.parent.identifier, 'children'),
            expects=(200, 201),
            data=json.dumps({
                'name': path.name,
                'user': self.auth['id'],
                'settings': self.settings['storage'],
                'metadata': metadata,
                'hashes': {
                    'md5': stream.writers['md5'].hexdigest,
                    'sha1': stream.writers['sha1'].hexdigest,
                    'sha256': stream.writers['sha256'].hexdigest,
                },
                'worker': {
                    'host': os.uname()[1],
                    # TODO: Include additional information
                    'address': None,
                    'version': self.__version__,
                },
            }),
            headers={'Content-Type': 'application/json'},
        ) as response:
            created = response.status == 201
            data = await response.json()

        if settings.RUN_TASKS and data.pop('archive', True):
            parity.main(
                local_complete_path,
                self.parity_credentials,
                self.parity_settings,
            )
            backup.main(
                local_complete_path,
                data['version'],
                self.build_url('hooks', 'metadata') + '/',
                self.archive_credentials,
                self.archive_settings,
            )

        name = path.name

        metadata.update({
            'name': name,
            'md5': data['data']['md5'],
            'path': data['data']['path'],
            'sha256': data['data']['sha256'],
            'version': data['data']['version'],
            'downloads': data['data']['downloads'],
            'checkout': data['data']['checkout'],
        })

        path._parts[-1]._id = metadata['path'].strip('/')
        return OsfStorageFileMetadata(metadata, str(path)), created

    async def delete(self, path, **kwargs):
        if path.identifier is None:
            raise exceptions.NotFoundError(str(path))

        await (await self.make_signed_request(
            'DELETE',
            self.build_url(path.identifier),
            params={'user': self.auth['id']},
            expects=(200, )
        )).release()

    async def metadata(self, path, **kwargs):
        if path.identifier is None:
            raise exceptions.MetadataError('{} not found'.format(str(path)), code=404)

        if not path.is_dir:
            return await self._item_metadata(path)
        return await self._children_metadata(path)

    async def revisions(self, path, view_only=None, **kwargs):
        if path.identifier is None:
            raise exceptions.MetadataError('File not found', code=404)

        async with self.signed_request(
            'GET',
            self.build_url(path.identifier, 'revisions', view_only=view_only),
            expects=(200, )
        ) as resp:
            return [
                OsfStorageRevisionMetadata(item)
                for item in (await resp.json())['revisions']
            ]

    async def create_folder(self, path, **kwargs):
        async with self.signed_request(
            'POST',
            self.build_url(path.parent.identifier, 'children'),
            data=json.dumps({
                'kind': 'folder',
                'name': path.name,
                'user': self.auth['id'],
            }),
            headers={'Content-Type': 'application/json'},
            expects=(201, )
        ) as resp:
            return OsfStorageFolderMetadata((await resp.json())['data'], str(path))

        resp_json = await resp.json()
        # save new folder's id into the WaterButlerPath object. logs will need it later.
        path._parts[-1]._id = resp_json['data']['path'].strip('/')
        return OsfStorageFolderMetadata(resp_json['data'], str(path))

    async def _item_metadata(self, path, revision=None):
        async with self.signed_request(
            'GET',
            self.build_url(path.identifier, revision=revision),
            expects=(200, )
        ) as resp:
            return OsfStorageFileMetadata((await resp.json()), str(path))

    async def _children_metadata(self, path):
        async with self.signed_request(
            'GET',
            self.build_url(path.identifier, 'children'),
            expects=(200, )
        ) as resp:
            resp_json = await resp.json()

        ret = []
        for item in resp_json:
            if item['kind'] == 'folder':
                ret.append(OsfStorageFolderMetadata(item, str(path.child(item['name'], folder=True))))
            else:
                ret.append(OsfStorageFileMetadata(item, str(path.child(item['name']))))
        return ret

    def _create_paths(self):
        try:
            os.mkdir(settings.FILE_PATH_PENDING)
        except FileExistsError:
            pass

        try:
            os.mkdir(settings.FILE_PATH_COMPLETE)
        except FileExistsError:
            pass

        return True
