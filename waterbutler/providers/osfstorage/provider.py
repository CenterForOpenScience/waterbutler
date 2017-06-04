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
    """Provider for the Open Science Framework's cloud storage service.

    ``osfstorage`` is actually a pair of providers.  One is the metadata provider, the other is the
    actual storage provider, where the data is kept.  The OSF is the metadata provider.  Any
    metadata queries about objects in ``osfstorage`` are routed to the OSF to be answered.  For
    https://osf.io, the storage provider is ``cloudfiles``. For local testing the ``filesystem``
    provider is used instead.  Uploads and downloads are routed to and from the storage provider,
    with additional queries to the metadata provider to set and get metadata about the object.
    """

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
        created = True
        if dest_path.identifier:
            created = False
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

        folder_meta = OsfStorageFolderMetadata(data, str(dest_path))
        dest_path = await dest_provider.validate_v1_path(data['path'])
        folder_meta.children = await dest_provider._children_metadata(dest_path)

        return folder_meta, created

    async def intra_copy(self, dest_provider, src_path, dest_path):
        created = True
        if dest_path.identifier:
            created = False
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

        folder_meta = OsfStorageFolderMetadata(data, str(dest_path))
        dest_path = await dest_provider.validate_v1_path(data['path'])
        folder_meta.children = await dest_provider._children_metadata(dest_path)

        return folder_meta, created

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

        self.metrics.add('download', {
            'mode_provided': mode is not None,
            'version_from': 'revision' if version is None else 'version',
            'user_logged_in': self.auth.get('id', None) is not None,
        })
        if version is None:
            # TODO Clean this up
            # version could be 0 here
            version = revision

        # Capture user_id for analytics if user is logged in
        user_param = {}
        if self.auth.get('id', None):
            user_param = {'user': self.auth['id']}

        # osf storage metadata will return a virtual path within the provider
        async with self.signed_request(
            'GET',
            self.build_url(path.identifier, 'download', version=version, mode=mode),
            expects=(200, ),
            params=user_param,
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
        """Upload a new file to osfstorage

        When a file is uploaded to osfstorage, WB does a bit of a dance to make sure it gets there
        reliably.  First we take the stream and add several hash calculators that can determine the
        hash of the file as it streams through.  We then tee the file so that it's written to a
        "pending" directory on both local disk and the remote storage provider.  Once that's
        complete, we determine the file's final location, which will be in another directory (by
        default called 'complete'), and renamed to its sha256 hash.   We then check to see if a
        file already exists at that path on the remote storage provider.  If it does, we can skip
        moving the file (since its already been uploaded) and instead delete the pending file. If
        it does not, we move the file on the remote storage provider from the pending path to its
        final path.

        Once this is done the local copy of the file is moved from the pending directory to the
        complete directory.  The file metadata is sent back to the metadata provider to be recorded.
        Finally, we schedule two futures to archive the locally complete file.  One copies the file
        into Amazon Glacier, the other calculates a parity archive, so that the file can be
        reconstructed if any on-disk corruption happens.  These tasks are scheduled via celery and
        don't need to complete for the request to finish.

        Finally, WB constructs its metadata response and sends that back to the original request
        issuer.

        The local file sitting in complete will be archived by the celery tasks at some point in
        the future.  The archivers do not signal when they have finished their task, so for the time
        being the local complete files are allowed to accumulate and must be deleted by some
        external process.  COS currently uses a cron job to delete files older than X days.  If the
        system is being heavily used, it's possible that the files may be deleted before the
        archivers are able to run.  To get around this we have another script in the osf.io
        repository that can audit our files on the remote storage and initiate any missing archives.

        """
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
            'modified': data['data']['modified'],
            'modified_utc': utils.normalize_datetime(data['data']['modified']),
        })

        path._parts[-1]._id = metadata['path'].strip('/')
        return OsfStorageFileMetadata(metadata, str(path)), created

    async def delete(self, path, confirm_delete=0, **kwargs):
        """Delete file, folder, or provider root contents

        :param OsfStoragePath path: path to delete
        :param int confirm_delete: Must be 1 to confirm root folder delete
        """
        if path.identifier is None:
            raise exceptions.NotFoundError(str(path))

        self.metrics.add('delete.is_root_delete', path.is_root)
        if path.is_root:
            self.metrics.add('delete.root_delete_confirmed', confirm_delete == 1)
            if confirm_delete == 1:
                await self._delete_folder_contents(path)
                return
            else:
                raise exceptions.DeleteError(
                    'confirm_delete=1 is required for deleting root provider folder',
                    code=400,
                )

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

        self.metrics.add('revisions', {'got_view_only': view_only is not None})

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

    async def _delete_folder_contents(self, path, **kwargs):
        """Delete the contents of a folder. For use against provider root.

        :param OsfStoragePath path: OsfStoragePath path object for folder
        """
        meta = (await self.metadata(path))
        for child in meta:
            osf_path = await self.validate_path(child.path)
            await self.delete(osf_path)
