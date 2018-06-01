import io
from http import client

import pytest
import aiohttpretty

from waterbutler.core import streams
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath
from waterbutler.providers.owncloud import OwnCloudProvider
from waterbutler.providers.owncloud.metadata import (OwnCloudFileMetadata,
                                                     OwnCloudFileRevisionMetadata)

from tests import utils
from tests.providers.owncloud.fixtures import (
    provider,
    provider_different_credentials,
    auth,
    settings,
    credentials,
    credentials_2,
    credentials_host_with_trailing_slash,
    file_content,
    file_metadata,
    folder_contents_metadata,
    file_metadata_object,
    folder_list,
    folder_metadata,
    file_metadata_unparsable_response,
    moved_folder_metadata,
    moved_parent_folder_metadata
)


@pytest.fixture
def file_like(file_content):
    return io.BytesIO(file_content)

@pytest.fixture
def file_stream(file_like):
    return streams.FileStreamReader(file_like)


class TestProviderConstruction:

    def test_base_folder_no_slash(self, auth, credentials):
        provider = OwnCloudProvider(auth, credentials, {'folder': '/foo', 'verify_ssl': False})
        assert provider.folder == '/foo/'

    def test_base_folder_with_slash(self, auth, credentials):
        provider = OwnCloudProvider(auth, credentials, {'folder': '/foo/', 'verify_ssl': False})
        assert provider.folder == '/foo/'

    def test_webdav_url_trailing_slash(self, auth, credentials_host_with_trailing_slash, provider):

        provider_host_with_trailing_slash = OwnCloudProvider(auth,
                                                             credentials_host_with_trailing_slash,
                                                             {'folder': '/foo/',
                                                              'verify_ssl': False})

        expected = 'https://cat/owncloud/remote.php/webdav/'
        assert expected == provider._webdav_url_
        assert expected == provider_host_with_trailing_slash._webdav_url_


class TestValidatePath:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_root(self, provider):
        assert WaterButlerPath('/', prepend=provider.folder) == await provider.validate_v1_path('/')

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_file(self, provider, file_metadata):
        path = WaterButlerPath('/triangles.txt', prepend=provider.folder)
        url = provider._webdav_url_ + path.full_path
        aiohttpretty.register_uri('PROPFIND', url, body=file_metadata, auto_length=True, status=207)
        try:
            wb_path_v1 = await provider.validate_v1_path('/triangles.txt')
        except Exception as exc:
            pytest.fail(str(exc))

        wb_path_v0 = await provider.validate_path('/triangles.txt')

        assert wb_path_v1 == wb_path_v0

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_folder(self, provider, folder_metadata):
        path = WaterButlerPath('/myfolder/', prepend=provider.folder)
        url = provider._webdav_url_ + path.full_path
        aiohttpretty.register_uri('PROPFIND',
                                  url,
                                  body=folder_metadata,
                                  auto_length=True,
                                  status=207)
        try:
            wb_path_v1 = await provider.validate_v1_path('/myfolder/')
        except Exception as exc:
            pytest.fail(str(exc))

        wb_path_v0 = await provider.validate_path('/myfolder/')

        assert wb_path_v1 == wb_path_v0

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_unparsable_dav_response(self, provider, file_metadata_unparsable_response):
        path = WaterButlerPath('/triangles.txt', prepend=provider.folder)
        url = provider._webdav_url_ + path.full_path
        aiohttpretty.register_uri('PROPFIND',
                                  url,
                                  body=file_metadata_unparsable_response,
                                  auto_length=True,
                                  status=207)

        with pytest.raises(exceptions.NotFoundError):
            await provider.validate_v1_path('/triangles.txt')

        try:
            await provider.validate_path('/triangles.txt')
        except Exception as exc:
            pytest.fail(str(exc))

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_v1_own_cloud_404(self, provider, file_metadata_unparsable_response):
        path = WaterButlerPath('/triangles.txt', prepend=provider.folder)
        url = provider._webdav_url_ + path.full_path
        aiohttpretty.register_uri('PROPFIND',
                                  url,
                                  body=file_metadata_unparsable_response,
                                  auto_length=True,
                                  status=404)

        with pytest.raises(exceptions.NotFoundError):
            await provider.validate_v1_path('/triangles.txt')

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_response_different_of_kind_than_path(self, provider, folder_metadata):
        path = WaterButlerPath('/triangles.txt', prepend=provider.folder)
        url = provider._webdav_url_ + path.full_path
        aiohttpretty.register_uri('PROPFIND',
                                  url,
                                  body=folder_metadata,
                                  auto_length=True,
                                  status=207)

        with pytest.raises(exceptions.NotFoundError):
            await provider.validate_v1_path('/triangles.txt')


class TestCRUD:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download(self, provider, file_metadata):
        path = WaterButlerPath('/triangles.txt', prepend=provider.folder)
        url = provider._webdav_url_ + path.full_path
        aiohttpretty.register_uri('PROPFIND', url, body=file_metadata, auto_length=True, status=207)
        aiohttpretty.register_uri('GET', url, body=b'squares', auto_length=True, status=200)
        result = await provider.download(path)
        content = await result.response.read()
        assert content == b'squares'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_range(self, provider, file_metadata):
        path = WaterButlerPath('/triangles.txt', prepend=provider.folder)
        url = provider._webdav_url_ + path.full_path
        aiohttpretty.register_uri('PROPFIND', url, body=file_metadata, auto_length=True, status=207)
        aiohttpretty.register_uri('GET', url, body=b'sq', auto_length=True, status=206)
        result = await provider.download(path, range=(0, 1))
        assert result.partial
        content = await result.response.read()
        assert content == b'sq'
        assert aiohttpretty.has_call(method='GET', uri=url, headers={'Range': 'bytes=0-1'})

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload(self, provider, file_stream, file_metadata, file_metadata_object):
        path = WaterButlerPath('/phile', prepend=provider.folder)
        url = provider._webdav_url_ + path.full_path
        aiohttpretty.register_uri('PROPFIND', url, body=file_metadata, auto_length=True, status=207)
        aiohttpretty.register_uri('PUT', url, body=b'squares', auto_length=True, status=201)
        metadata, created = await provider.upload(file_stream, path)

        assert created is True
        assert metadata.name == file_metadata_object.name
        assert metadata.size == file_metadata_object.size
        assert metadata.size_as_int == int(file_metadata_object.size)
        assert aiohttpretty.has_call(method='PUT', uri=url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_keep(self, provider, file_stream, file_metadata, file_metadata_object):
        path = WaterButlerPath('/phile', prepend=provider.folder)
        renamed_path = WaterButlerPath('/phile (1)', prepend=provider.folder)
        path._parts[-1]._id = 'fake_id'

        provider.handle_name_conflict = utils.MockCoroutine(return_value=(renamed_path, True))
        url = provider._webdav_url_ + renamed_path.full_path

        aiohttpretty.register_uri('PROPFIND', url, body=file_metadata, auto_length=True, status=207)
        aiohttpretty.register_uri('PUT',
                                  provider._webdav_url_ + '/my_folder/phile (1)',
                                  body=b'squares',
                                  auto_length=True,
                                  status=201)
        metadata, created = await provider.upload(file_stream, path, 'keep')

        assert created is True
        assert metadata.name == file_metadata_object.name
        assert metadata.size == file_metadata_object.size
        assert metadata.size_as_int == int(file_metadata_object.size)
        assert aiohttpretty.has_call(method='PUT', uri=url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete(self, provider, file_metadata):
        path = WaterButlerPath('/phile', prepend=provider.folder)
        url = provider._webdav_url_ + path.full_path
        aiohttpretty.register_uri('PROPFIND', url, body=file_metadata, auto_length=True, status=207)
        path = await provider.validate_path('/phile')
        url = provider._webdav_url_ + path.full_path
        aiohttpretty.register_uri('DELETE', url, status=204)
        await provider.delete(path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_create_folder(self, provider, folder_contents_metadata):
        path = WaterButlerPath('/pumpkin/', prepend=provider.folder)
        folder_url = provider._webdav_url_ + path.full_path
        aiohttpretty.register_uri('MKCOL', folder_url, status=201)

        parent_url = provider._webdav_url_ + path.parent.full_path
        aiohttpretty.register_uri('PROPFIND', parent_url, body=folder_contents_metadata,
                                  auto_length=True, status=207)

        folder_metadata = await provider.create_folder(path)
        assert folder_metadata.name == 'pumpkin'
        assert folder_metadata.path == '/pumpkin/'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_create_folder_naming_conflict(self, provider, folder_contents_metadata):
        path = WaterButlerPath('/pumpkin/', prepend=provider.folder)
        folder_url = provider._webdav_url_ + path.full_path
        aiohttpretty.register_uri('MKCOL', folder_url, status=405)

        with pytest.raises(exceptions.FolderNamingConflict):
            await provider.create_folder(path)


class TestIntraMoveCopy:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_copy_folder(self,
                                     provider,
                                     moved_folder_metadata,
                                     moved_parent_folder_metadata):
        provider.folder = '/'
        src_path = WaterButlerPath('/moved_folder/', prepend=provider.folder, folder=True)
        dest_path = WaterButlerPath('/parent_folder/moved_folder/',
                                    prepend=provider.folder,
                                    folder=True)

        url = provider._webdav_url_ + src_path.full_path
        metadata_parent_url = provider._webdav_url_ + dest_path.parent.full_path
        metadata_url = provider._webdav_url_ + dest_path.full_path
        aiohttpretty.register_uri('COPY', url, auto_length=True, status=201)
        aiohttpretty.register_uri('PROPFIND', metadata_url, body=moved_folder_metadata, status=207)
        aiohttpretty.register_uri('PROPFIND',
                                  metadata_parent_url,
                                  body=moved_parent_folder_metadata,
                                  status=207)

        metadata, exists = await provider.intra_copy(None, src_path, dest_path)

        assert exists
        assert metadata.name == 'moved_folder'
        assert metadata.kind == 'folder'
        assert len(metadata.children) == 1
        assert metadata.children[0].name == 'child_file'
        assert metadata.children[0].kind == 'file'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_move_folder(self,
                                     provider,
                                     moved_folder_metadata,
                                     moved_parent_folder_metadata):
        provider.folder = '/'
        src_path = WaterButlerPath('/moved_folder/', prepend=provider.folder, folder=True)
        dest_path = WaterButlerPath('/parent_folder/moved_folder/',
                                    prepend=provider.folder,
                                    folder=True)

        url = provider._webdav_url_ + src_path.full_path
        metadata_parent_url = provider._webdav_url_ + dest_path.parent.full_path
        metadata_url = provider._webdav_url_ + dest_path.full_path
        aiohttpretty.register_uri('MOVE', url, auto_length=True, status=201)
        aiohttpretty.register_uri('PROPFIND', metadata_url, body=moved_folder_metadata, status=207)
        aiohttpretty.register_uri('PROPFIND',
                                  metadata_parent_url,
                                  body=moved_parent_folder_metadata,
                                  status=207)

        metadata, exists = await provider.intra_move(None, src_path, dest_path)

        assert exists
        assert metadata.name == 'moved_folder'
        assert metadata.kind == 'folder'
        assert len(metadata.children) == 1
        assert metadata.children[0].name == 'child_file'
        assert metadata.children[0].kind == 'file'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_copy_file(self, provider, file_metadata):
        provider.folder = '/'
        src_path = WaterButlerPath('/dissertation.aux', prepend=provider.folder, folder=True)
        dest_path = WaterButlerPath('/parent_folder/', prepend=provider.folder, folder=False)

        url = provider._webdav_url_ + src_path.full_path
        metadata_url = provider._webdav_url_ + dest_path.full_path
        aiohttpretty.register_uri('COPY', url, auto_length=True, status=201)
        aiohttpretty.register_uri('PROPFIND', metadata_url, body=file_metadata, status=207)

        metadata, exists = await provider.intra_copy(None, src_path, dest_path)

        assert exists
        assert metadata.name == 'dissertation.aux'
        assert metadata.kind == 'file'


class TestMetadata:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata(self, provider, folder_list):
        path = WaterButlerPath('/', prepend=provider.folder)
        url = provider._webdav_url_ + path.full_path
        aiohttpretty.register_uri('PROPFIND', url, body=folder_list, auto_length=True, status=207)
        path = await provider.validate_path('/')
        url = provider._webdav_url_ + path.full_path
        aiohttpretty.register_uri('PROPFIND', url, body=folder_list, status=207)
        result = await provider.metadata(path)

        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0].kind == 'folder'
        assert result[0].name == 'Documents'


class TestRevisions:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_revisions(self, provider, file_metadata):
        path = WaterButlerPath('/dissertation.aux', prepend=provider.folder)
        url = provider._webdav_url_ + path.full_path
        aiohttpretty.register_uri('PROPFIND', url, body=file_metadata, auto_length=True, status=207)

        result = await provider.revisions(path)

        assert isinstance(result, list)
        assert isinstance(result[0], OwnCloudFileRevisionMetadata)
        assert len(result) == 1
        assert result[0].modified == 'Sun, 10 Jul 2016 23:28:31 GMT'


class TestOperations:

    def test_can_intra_copy(self, provider, provider_different_credentials):
        assert provider.can_intra_copy(provider)
        assert not provider.can_intra_copy(provider_different_credentials)

    def test_can_intra_move(self, provider, provider_different_credentials):
        assert provider.can_intra_move(provider)
        assert not provider.can_intra_move(provider_different_credentials)

    def test_shares_storage_root(self, provider, provider_different_credentials):
        assert provider.shares_storage_root(provider)
        assert not provider.shares_storage_root(provider_different_credentials)

    def test_can_duplicate_names(self, provider):
        assert provider.can_duplicate_names()
