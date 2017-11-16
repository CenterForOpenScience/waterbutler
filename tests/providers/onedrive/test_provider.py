import io
import pytest

import aiohttpretty

from waterbutler.core import streams
from waterbutler.core import exceptions

from waterbutler.providers.onedrive import OneDriveProvider
from waterbutler.providers.onedrive.provider import OneDrivePath
from waterbutler.providers.onedrive.metadata import OneDriveFileMetadata
from waterbutler.providers.onedrive.metadata import OneDriveFolderMetadata
from waterbutler.providers.onedrive.metadata import OneDriveRevisionMetadata

from tests.providers.onedrive.fixtures import (download_fixtures,
                                               revision_fixtures,
                                               root_provider_fixtures,
                                               subfolder_provider_fixtures)


@pytest.fixture
def auth():
    return {
        'name': 'cat',
        'email': 'cat@cat.com',
    }


@pytest.fixture
def credentials():
    return {'token': 'wrote harry potter'}


@pytest.fixture
def subfolder_settings(subfolder_provider_fixtures):
    return {'folder': subfolder_provider_fixtures['root_id']}


@pytest.fixture
def subfolder_provider(auth, credentials, subfolder_settings):
    """Provider root is subfolder of OneDrive account root"""
    return OneDriveProvider(auth, credentials, subfolder_settings)


@pytest.fixture
def root_settings():
    return {'folder': 'root'}


@pytest.fixture
def root_provider(auth, credentials, root_settings):
    """Provider root is OneDrive account root"""
    return OneDriveProvider(auth, credentials, root_settings)


@pytest.fixture
def provider(root_provider):
    """Alias"""
    return root_provider


@pytest.fixture
def file_content():
    return b'SLEEP IS FOR OSX GO SERVE STREAMS'


@pytest.fixture
def file_like(file_content):
    return io.BytesIO(file_content)


@pytest.fixture
def file_stream(file_like):
    return streams.FileStreamReader(file_like)


class TestRootProviderValidatePath:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_root(self, root_provider):
        try:
            wb_path_v1 = await root_provider.validate_v1_path('/')
        except Exception as exc:
            pytest.fail(str(exc))

        wb_path_v0 = await root_provider.validate_path('/')

        assert wb_path_v1 == wb_path_v0
        assert wb_path_v1.identifier == 'root'

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_validate_v1_path_file(self, root_provider, root_provider_fixtures):
        file_id = root_provider_fixtures['file_id']
        file_metadata = root_provider_fixtures['file_metadata']

        item_url = root_provider._build_item_url(file_id)
        print('item url: {}'.format(item_url))
        aiohttpretty.register_json_uri('GET', item_url, body=file_metadata, status=200)

        file_path = '/{}'.format(file_id)
        try:
            wb_path_v1 = await root_provider.validate_v1_path(file_path)
        except Exception as exc:
            pytest.fail(str(exc))

        file_name = '/{}'.format(file_metadata['name'])
        assert str(wb_path_v1) == file_name
        assert wb_path_v1.identifier == file_id

        wb_path_v0 = await root_provider.validate_path(file_path)
        assert str(wb_path_v0) == file_name

        assert wb_path_v1 == wb_path_v0

        with pytest.raises(exceptions.NotFoundError) as exc:
            await root_provider.validate_v1_path(file_path + '/')

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_validate_v1_path_folder(self, root_provider, root_provider_fixtures):
        folder_id = root_provider_fixtures['folder_id']
        folder_metadata = root_provider_fixtures['folder_metadata']

        item_url = root_provider._build_item_url(folder_id)
        print('item url: {}'.format(item_url))
        aiohttpretty.register_json_uri('GET', item_url, body=folder_metadata, status=200)

        folder_path = '/{}/'.format(folder_id)
        folder_name = '/{}/'.format(folder_metadata['name'])
        try:
            wb_path_v1 = await root_provider.validate_v1_path(folder_path)
        except Exception as exc:
            pytest.fail(str(exc))

        assert str(wb_path_v1) == folder_name
        assert wb_path_v1.identifier == folder_id

        wb_path_v0 = await root_provider.validate_path(folder_path)
        assert str(wb_path_v0) == folder_name

        assert wb_path_v1 == wb_path_v0

        with pytest.raises(exceptions.NotFoundError) as exc:
            await root_provider.validate_v1_path(folder_path.rstrip('/'))


class TestSubfolderProviderValidatePath:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_root(self, subfolder_provider, subfolder_provider_fixtures):
        try:
            wb_path_v1 = await subfolder_provider.validate_v1_path('/')
        except Exception as exc:
            pytest.fail(str(exc))

        wb_path_v0 = await subfolder_provider.validate_path('/')

        assert wb_path_v1 == wb_path_v0
        assert wb_path_v1.identifier == subfolder_provider_fixtures['root_id']

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_validate_v1_path_folder(self, subfolder_provider, subfolder_provider_fixtures):
        folder_id = subfolder_provider_fixtures['folder_id']
        folder_metadata = subfolder_provider_fixtures['folder_metadata']

        item_url = subfolder_provider._build_item_url(folder_id)
        print('item url: {}'.format(item_url))
        aiohttpretty.register_json_uri('GET', item_url, body=folder_metadata, status=200)

        folder_path = '/{}/'.format(folder_id)
        folder_name = '/{}/'.format(folder_metadata['name'])
        try:
            wb_path_v1 = await subfolder_provider.validate_v1_path(folder_path)
        except Exception as exc:
            pytest.fail(str(exc))

        assert str(wb_path_v1) == folder_name
        assert wb_path_v1.identifier == folder_id

        wb_path_v0 = await subfolder_provider.validate_path(folder_path)
        assert str(wb_path_v0) == folder_name

        assert wb_path_v1 == wb_path_v0

        with pytest.raises(exceptions.NotFoundError) as exc:
            await subfolder_provider.validate_v1_path(folder_path.rstrip('/'))

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_validate_v1_path_file_is_child(self, subfolder_provider,
                                                  subfolder_provider_fixtures):
        """file is immediate child of provider base folder"""
        file_id = subfolder_provider_fixtures['file_id']
        file_metadata = subfolder_provider_fixtures['file_metadata']

        item_url = subfolder_provider._build_item_url(file_id)
        print('item url: {}'.format(item_url))
        aiohttpretty.register_json_uri('GET', item_url, body=file_metadata, status=200)

        file_path = '/{}'.format(file_id)
        file_name = '/{}'.format(file_metadata['name'])
        try:
            wb_path_v1 = await subfolder_provider.validate_v1_path(file_path)
        except Exception as exc:
            pytest.fail(str(exc))

        assert str(wb_path_v1) == file_name
        assert wb_path_v1.identifier == file_id

        wb_path_v0 = await subfolder_provider.validate_path(file_path)
        assert str(wb_path_v0) == file_name

        assert wb_path_v1 == wb_path_v0

        with pytest.raises(exceptions.NotFoundError) as exc:
            await subfolder_provider.validate_v1_path(file_path + '/')

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_validate_v1_path_file_is_grandchild(self, subfolder_provider,
                                                       subfolder_provider_fixtures):
        """file is *not* immediate child of provider base folder"""
        subfile_id = subfolder_provider_fixtures['subfile_id']
        subfile_metadata = subfolder_provider_fixtures['subfile_metadata']

        item_url = subfolder_provider._build_item_url(subfile_id)
        print('item url: {}'.format(item_url))
        aiohttpretty.register_json_uri('GET', item_url, body=subfile_metadata, status=200)

        root_url = subfolder_provider._build_item_url(subfolder_provider_fixtures['root_id'])
        print('root url: {}'.format(root_url))
        aiohttpretty.register_json_uri('GET', root_url,
                                       body=subfolder_provider_fixtures['root_metadata'],
                                       status=200)

        subfile_path = '/{}'.format(subfile_id)
        subfile_name = '/{}/{}'.format(subfolder_provider_fixtures['folder_metadata']['name'],
                                       subfile_metadata['name'])
        try:
            wb_path_v1 = await subfolder_provider.validate_v1_path(subfile_path)
        except Exception as exc:
            pytest.fail(str(exc))
        assert str(wb_path_v1) == subfile_name

        wb_path_v0 = await subfolder_provider.validate_path(subfile_path)
        assert str(wb_path_v0) == subfile_name

        assert wb_path_v1 == wb_path_v0

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_validate_v1_path_file_is_outside_root(self, subfolder_provider,
                                                         subfolder_provider_fixtures):
        """file is outside of the base storage root"""
        file_id = subfolder_provider_fixtures['outside_file_id']
        file_metadata = subfolder_provider_fixtures['outside_file_metadata']

        item_url = subfolder_provider._build_item_url(file_id)
        aiohttpretty.register_json_uri('GET', item_url, body=file_metadata, status=200)

        root_url = subfolder_provider._build_item_url(subfolder_provider_fixtures['root_id'])
        aiohttpretty.register_json_uri('GET', root_url,
                                       body=subfolder_provider_fixtures['root_metadata'],
                                       status=200)

        file_path = '/{}'.format(file_id)
        with pytest.raises(exceptions.NotFoundError) as exc:
            await subfolder_provider.validate_v1_path(file_path)

        with pytest.raises(exceptions.NotFoundError) as exc:
            await subfolder_provider.validate_path(file_path)


class TestRevalidatePath:

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_revalidate_path_file(self, root_provider, root_provider_fixtures):
        file_name = 'toes.txt'
        file_id = root_provider_fixtures['file_id']
        root_id = 'root'

        parent_path = OneDrivePath('/', _ids=[root_id])
        expected_path = OneDrivePath('/{}'.format(file_name), _ids=[root_id, file_id])

        parent_url = root_provider._build_drive_url(*parent_path.api_identifier, expand='children')
        aiohttpretty.register_json_uri('GET', parent_url,
                                       body=root_provider_fixtures['root_metadata'], status=200)

        actual_path = await root_provider.revalidate_path(parent_path, file_name, False)
        assert actual_path == expected_path

        with pytest.raises(exceptions.NotFoundError) as exc:
            await root_provider.revalidate_path(parent_path, file_name, True)

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_revalidate_path_folder(self, root_provider, root_provider_fixtures):
        folder_name = 'teeth'
        folder_id = root_provider_fixtures['folder_id']
        root_id = 'root'

        parent_path = OneDrivePath('/', _ids=[root_id])
        expected_path = OneDrivePath('/{}/'.format(folder_name), _ids=[root_id, folder_id])

        parent_url = root_provider._build_drive_url(*parent_path.api_identifier, expand='children')
        aiohttpretty.register_json_uri('GET', parent_url,
                                       body=root_provider_fixtures['root_metadata'], status=200)

        actual_path = await root_provider.revalidate_path(parent_path, folder_name, True)
        assert actual_path == expected_path

        with pytest.raises(exceptions.NotFoundError) as exc:
            await root_provider.revalidate_path(parent_path, folder_name, False)

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_revalidate_path_subfile(self, root_provider, root_provider_fixtures):
        root_id = 'root'
        parent_id = root_provider_fixtures['folder_id']
        subfile_id = root_provider_fixtures['subfile_id']

        parent_name = 'teeth'
        subfile_name = 'bicuspid.txt'

        parent_path = OneDrivePath('/{}/'.format(parent_name), _ids=[root_id, parent_id])
        expected_path = OneDrivePath('/{}/{}'.format(parent_name, subfile_name),
                                     _ids=[root_id, parent_id, subfile_id])

        parent_url = root_provider._build_drive_url(*parent_path.api_identifier, expand='children')
        aiohttpretty.register_json_uri('GET', parent_url,
                                       body=root_provider_fixtures['folder_metadata'], status=200)

        actual_path = await root_provider.revalidate_path(parent_path, subfile_name, False)
        assert actual_path == expected_path

        with pytest.raises(exceptions.NotFoundError) as exc:
            await root_provider.revalidate_path(parent_path, subfile_name, True)


class TestMetadata:

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_metadata_root(self, subfolder_provider, subfolder_provider_fixtures):

        path = OneDrivePath('/', _ids=(subfolder_provider_fixtures['root_id'], ))

        list_url = subfolder_provider._build_drive_url(*path.api_identifier, expand='children')
        aiohttpretty.register_json_uri('GET', list_url, body=subfolder_provider_fixtures['root_metadata'])

        result = await subfolder_provider.metadata(path)
        assert len(result) == 2

        folder_metadata = result[0]
        assert folder_metadata.kind == 'folder'
        assert folder_metadata.name == 'crushers'

        file_metadata = result[1]
        assert file_metadata.kind == 'file'
        assert file_metadata.name == 'bicuspid.txt'

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_metadata_folder(self, subfolder_provider, subfolder_provider_fixtures):
        folder_id = subfolder_provider_fixtures['folder_id']
        folder_metadata = subfolder_provider_fixtures['folder_metadata']
        folder_name = folder_metadata['name']
        path = OneDrivePath('/{}/'.format(folder_name),
                            _ids=(subfolder_provider_fixtures['root_id'], folder_id, ))

        list_url = subfolder_provider._build_drive_url(*path.api_identifier, expand='children')
        aiohttpretty.register_json_uri('GET', list_url, body=folder_metadata)

        result = await subfolder_provider.metadata(path)
        assert len(result) == 1

        file_metadata = result[0]
        assert file_metadata.kind == 'file'
        assert file_metadata.name == 'molars.txt'

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_metadata_file(self, subfolder_provider, subfolder_provider_fixtures):
        file_id = subfolder_provider_fixtures['file_id']
        file_metadata = subfolder_provider_fixtures['file_metadata']
        file_name = file_metadata['name']
        path = OneDrivePath('/{}'.format(file_name),
                            _ids=(subfolder_provider_fixtures['root_id'], file_id, ))

        list_url = subfolder_provider._build_drive_url(*path.api_identifier, expand='children')
        aiohttpretty.register_json_uri('GET', list_url, body=file_metadata)

        result = await subfolder_provider.metadata(path)
        assert result.kind == 'file'
        assert result.name == 'bicuspid.txt'


class TestRevisions:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_get_revisions(self, provider, revision_fixtures):
        file_id = revision_fixtures['file_id']
        path = OneDrivePath('/bicuspids.txt', _ids=[revision_fixtures['root_id'], file_id])

        revision_response = revision_fixtures['file_revisions']
        revisions_url = provider._build_drive_url('items', file_id, 'view.delta',
                                                  top=provider.MAX_REVISIONS)
        aiohttpretty.register_json_uri('GET', revisions_url, body=revision_response)

        result = await provider.revisions(path)

        assert len(result) == 1


class TestDownload:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_standard_file(self, provider, download_fixtures):
        file_id = download_fixtures['file_id']
        path = OneDrivePath('/toes.txt', _ids=[download_fixtures['root_id'], file_id])

        metadata_response = download_fixtures['file_metadata']
        metadata_url = provider._build_drive_url('items', file_id)
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_response)

        aiohttpretty.register_uri('GET', download_fixtures['file_download_url'],
                                  body=download_fixtures['file_content'],
                                  headers={'Content-Length': '11'})

        response = await provider.download(path)
        content = await response.read()
        assert content == b'ten of them'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_by_revision(self, provider, download_fixtures):
        file_id = download_fixtures['file_id']
        path = OneDrivePath('/toes.txt', _ids=[download_fixtures['root_id'], file_id])

        revision_response = download_fixtures['file_revisions']
        revisions_url = provider._build_drive_url('items', file_id, 'view.delta',
                                                  top=provider.MAX_REVISIONS)
        aiohttpretty.register_json_uri('GET', revisions_url, body=revision_response)

        aiohttpretty.register_uri('GET', download_fixtures['file_revision_download_url'],
                                  body=download_fixtures['file_content'],
                                  headers={'Content-Length': '11'})

        response = await provider.download(path, revision=download_fixtures['file_revision'])
        content = await response.read()
        assert content == b'ten of them'

    @pytest.mark.asyncio
    async def test_download_no_such_file(self, provider):
        od_path = OneDrivePath('/does-not-exists', _ids=[None, None])
        with pytest.raises(exceptions.DownloadError) as exc:
            await provider.download(od_path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_by_bad_revision(self, provider, download_fixtures):
        file_id = download_fixtures['file_id']
        path = OneDrivePath('/toes.txt', _ids=[download_fixtures['root_id'], file_id])

        revision_response = download_fixtures['file_revisions']
        revisions_url = provider._build_drive_url('items', file_id, 'view.delta',
                                                  top=provider.MAX_REVISIONS)
        aiohttpretty.register_json_uri('GET', revisions_url, body=revision_response)

        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider.download(path, revision='thisisafakerevision')

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_unexportable_file(self, provider, download_fixtures):
        onenote_id = download_fixtures['onenote_id']
        path = OneDrivePath('/onenote', _ids=[download_fixtures['root_id'], onenote_id])

        metadata_response = download_fixtures['onenote_metadata']
        metadata_url = provider._build_drive_url('items', onenote_id)
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_response)

        with pytest.raises(exceptions.UnexportableFileTypeError) as exc:
            await provider.download(path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_unexportable_by_revision(self, provider, download_fixtures):
        onenote_id = download_fixtures['onenote_id']
        path = OneDrivePath('/onenote', _ids=[download_fixtures['root_id'], onenote_id])

        revision_response = download_fixtures['onenote_revisions']
        revisions_url = provider._build_drive_url('items', onenote_id, 'view.delta',
                                                  top=provider.MAX_REVISIONS)
        aiohttpretty.register_json_uri('GET', revisions_url, body=revision_response)

        with pytest.raises(exceptions.UnexportableFileTypeError) as exc:
            await provider.download(path, revision=download_fixtures['onenote_revision'])


class TestReadOnlyProvider:

    @pytest.mark.asyncio
    async def test_upload(self, provider):
        with pytest.raises(exceptions.ReadOnlyProviderError) as e:
            await provider.upload('/foo-file.txt')
        assert e.value.code == 501

    @pytest.mark.asyncio
    async def test_delete(self, provider):
        with pytest.raises(exceptions.ReadOnlyProviderError) as e:
            await provider.delete()
        assert e.value.code == 501

    @pytest.mark.asyncio
    async def test_move(self, provider):
        with pytest.raises(exceptions.ReadOnlyProviderError) as e:
            await provider.move()
        assert e.value.code == 501

    @pytest.mark.asyncio
    async def test_copy_to(self, provider):
        with pytest.raises(exceptions.ReadOnlyProviderError) as e:
            await provider.copy(provider)
        assert e.value.code == 501

    def test_can_intra_move(self, provider):
        assert provider.can_intra_move(provider) == False

    def test_can_intra_copy(self, provider):
        assert provider.can_intra_copy(provider) == False


# leftover bits
class TestMisc:

    def test_can_duplicate_name(self, provider):
        assert provider.can_duplicate_names() == False
