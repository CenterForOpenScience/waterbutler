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

from tests import utils
from tests.providers.onedrive.fixtures import (download_fixtures,
                                               revision_fixtures,
                                               root_provider_fixtures,
                                               subfolder_provider_fixtures,
                                               readwrite_fixtures)


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
def other_credentials():
    return {'token': 'wrote lord of the rings'}


@pytest.fixture
def subfolder_settings(subfolder_provider_fixtures):
    return {'folder': subfolder_provider_fixtures['root_id'], 'drive_id': '43218765'}


@pytest.fixture
def subfolder_provider(auth, credentials, subfolder_settings):
    """Provider root is subfolder of OneDrive account root"""
    return OneDriveProvider(auth, credentials, subfolder_settings)


@pytest.fixture
def root_settings():
    return {'folder': 'root', 'drive_id': 'deadbeef'}


@pytest.fixture
def root_provider(auth, credentials, root_settings):
    """Provider root is OneDrive account root"""
    return OneDriveProvider(auth, credentials, root_settings)


@pytest.fixture
def provider(root_provider):
    return root_provider


@pytest.fixture
def other_provider(auth, other_credentials, root_settings):
    return OneDriveProvider(auth, other_credentials, root_settings)


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

        item_url = root_provider._build_graph_item_url(file_id)
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

        item_url = root_provider._build_graph_item_url(folder_id)
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

        item_url = subfolder_provider._build_graph_item_url(folder_id)
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

        item_url = subfolder_provider._build_graph_item_url(file_id)
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

        item_url = subfolder_provider._build_graph_item_url(subfile_id)
        aiohttpretty.register_json_uri('GET', item_url, body=subfile_metadata, status=200)

        root_url = subfolder_provider._build_graph_item_url(subfolder_provider_fixtures['root_id'])
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

        item_url = subfolder_provider._build_graph_item_url(file_id)
        aiohttpretty.register_json_uri('GET', item_url, body=file_metadata, status=200)

        root_url = subfolder_provider._build_graph_item_url(subfolder_provider_fixtures['root_id'])
        aiohttpretty.register_json_uri('GET', root_url,
                                       body=subfolder_provider_fixtures['root_metadata'],
                                       status=200)

        file_path = '/{}'.format(file_id)
        with pytest.raises(exceptions.NotFoundError):
            await subfolder_provider.validate_v1_path(file_path)

        with pytest.raises(exceptions.NotFoundError):
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

        parent_url = root_provider._build_graph_item_url(parent_path.identifier, 'children')
        aiohttpretty.register_json_uri('GET', parent_url,
                                       body=root_provider_fixtures['root_metadata_children'],
                                       status=200)

        actual_path = await root_provider.revalidate_path(parent_path, file_name, False)
        assert actual_path == expected_path

        potential_name = 'this-should-not-exist'
        potential_expected_path = OneDrivePath('/{}'.format(potential_name), _ids=[root_id])
        potential_returned_path = await root_provider.revalidate_path(parent_path, potential_name,
                                                                      False)
        assert potential_returned_path == potential_expected_path

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_revalidate_path_folder(self, root_provider, root_provider_fixtures):
        folder_name = 'teeth'
        folder_id = root_provider_fixtures['folder_id']
        root_id = 'root'

        parent_path = OneDrivePath('/', _ids=[root_id])
        expected_path = OneDrivePath('/{}/'.format(folder_name), _ids=[root_id, folder_id])

        parent_url = root_provider._build_graph_item_url(parent_path.identifier, 'children')
        aiohttpretty.register_json_uri('GET', parent_url,
                                       body=root_provider_fixtures['root_metadata_children'],
                                       status=200)

        actual_path = await root_provider.revalidate_path(parent_path, folder_name, True)
        assert actual_path == expected_path

        potential_name = 'this-should-not-exist'
        potential_expected_path = OneDrivePath('/{}/'.format(potential_name), _ids=[root_id])
        potential_returned_path = await root_provider.revalidate_path(parent_path, potential_name,
                                                                      True)
        assert potential_returned_path == potential_expected_path

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

        parent_url = root_provider._build_graph_item_url(parent_path.identifier, 'children')
        aiohttpretty.register_json_uri('GET', parent_url,
                                       body=root_provider_fixtures['folder_metadata_children'],
                                       status=200)

        actual_path = await root_provider.revalidate_path(parent_path, subfile_name, False)
        assert actual_path == expected_path

        potential_name = 'this-should-not-exist'
        potential_expected_path = OneDrivePath('/{}/{}'.format(parent_name, potential_name),
                                               _ids=[root_id, parent_id])
        expected_path           = OneDrivePath('/{}/{}'.format(parent_name, subfile_name),
                                             _ids=[root_id, parent_id, subfile_id])
        potential_returned_path = await root_provider.revalidate_path(parent_path, potential_name,
                                                                      False)
        assert potential_returned_path == potential_expected_path

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_revalidate_path_file_no_base(self, root_provider, root_provider_fixtures):
        file_name = 'toes.txt'
        root_id = '12345'

        parent_path = OneDrivePath('/', _ids=[root_id])
        expected_path = OneDrivePath('/{}'.format(file_name), _ids=[root_id, None])

        parent_url = root_provider._build_graph_item_url(parent_path.identifier, 'children')
        aiohttpretty.register_json_uri('GET', parent_url, status=404)

        actual_path = await root_provider.revalidate_path(parent_path, file_name, False)
        assert actual_path == expected_path


class TestMetadata:

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_metadata_root(self, subfolder_provider, subfolder_provider_fixtures):

        path = OneDrivePath('/', _ids=(subfolder_provider_fixtures['root_id'], ))

        list_url = subfolder_provider._build_graph_item_url(path.identifier, expand='children')
        aiohttpretty.register_json_uri('GET', list_url,
                                       body=subfolder_provider_fixtures['root_metadata'])

        result = await subfolder_provider.metadata(path)
        assert len(result) == 2

        folder_metadata = result[0]
        assert folder_metadata.kind == 'folder'
        assert folder_metadata.name == 'crushers'
        assert folder_metadata.materialized_path == '/crushers/'

        file_metadata = result[1]
        assert file_metadata.kind == 'file'
        assert file_metadata.name == 'bicuspid.txt'
        assert file_metadata.materialized_path == '/bicuspid.txt'

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_metadata_folder(self, subfolder_provider, subfolder_provider_fixtures):
        folder_id = subfolder_provider_fixtures['folder_id']
        folder_metadata = subfolder_provider_fixtures['folder_metadata']
        folder_name = folder_metadata['name']
        path = OneDrivePath('/{}/'.format(folder_name),
                            _ids=(subfolder_provider_fixtures['root_id'], folder_id, ))

        list_url = subfolder_provider._build_graph_item_url(path.identifier, expand='children')
        aiohttpretty.register_json_uri('GET', list_url, body=folder_metadata)

        result = await subfolder_provider.metadata(path)
        assert len(result) == 1

        file_metadata = result[0]
        assert file_metadata.kind == 'file'
        assert file_metadata.name == 'molars.txt'
        assert file_metadata.materialized_path == '/crushers/molars.txt'

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_metadata_file(self, subfolder_provider, subfolder_provider_fixtures):
        file_id = subfolder_provider_fixtures['file_id']
        file_metadata = subfolder_provider_fixtures['file_metadata']
        file_name = file_metadata['name']
        path = OneDrivePath('/{}'.format(file_name),
                            _ids=(subfolder_provider_fixtures['root_id'], file_id, ))

        list_url = subfolder_provider._build_graph_item_url(path.identifier, expand='children')
        aiohttpretty.register_json_uri('GET', list_url, body=file_metadata)

        result = await subfolder_provider.metadata(path)
        assert result.kind == 'file'
        assert result.name == 'bicuspid.txt'
        assert result.materialized_path == '/bicuspid.txt'

    @pytest.mark.asyncio
    async def test_metadata_file_not_found(self, subfolder_provider):
        path = OneDrivePath('/{}'.format('no-such-file.bin'), _ids=('root', None, ))
        with pytest.raises(exceptions.NotFoundError):
            _ = await subfolder_provider.metadata(path)

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_metadata_empty_folder(self, provider, root_provider_fixtures):
        path = OneDrivePath('/tonk/', _ids=['root', '123!456'])

        list_url = provider._build_graph_item_url(path.identifier, expand='children')
        aiohttpretty.register_json_uri('GET', list_url,
                                       body=root_provider_fixtures['empty_folder_metadata'])

        result = await provider.metadata(path)
        assert result == []


class TestRevisions:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_get_revisions(self, provider, revision_fixtures):
        file_id = revision_fixtures['file_id']
        path = OneDrivePath('/bicuspids.txt', _ids=[revision_fixtures['root_id'], file_id])

        revision_response = revision_fixtures['file_revisions']
        revisions_url = provider._build_graph_item_url(path.identifier, 'versions')
        aiohttpretty.register_json_uri('GET', revisions_url, body=revision_response)

        result = await provider.revisions(path)

        assert len(result) == 5


class TestDownload:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_standard_file(self, provider, download_fixtures):
        file_id = download_fixtures['file_id']
        path = OneDrivePath('/toes.txt', _ids=[download_fixtures['root_id'], file_id])

        metadata_response = download_fixtures['file_metadata']
        metadata_url = provider._build_graph_drive_url('items', file_id)
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_response)

        aiohttpretty.register_uri('GET', download_fixtures['file_download_url'],
                                  body=download_fixtures['file_content'],
                                  headers={'Content-Length': '11'})

        response = await provider.download(path)
        content = await response.read()
        assert content == b'ten of them'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_standard_file_range(self, provider, download_fixtures):
        file_id = download_fixtures['file_id']
        path = OneDrivePath('/toes.txt', _ids=[download_fixtures['root_id'], file_id])

        metadata_response = download_fixtures['file_metadata']
        metadata_url = provider._build_graph_drive_url('items', file_id)
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_response)

        download_url = download_fixtures['file_download_url']
        aiohttpretty.register_uri('GET', download_url, status=206,
                                  body=download_fixtures['file_content'][0:2])

        response = await provider.download(path, range=(0, 1))
        assert response.partial
        content = await response.read()
        assert content == b'te'
        assert aiohttpretty.has_call(method='GET', uri=download_url,
                                     headers={'Range': 'bytes=0-1',
                                              'Authorization': 'bearer wrote harry potter',
                                              'accept-encoding': ''})

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_by_revision(self, provider, download_fixtures, revision_fixtures):
        file_id = download_fixtures['file_id']
        path = OneDrivePath('/toes.txt', _ids=[download_fixtures['root_id'], file_id])

        revision_response = revision_fixtures['file_revisions']
        revisions_url = provider._build_graph_item_url(path.identifier, 'versions')
        aiohttpretty.register_json_uri('GET', revisions_url, body=revision_response)

        aiohttpretty.register_uri('GET', revision_fixtures['file_revision_download_url'],
                                  body=download_fixtures['file_content'],
                                  headers={'Content-Length': '11'})

        response = await provider.download(path, revision=revision_fixtures['revision_id'])
        content = await response.read()
        assert content == b'ten of them'

    @pytest.mark.asyncio
    async def test_download_no_such_file(self, provider):
        od_path = OneDrivePath('/does-not-exists', _ids=[None, None])
        with pytest.raises(exceptions.DownloadError):
            await provider.download(od_path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_by_bad_revision(self, provider, download_fixtures):
        file_id = download_fixtures['file_id']
        path = OneDrivePath('/toes.txt', _ids=[download_fixtures['root_id'], file_id])

        revision_response = download_fixtures['file_revisions']
        revisions_url = provider._build_graph_item_url(path.identifier, 'versions')
        aiohttpretty.register_json_uri('GET', revisions_url, body=revision_response)

        with pytest.raises(exceptions.NotFoundError):
            await provider.download(path, revision='thisisafakerevision')

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_unexportable_file(self, provider, download_fixtures):
        onenote_id = download_fixtures['onenote_id']
        path = OneDrivePath('/onenote', _ids=[download_fixtures['root_id'], onenote_id])

        metadata_response = download_fixtures['onenote_metadata']
        metadata_url = provider._build_graph_item_url(onenote_id)
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_response)

        with pytest.raises(exceptions.UnexportableFileTypeError):
            await provider.download(path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_unexportable_by_revision(self, provider, download_fixtures):
        onenote_id = download_fixtures['onenote_id']
        path = OneDrivePath('/onenote', _ids=[download_fixtures['root_id'], onenote_id])

        revision_response = download_fixtures['onenote_revisions']
        revisions_url = provider._build_graph_drive_url('items', onenote_id, 'versions')
        aiohttpretty.register_json_uri('GET', revisions_url, body=revision_response)

        with pytest.raises(exceptions.UnexportableFileTypeError):
            await provider.download(path,
                                    revision=download_fixtures['onenote_revision_non_exportable'])


class TestCreateFolder:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_create_folder_in_root(self, provider, readwrite_fixtures):
        folder_sub_response = readwrite_fixtures['folder_sub_response']
        newfolder_path = OneDrivePath('/ryan-test1/', _ids=['root'])
        create_url = provider._build_graph_item_url('root', 'children')
        aiohttpretty.register_json_uri('POST', create_url, body=folder_sub_response, status=201)

        resp = await provider.create_folder(newfolder_path)

        assert aiohttpretty.has_call(method='POST', uri=create_url)
        assert resp.kind == 'folder'
        assert resp.name == newfolder_path.name
        assert resp.path == '/75BFE374EBEB1211!107/'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_create_folder_in_subfolder(self, provider, readwrite_fixtures):
        folder_sub_sub_response = readwrite_fixtures['folder_sub_sub_response']
        newfolder_path = OneDrivePath('/ryan-test1/sub1-b/', _ids=['root', '75BFE374EBEB1211!107'])
        create_url = provider._build_graph_item_url('75BFE374EBEB1211!107', 'children')
        aiohttpretty.register_json_uri('POST', create_url, body=folder_sub_sub_response, status=201)

        resp = await provider.create_folder(newfolder_path)

        assert aiohttpretty.has_call(method='POST', uri=create_url)
        assert resp.kind == 'folder'
        assert resp.name == 'sub1-b'
        assert resp.path == '/75BFE374EBEB1211!118/'

    @pytest.mark.asyncio
    async def test_folder_already_exists_with_precheck(self, provider):
        preexisting_folder = OneDrivePath('/already-exists/', _ids=['root', '75BFE374EBEB1211!107'])
        with pytest.raises(exceptions.FolderNamingConflict) as e:
            await provider.create_folder(preexisting_folder)

        assert e.value.code == 409

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_folder_alredy_exists_no_precheck(self, provider, readwrite_fixtures):
        alternative_folder_response = readwrite_fixtures['folder_sub_sub_response']
        newfolder_path = OneDrivePath('/ryan-test1/', _ids=['root', '75BFE374EBEB1211!107'])
        create_url = provider._build_graph_item_url('root', 'children')
        aiohttpretty.register_json_uri('POST', create_url, body=alternative_folder_response,
                                       status=201)

        resp = await provider.create_folder(newfolder_path, folder_precheck=False)

        assert aiohttpretty.has_call(method='POST', uri=create_url)
        assert resp.kind == 'folder'
        assert resp.name == 'sub1-b'
        assert resp.path == '/75BFE374EBEB1211!118/'

    @pytest.mark.asyncio
    async def test_must_be_folder(self, provider):
        with pytest.raises(exceptions.CreateFolderError) as e:
            await provider.create_folder(OneDrivePath('/test.jpg', _ids=['root']))


class TestUpload:

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_contiguous_upload(self, provider, readwrite_fixtures, file_stream):
        file_root_response = readwrite_fixtures['file_root_response']
        path = OneDrivePath('/elect-a.jpg', _ids=['root'])

        file_upload_url = provider._build_graph_item_url('root:', 'elect-a.jpg:', 'content')
        aiohttpretty.register_json_uri('PUT', file_upload_url, body=file_root_response, status=201)

        assert path.identifier is None

        received, created = await provider.upload(file_stream, path)

        assert aiohttpretty.has_call(method='PUT', uri=file_upload_url)

        expected = OneDriveFileMetadata(file_root_response, path)
        assert received == expected
        assert created is True

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_upload_update(self, provider, file_stream, readwrite_fixtures):
        file_root_response = readwrite_fixtures['file_root_response']
        file_id = file_root_response['id']
        upload_path = OneDrivePath('/elect-a.jpg', _ids=['root', file_id])

        metadata_url = provider._build_graph_item_url(file_id, expand='children')
        aiohttpretty.register_json_uri('GET', metadata_url, body=file_root_response, status=200)

        file_upload_url = provider._build_graph_item_url(file_id, 'content')
        aiohttpretty.register_json_uri('PUT', file_upload_url, body=file_root_response, status=200)

        received, created = await provider.upload(file_stream, upload_path, conflict='replace')

        assert aiohttpretty.has_call(method='GET', uri=metadata_url)
        assert aiohttpretty.has_call(method='PUT', uri=file_upload_url)
        assert created is False

        expected = OneDriveFileMetadata(file_root_response, upload_path)
        assert expected == received

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_upload_exists(self, provider, file_stream, readwrite_fixtures):
        file_root_response = readwrite_fixtures['file_root_response']
        path = OneDrivePath('/elect-a.jpg', _ids=['root', '123!456'])

        metadata_url = provider._build_graph_item_url('123!456', expand='children')
        aiohttpretty.register_json_uri('GET', metadata_url, body=file_root_response, status=200)

        with pytest.raises(exceptions.NamingConflict) as e:
            file_metadata, created = await provider.upload(file_stream, path, conflict='warn')

        assert aiohttpretty.has_call(method='GET', uri=metadata_url)
        assert e.value.message == ('Cannot complete action: file or folder "{}" '
                                   'already exists in this location'.format(path.name))

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_upload_rename(self, provider, file_stream, readwrite_fixtures):
        subfolder_children = readwrite_fixtures['subfolder_children']
        file_sub_response = readwrite_fixtures['file_sub_response']
        file_rename_sub_response = readwrite_fixtures['file_rename_sub_response']

        base_folder_id = '75BFE374EBEB1211!107'
        file_one_id = '75BFE374EBEB1211!150'
        file_two_id = '66BFE321EBEB1211!150'

        intended_path = OneDrivePath('/elect-a.jpg', _ids=[base_folder_id, file_one_id])
        actual_path = OneDrivePath('/elect-a (1).jpg', _ids=[base_folder_id, file_two_id])

        intended_metadata_url = provider._build_graph_item_url(file_one_id, expand='children')
        aiohttpretty.register_json_uri('GET', intended_metadata_url, body=file_sub_response,
                                       status=200)

        base_metadata_url = provider._build_graph_item_url(base_folder_id, 'children')
        aiohttpretty.register_json_uri('GET', base_metadata_url, body=subfolder_children,
                                       status=200)

        new_file_upload_url = provider._build_graph_item_url('{}:'.format(base_folder_id),
                                                             'elect-a (1).jpg:', 'content')
        aiohttpretty.register_json_uri('PUT', new_file_upload_url, body=file_rename_sub_response,
                                       status=201)

        file_metadata, created = await provider.upload(file_stream, intended_path,
                                                       conflict='rename')

        assert aiohttpretty.has_call(method='GET', uri=intended_metadata_url)
        assert aiohttpretty.has_call(method='GET', uri=base_metadata_url)
        assert aiohttpretty.has_call(method='PUT', uri=new_file_upload_url)
        assert created is True

        assert actual_path.identifier == file_metadata.extra['id']
        assert actual_path.name == file_metadata.name

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_chunked_upload(self, monkeypatch, provider, file_stream, readwrite_fixtures):
        file_root_response = readwrite_fixtures['file_root_response']
        create_upload_session_response = readwrite_fixtures['create_upload_session_response']
        file_id = file_root_response['id']

        parent_path = OneDrivePath('/', _ids=['root'])
        upload_path = OneDrivePath('/elect-a.jpg', _ids=['root'])
        fragment_size = int(file_stream.size / 2 + 1)
        monkeypatch.setattr(
            'waterbutler.providers.onedrive.settings.ONEDRIVE_CHUNKED_UPLOAD_FILE_SIZE',
            file_stream.size - 1
        )
        monkeypatch.setattr(
            'waterbutler.providers.onedrive.settings.ONEDRIVE_CHUNKED_UPLOAD_CHUNK_SIZE',
            fragment_size
        )

        chunk_upload_mock = utils.MockCoroutine()
        chunk_upload_mock.return_value = (None, file_root_response)
        monkeypatch.setattr(provider, '_chunked_upload_stream_by_range', chunk_upload_mock)

        create_session_url = provider._build_graph_item_url('{}:'.format(parent_path.identifier),
                                                            '{}:'.format(upload_path.name),
                                                            'createUploadSession')
        aiohttpretty.register_json_uri('POST', create_session_url,
                                       body=create_upload_session_response)

        received, created = await provider.upload(file_stream, upload_path)

        assert created is True
        assert aiohttpretty.has_call(method='POST', uri=create_session_url)
        assert chunk_upload_mock.call_count == 2

        expected = OneDriveFileMetadata(file_root_response, upload_path)
        assert expected == received

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_chunked_upload_failed(self, monkeypatch, provider, file_stream, readwrite_fixtures):
        file_root_response = readwrite_fixtures['file_root_response']
        create_upload_session_response = readwrite_fixtures['create_upload_session_response']
        file_id = file_root_response['id']

        parent_path = OneDrivePath('/', _ids=['root'])
        upload_path = OneDrivePath('/elect-a.jpg', _ids=['root'])
        fragment_size = int(file_stream.size / 2 + 1)
        monkeypatch.setattr(
            'waterbutler.providers.onedrive.settings.ONEDRIVE_CHUNKED_UPLOAD_FILE_SIZE',
            file_stream.size - 1
        )
        monkeypatch.setattr(
            'waterbutler.providers.onedrive.settings.ONEDRIVE_CHUNKED_UPLOAD_CHUNK_SIZE',
            fragment_size
        )

        create_session_url = provider._build_graph_item_url('{}:'.format(parent_path.identifier),
                                                            '{}:'.format(upload_path.name),
                                                            'createUploadSession')
        aiohttpretty.register_json_uri('POST', create_session_url,
                                       body=create_upload_session_response)
        aiohttpretty.register_json_uri('DELETE', create_upload_session_response['uploadUrl'],
                                       status=204)

        chunk_upload_mock = utils.MockCoroutine()
        chunk_upload_mock.return_value = (["2-5", ], None)
        monkeypatch.setattr(provider, '_chunked_upload_stream_by_range', chunk_upload_mock)

        with pytest.raises(exceptions.UploadError) as e:
            await provider.upload(file_stream, upload_path)

        assert e.value.code == 400
        assert chunk_upload_mock.call_count == 3
        assert aiohttpretty.has_call(method='POST', uri=create_session_url)
        assert aiohttpretty.has_call(method='DELETE',
                                     uri=create_upload_session_response['uploadUrl'])


class TestDelete:

    @pytest.mark.asyncio
    async def test_delete_not_found(self, provider):
        path = OneDrivePath('/not-exist.jpg', _ids=['root'])
        with pytest.raises(exceptions.NotFoundError) as e:
            await provider.delete(path)

        assert e.value.code == 404
        assert e.value.message == 'Could not retrieve file or directory {}'.format(path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_root(self, provider, readwrite_fixtures):
        path = OneDrivePath('/', _ids=['root'])

        root_metadata_url = provider._build_graph_item_url('root', expand='children')
        aiohttpretty.register_json_uri('GET', root_metadata_url,
                                       body=readwrite_fixtures['root_delete_root_metadata'],
                                       status=200)

        root_delete_url = provider._build_graph_item_url('root')
        aiohttpretty.register_json_uri('DELETE', root_delete_url, status=204)

        child1_url = provider._build_graph_item_url('F4D50E400DFE7D4E!134')
        aiohttpretty.register_json_uri('GET', child1_url, status=200,
                                       body=readwrite_fixtures['root_delete_folder_metadata'])
        aiohttpretty.register_json_uri('DELETE', child1_url, status=204)

        child2_url = provider._build_graph_item_url('F4D50E400DFE7D4E!104')
        aiohttpretty.register_json_uri('GET', child2_url, status=200,
                                       body=readwrite_fixtures['root_delete_file_metadata'])
        aiohttpretty.register_json_uri('DELETE', child2_url, status=204)

        await provider.delete(path, confirm_delete=1)

        assert aiohttpretty.has_call(method='GET', uri=root_metadata_url)
        assert not aiohttpretty.has_call(method='DELETE', uri=root_delete_url)
        assert aiohttpretty.has_call(method='DELETE', uri=child1_url)
        assert aiohttpretty.has_call(method='DELETE', uri=child2_url)

    @pytest.mark.asyncio
    async def test_delete_root_not_confirmed(self, provider):
        path = OneDrivePath('/', _ids=['root'])
        with pytest.raises(exceptions.DeleteError) as e:
            await provider.delete(path)

        assert e.value.code == 400
        assert e.value.message == 'confirm_delete=1 is required for deleting root provider folder'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_file(self, provider):
        path = OneDrivePath('/delete-this-file', _ids=['root', '123!456'])

        delete_url = provider._build_graph_item_url('123!456')
        aiohttpretty.register_json_uri('DELETE', delete_url, status=204)

        await provider.delete(path)
        assert aiohttpretty.has_call(method='DELETE', uri=delete_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_folder(self, provider):
        path = OneDrivePath('/delete-this-folder/', _ids=['root', '123!456'])

        delete_url = provider._build_graph_item_url('123!456')
        aiohttpretty.register_json_uri('DELETE', delete_url, status=204)

        await provider.delete(path)
        assert aiohttpretty.has_call(method='DELETE', uri=delete_url)


class TestOperations:

    def test_can_duplicate_names(self, provider):
        assert provider.can_duplicate_names() is False

    def test_shares_storage_root(self, provider, other_provider):
        assert provider.shares_storage_root(other_provider) is False
        assert provider.shares_storage_root(provider) is True

    def test_can_intra_move(self, provider, other_provider):
        assert provider.can_intra_move(other_provider) is False
        assert provider.can_intra_move(provider) is True

    def test_can_intra_copy(self, provider, other_provider):
        assert provider.can_intra_copy(other_provider) is False
        assert provider.can_intra_copy(provider) is True


# class TestIntraMove:

#     @pytest.mark.asyncio
#     @pytest.mark.aiohttpretty
#     async def test_rename_file_root_clean(self, provider, readwrite_fixtures):
#         src_path = OneDrivePath('/meow-mix.bin', _ids=['root', '123'])
#         dest_path = OneDrivePath('/woof-mix.bin', _ids=['root', ])

#         upload_params = {'name': 'woof-mix.bin', 'parentReference': {'path': '/drive/root:/'}}
#         update_url = provider._build_graph_item_url(src_path.identifier)
#         aiohttpretty.register_json_uri('PATCH', update_url,
#                                        params=upload_params,
#                                        body=file_rename_sub_response)
#         metadata, created = await provider.intra_move(provider, src_path, dest_path)

#         assert aiohttpretty.has_call(method='PATCH', uri=update_url)
#         assert created is True
#         assert metadata.materialized_path == dest_path.materialized_path

#     @pytest.mark.asyncio
#     @pytest.mark.aiohttpretty
#     async def test_rename_file_root_overwrite(self, provider, readwrite_fixtures):
#         file_sub_response = readwrite_fixtures['file_sub_response']
#         file_rename_sub_response = readwrite_fixtures['file_rename_sub_response']

#         src_path = OneDrivePath('/meow-mix.bin', _ids=['root', '123'])
#         dest_path = OneDrivePath('/woof-mix.bin', _ids=['root', ])
#         assert dest_path.identifier is None

#         upload_params = {'name': 'meow-mix.bin', 'parentReference': {'id': '456'}}
#         update_url = provider._build_graph_item_url(src_path.identifier)
#         aiohttpretty.register_json_uri('PATCH', update_url,
#                                        params=upload_params,
#                                        body=file_rename_sub_response)
#         metadata, created = await provider.intra_move(provider, src_path, dest_path)

#         assert aiohttpretty.has_call(method='PATCH', uri=update_url)
#         assert created is True
#         assert metadata.materialized_path == dest_path.materialized_path
#         assert metadata._path_obj.parent == dest_path.parent

#     @pytest.mark.asyncio
#     @pytest.mark.aiohttpretty
#     async def test_rename_file_subfolder_clean(self, provider, readwrite_fixtures):
#         file_sub_response = readwrite_fixtures['file_sub_response']
#         file_rename_sub_response = readwrite_fixtures['file_rename_sub_response']

#         src_path = OneDrivePath('/meow-mix.bin', _ids=['root', '123'])
#         dest_path = OneDrivePath('/woof-mix.bin', _ids=['root', ])
#         assert dest_path.identifier is None

#         upload_params = {'name': 'meow-mix.bin', 'parentReference': {'id': '456'}}
#         update_url = provider._build_graph_item_url(src_path.identifier)
#         aiohttpretty.register_json_uri('PATCH', update_url,
#                                        params=upload_params,
#                                        body=file_rename_sub_response)
#         metadata, created = await provider.intra_move(provider, src_path, dest_path)

#         assert aiohttpretty.has_call(method='PATCH', uri=update_url)
#         assert created is True
#         assert metadata.materialized_path == dest_path.materialized_path
#         assert metadata._path_obj.parent == dest_path.parent

#     @pytest.mark.asyncio
#     @pytest.mark.aiohttpretty
#     async def test_rename_file_subfolder_overwrite(self, provider, readwrite_fixtures):
#         file_sub_response = readwrite_fixtures['file_sub_response']
#         file_rename_sub_response = readwrite_fixtures['file_rename_sub_response']

#         src_path = OneDrivePath('/meow-mix.bin', _ids=['root', '123'])
#         dest_path = OneDrivePath('/woof-mix.bin', _ids=['root', ])
#         assert dest_path.identifier is None

#         upload_params = {'name': 'meow-mix.bin', 'parentReference': {'id': '456'}}
#         update_url = provider._build_graph_item_url(src_path.identifier)
#         aiohttpretty.register_json_uri('PATCH', update_url,
#                                        params=upload_params,
#                                        body=file_rename_sub_response)
#         metadata, created = await provider.intra_move(provider, src_path, dest_path)

#         assert aiohttpretty.has_call(method='PATCH', uri=update_url)
#         assert created is True
#         assert metadata.materialized_path == dest_path.materialized_path
#         assert metadata._path_obj.parent == dest_path.parent

#     @pytest.mark.asyncio
#     @pytest.mark.aiohttpretty
#     async def test_rename_file_root_subfolder_clean(self, provider, readwrite_fixtures):
#         file_sub_response = readwrite_fixtures['file_sub_response']
#         file_rename_sub_response = readwrite_fixtures['file_rename_sub_response']

#         src_path = OneDrivePath('/meow-mix.bin', _ids=['root', '123'])
#         dest_path = OneDrivePath('/woof-mix.bin', _ids=['root', ])
#         assert dest_path.identifier is None

#         upload_params = {'name': 'meow-mix.bin', 'parentReference': {'id': '456'}}
#         update_url = provider._build_graph_item_url(src_path.identifier)
#         aiohttpretty.register_json_uri('PATCH', update_url,
#                                        params=upload_params,
#                                        body=file_rename_sub_response)
#         metadata, created = await provider.intra_move(provider, src_path, dest_path)

#         assert aiohttpretty.has_call(method='PATCH', uri=update_url)
#         assert created is True
#         assert metadata.materialized_path == dest_path.materialized_path
#         assert metadata._path_obj.parent == dest_path.parent

#     @pytest.mark.asyncio
#     @pytest.mark.aiohttpretty
#     async def test_rename_file_root_subfolder_overwrite(self, provider, readwrite_fixtures):
#         file_sub_response = readwrite_fixtures['file_sub_response']
#         file_rename_sub_response = readwrite_fixtures['file_rename_sub_response']

#         src_path = OneDrivePath('/meow-mix.bin', _ids=['root', '123'])
#         dest_path = OneDrivePath('/woof-mix.bin', _ids=['root', ])
#         assert dest_path.identifier is None

#         upload_params = {'name': 'meow-mix.bin', 'parentReference': {'id': '456'}}
#         update_url = provider._build_graph_item_url(src_path.identifier)
#         aiohttpretty.register_json_uri('PATCH', update_url,
#                                        params=upload_params,
#                                        body=file_rename_sub_response)
#         metadata, created = await provider.intra_move(provider, src_path, dest_path)

#         assert aiohttpretty.has_call(method='PATCH', uri=update_url)
#         assert created is True
#         assert metadata.materialized_path == dest_path.materialized_path
#         assert metadata._path_obj.parent == dest_path.parent

#     @pytest.mark.asyncio
#     @pytest.mark.aiohttpretty
#     async def test_move_file(self, provider, readwrite_fixtures):
#         file_sub_response = readwrite_fixtures['file_sub_response']
#         file_rename_sub_response = readwrite_fixtures['file_rename_sub_response']
#         # src_path = OneDrivePath.new_from_response(file_sub_response, '75BFE374EBEB1211!107')
#         # dest_path = src_path.parent.child(file_rename_sub_response['name'])

#         src_path = OneDrivePath('/meow-mix.bin', _ids=['root', '123'])
#         dest_path = OneDrivePath('/target/meow-mix.bin', _ids=['root', '456'])
#         assert dest_path.identifier is None

#         upload_params = {'name': 'meow-mix.bin', 'parentReference': {'id': '456'}}
#         update_url = provider._build_graph_item_url(src_path.identifier)
#         aiohttpretty.register_json_uri('PATCH', update_url,
#                                        params=upload_params,
#                                        body=file_rename_sub_response)
#         metadata, created = await provider.intra_move(provider, src_path, dest_path)

#         assert aiohttpretty.has_call(method='PATCH', uri=update_url)
#         assert created is True
#         assert metadata.materialized_path == dest_path.materialized_path
#         assert metadata._path_obj.parent == dest_path.parent

#     @pytest.mark.asyncio
#     @pytest.mark.aiohttpretty
#     async def test_move_file_exists(self, provider, readwrite_fixtures):
#         file_sub_response = readwrite_fixtures['file_sub_response']
#         file_root_response = readwrite_fixtures['file_root_response']
#         src_path = OneDrivePath.new_from_response(file_sub_response, '75BFE374EBEB1211!107')
#         dest_path = OneDrivePath.new_from_response(file_root_response, '75BFE374EBEB1211!107')

#         update_url = provider._build_graph_item_url(src_path.identifier)
#         aiohttpretty.register_json_uri('PATCH', update_url, body=file_root_response)
#         delete_url = provider._build_graph_item_url(dest_path.identifier)
#         aiohttpretty.register_json_uri('DELETE', delete_url, status=204)

#         metadata, created = await provider.intra_move(provider, src_path, dest_path)

#         assert aiohttpretty.has_call(method='PATCH', uri=update_url)
#         assert aiohttpretty.has_call(method='DELETE', uri=delete_url)
#         assert created is False

#         expected_path = OneDrivePath.new_from_response(file_root_response)
#         assert metadata._path == expected_path


# class TestIntraCopy:
#     @pytest.mark.asyncio
#     @pytest.mark.aiohttpretty
#     async def test_copy_directory(self, provider, readwrite_fixtures):
#         folder_sub_response = readwrite_fixtures['folder_sub_response']
#         src_path = OneDrivePath.new_from_response(folder_sub_response, readwrite_fixtures['root_folder_id'])
#         dest_response = folder_sub_response.copy()
#         dest_response['name'] = 'elect-y.jpg'
#         dest_path = src_path.parent.child(dest_response['name'])

#         status_url = 'http://any.url'
#         copy_url = provider._build_content_url(src_path.identifier, 'action.copy')
#         aiohttpretty.register_uri('POST', copy_url, headers={
#             'LOCATION': status_url
#         }, status=202)
#         aiohttpretty.register_json_uri('GET', status_url, body=dest_response, status=200)

#         metadata, created = await provider.intra_copy(provider, src_path, dest_path)

#         assert aiohttpretty.has_call(method='POST', uri=copy_url)
#         assert aiohttpretty.has_call(method='GET', uri=status_url)

#         assert created is True
#         assert metadata._path.name == dest_path.name

#     @pytest.mark.asyncio
#     @pytest.mark.aiohttpretty
#     async def test_copy_timeout(self, monkeypatch, provider, readwrite_fixtures):
#         monkeypatch.setattr('waterbutler.providers.onedrive.settings.ONEDRIVE_COPY_REQUEST_TIMEOUT', 3)

#         folder_sub_sub_response = readwrite_fixtures['folder_sub_sub_response']

#         src_path = OneDrivePath.new_from_response(folder_sub_sub_response, readwrite_fixtures['root_folder_id'])
#         dest_path = src_path.parent.child('new_big_folder/', folder=True)

#         status_url = 'http://any.url'
#         copy_url = provider._build_content_url(src_path.identifier, 'action.copy')
#         aiohttpretty.register_uri('POST', copy_url, headers={'LOCATION': status_url}, status=202)
#         aiohttpretty.register_uri('GET', status_url, status=202)

#         with pytest.raises(exceptions.CopyError) as e:
#             await provider.intra_copy(provider, src_path, dest_path)

#         assert e.value.code == 202
#         assert e.value.message.startswith("OneDrive API file copy has not responded in a timely manner")
