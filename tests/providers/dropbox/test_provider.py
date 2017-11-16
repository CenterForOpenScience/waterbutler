import pytest

import io
import json
from http import client

import aiohttpretty

from waterbutler.core import streams
from waterbutler.core import metadata
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.dropbox import DropboxProvider
from waterbutler.providers.dropbox.metadata import (DropboxFileMetadata,
                                                    DropboxRevision,
                                                    DropboxFolderMetadata)
from waterbutler.providers.dropbox.exceptions import (DropboxNamingConflictError,
                                                      DropboxUnhandledConflictError)

from tests.providers.dropbox.fixtures import(root_provider_fixtures,
                                             revision_fixtures,
                                             intra_copy_fixtures,
                                             error_fixtures)


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
    return {'token': 'did not write harry potter'}


@pytest.fixture
def settings():
    return {'folder': '/Photos'}


@pytest.fixture
def provider(auth, credentials, settings):
    return DropboxProvider(auth, credentials, settings)


@pytest.fixture
def other_provider(auth, other_credentials, settings):
    return DropboxProvider(auth, other_credentials, settings)


# file stream fixtures

@pytest.fixture
def file_content():
    return b'SLEEP IS FOR THE WEAK GO SERVE STREAMS'


@pytest.fixture
def file_like(file_content):
    return io.BytesIO(file_content)


@pytest.fixture
def file_stream(file_like):
    return streams.FileStreamReader(file_like)


def build_folder_metadata_data(path):
    return {'path': path.full_path}


class TestValidatePath:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('settings', [{'folder': '/'}])
    async def test_validate_v1_path_file(self, provider, root_provider_fixtures):
        file_path = '/Photos/Getting_Started.pdf'
        data = {"path": file_path}

        metadata_url = provider.build_url('files', 'get_metadata')
        aiohttpretty.register_json_uri('POST', metadata_url, data=data,
                                       body=root_provider_fixtures['file_metadata'])

        try:
            wb_path_v1 = await provider.validate_v1_path(file_path)
        except Exception as exc:
            pytest.fail(str(exc))

        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider.validate_v1_path(file_path + '/')

        assert exc.value.code == client.NOT_FOUND

        wb_path_v0 = await provider.validate_path(file_path)

        assert wb_path_v1 == wb_path_v0

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('settings', [{'folder': '/'}])
    async def test_validate_v1_path_folder(self, provider, root_provider_fixtures):
        folder_path = '/Photos'
        data = {"path": folder_path}

        metadata_url = provider.build_url('files', 'get_metadata')
        aiohttpretty.register_json_uri('POST', metadata_url, data=data,
                                       body=root_provider_fixtures['folder_metadata'])

        try:
            wb_path_v1 = await provider.validate_v1_path(folder_path + '/')
        except Exception as exc:
            pytest.fail(str(exc))

        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider.validate_v1_path(folder_path)

        assert exc.value.code == client.NOT_FOUND

        wb_path_v0 = await provider.validate_path(folder_path + '/')

        assert wb_path_v1 == wb_path_v0

    @pytest.mark.asyncio
    async def test_returns_path_obj(self, provider):
        path = await provider.validate_path('/thisisapath')

        assert path.is_file
        assert len(path.parts) > 1
        assert path.name == 'thisisapath'
        assert provider.folder in path.full_path

    @pytest.mark.asyncio
    async def test_with_folder(self, provider):
        path = await provider.validate_path('/this/isa/folder/')

        assert path.is_dir
        assert len(path.parts) > 1
        assert path.name == 'folder'
        assert provider.folder in path.full_path

    @pytest.mark.asyncio
    async def test_validate_v1_path_base(self, provider):
        path = await provider.validate_v1_path('/')

        assert path.is_dir
        assert len(path.parts) == 1
        assert path.name == ''
        assert provider.folder in path.full_path


class TestCRUD:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download(self, provider):
        path = WaterButlerPath('/triangles.txt', prepend=provider.folder)
        url = provider._build_content_url('files', 'download')
        aiohttpretty.register_uri('POST', url, body=b'better', auto_length=True)
        result = await provider.download(path)
        content = await result.response.read()

        assert content == b'better'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_not_found(self, provider, error_fixtures):
        path = await provider.validate_path('/vectors.txt')
        url = provider._build_content_url('files', 'download')
        aiohttpretty.register_json_uri('POST', url, status=409,
                                       body=error_fixtures['not_found_metadata_data'])

        with pytest.raises(exceptions.NotFoundError) as e:
            await provider.download(path)

        assert e.value.code == 404

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload(self, provider, root_provider_fixtures,
                          error_fixtures, file_stream, settings):
        path = await provider.validate_path('/phile')

        metadata_url = provider.build_url('files', 'get_metadata')
        data = {'path': path.full_path}
        url = provider._build_content_url('files', 'upload')

        aiohttpretty.register_json_uri('POST', metadata_url, data=data,
                                       status=409, body=error_fixtures['not_found_metadata_data'])
        aiohttpretty.register_json_uri('POST', url, status=200,
                                       body=root_provider_fixtures['file_metadata'])

        metadata, created = await provider.upload(file_stream, path)
        expected = DropboxFileMetadata(root_provider_fixtures['file_metadata'], provider.folder)

        assert created is True
        assert metadata == expected
        assert aiohttpretty.has_call(method='POST', uri=url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_file(self, provider):
        url = provider.build_url('files', 'delete')
        path = await provider.validate_path('/The past')
        data = {'path': path.full_path}

        aiohttpretty.register_json_uri('POST', url, data=data, status=200)

        await provider.delete(path)

        assert aiohttpretty.has_call(method='POST', uri=url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_root_bad(self, provider):
        url = provider.build_url('files', 'delete')
        path = await provider.validate_path('/')
        data = {'path': path.full_path}

        aiohttpretty.register_json_uri('POST', url, data=data, status=200)

        with pytest.raises(exceptions.DeleteError) as e:
            await provider.delete(path)
        assert e.value.code == 400
        assert e.value.message == 'confirm_delete=1 is required for deleting root provider folder'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_root(self, provider, root_provider_fixtures):
        url = provider.build_url('files', 'list_folder')
        path = await provider.validate_path('/')
        data = {'path': path.full_path}
        aiohttpretty.register_json_uri('POST', url, data=data,
                                       body=root_provider_fixtures['folder_children'], status=200)
        path2 = await provider.validate_path('/photos/flower.jpg')
        url = provider.build_url('files', 'delete')
        data = {'path': provider.folder.rstrip('/') + '/' + path2.path.rstrip('/')}
        aiohttpretty.register_json_uri('POST', url, data=data, status=200)

        await provider.delete(path, 1)

        assert aiohttpretty.has_call(method='POST', uri=url)


class TestMetadata:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata(self, provider, root_provider_fixtures):
        path = await provider.validate_path('/')
        url = provider.build_url('files', 'list_folder')
        data = {'path': path.full_path}
        aiohttpretty.register_json_uri('POST', url, data=data,
                                       body=root_provider_fixtures['folder_children'])
        result = await provider.metadata(path)

        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0].kind == 'file'
        assert result[0].name == 'flower.jpg'
        assert result[0].path == '/flower.jpg'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_revision_metadata(self, provider, revision_fixtures):
        path = await provider.validate_path('/testfile')
        url = provider.build_url('files', 'get_metadata')
        revision = 'c5bb27d11'
        data = {'path': 'rev:' + revision}
        aiohttpretty.register_json_uri('POST', url, data=data,
                                       body=revision_fixtures['single_file_revision_metadata'])
        result = await provider.metadata(path, revision)
        expected = DropboxFileMetadata(revision_fixtures['single_file_revision_metadata'],
                                       provider.folder)

        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_folder_with_subdirectory_metadata(self, provider, root_provider_fixtures):
        path = await provider.validate_path('/')
        url = provider.build_url('files', 'list_folder')
        data = {'path': path.full_path}
        aiohttpretty.register_json_uri('POST', url, data=data,
                                       body=root_provider_fixtures['folder_with_subdirectory_metadata'])
        result = await provider.metadata(path)

        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0].kind == 'folder'
        assert result[0].name == 'randomfolder'
        assert result[0].path == '/conflict folder/randomfolder/'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_folder_with_hasmore_metadata(self, provider, root_provider_fixtures):
        path = await provider.validate_path('/')
        url = provider.build_url('files', 'list_folder')
        data = {'path': path.full_path}
        aiohttpretty.register_json_uri('POST', url, data=data,
                                       body=root_provider_fixtures['folder_with_hasmore_metadata'])
        aiohttpretty.register_json_uri('POST', url + '/continue', data=data,
                                       body=root_provider_fixtures['folder_with_subdirectory_metadata'])

        result = await provider.metadata(path)

        assert isinstance(result, list)
        assert len(result) == 4
        assert result[0].kind == 'folder'
        assert result[0].name == 'randomfolder'
        assert result[0].path == '/conflict folder/randomfolder/'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_get_revisions(self, provider, revision_fixtures):
        path = WaterButlerPath('/pfile', prepend=provider.folder)
        url = provider.build_url('files', 'list_revisions')
        data = {'path': path.full_path.rstrip('/'), 'limit': 100}
        aiohttpretty.register_json_uri('POST', url, data=data,
                                        body=revision_fixtures['file_revision_metadata'])

        result = await provider.revisions(path)
        expected = [
            DropboxRevision(item)
            for item in revision_fixtures['file_revision_metadata']['entries']
        ]

        assert result == expected
        assert len(result) == 3

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_deleted_revision_metadata(self, provider, revision_fixtures):
        path = WaterButlerPath('/pfile', prepend=provider.folder)
        url = provider.build_url('files', 'list_revisions')
        data = {'path': path.full_path.rstrip('/'), 'limit': 100}
        aiohttpretty.register_json_uri('POST', url, data=data,
                                        body=revision_fixtures['deleted_file_revision_metadata'])

        with pytest.raises(exceptions.RevisionsError) as e:
            result = await provider.revisions(path)

        assert e.value.code == 404
        assert e.value.message == "Could not retrieve '/pfile'"

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_root_file(self, provider, root_provider_fixtures):
        path = WaterButlerPath('/pfile', prepend=provider.folder)
        url = provider.build_url('files', 'get_metadata')
        data = {'path': path.full_path}
        aiohttpretty.register_json_uri('POST', url, data=data,
                                       body=root_provider_fixtures['file_metadata'])
        result = await provider.metadata(path)

        assert isinstance(result, metadata.BaseMetadata)
        assert result.kind == 'file'
        assert result.name == 'Getting_Started.pdf'
        assert result.path == '/Getting_Started.pdf'
        assert result.extra == {
            'revisionId': '2ba1017a0c1e',
            'id': 'id:8y8sAJlrhuAAAAAAAAAAAQ',
            'hashes': {
                'dropbox': 'meow'
            },
        }


    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_deleted_file_metadata(self, provider, error_fixtures):
        path = WaterButlerPath('/pfile', prepend=provider.folder)
        url = provider.build_url('files', 'get_metadata')
        data = {'path': path.full_path}
        aiohttpretty.register_json_uri('POST', url, data=data,
                                       body=error_fixtures['deleted_file_metadata'])

        with pytest.raises(exceptions.MetadataError) as e:
            await provider.metadata(path)
        assert e.value.code == 404

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_file_metadata_folder_tag(self, provider, error_fixtures):
        path = WaterButlerPath('/pfile', prepend=provider.folder)
        url = provider.build_url('files', 'get_metadata')
        data = {'path': path.full_path}
        aiohttpretty.register_json_uri('POST', url, data=data,
                                       body=error_fixtures['file_metadata_folder_tag'])

        with pytest.raises(exceptions.MetadataError) as e:
            await provider.metadata(path)
        assert e.value.code == 404

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_missing(self, provider, error_fixtures):
        path = WaterButlerPath('/pfile', prepend=provider.folder)
        url = provider.build_url('files', 'get_metadata')
        data = {"path": "/pfile"}
        aiohttpretty.register_json_uri('POST', url, data=data, status=409,
                                       body=error_fixtures['not_found_metadata_data'])

        with pytest.raises(exceptions.NotFoundError):
            await provider.metadata(path)


class TestCreateFolder:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_already_exists(self, provider):
        path = WaterButlerPath('/newfolder/', prepend=provider.folder)
        url = provider.build_url('files', 'create_folder')
        data = build_folder_metadata_data(path)
        body = {
            "error_summary": "path/conflict/folder/...",
            "error": {
                ".tag": "path",
                "path": {
                    ".tag": "conflict",
                    "conflict": {".tag": "folder"}
                }
            }
        }
        aiohttpretty.register_json_uri('POST', url, data=data, status=409,
                                       body=body)

        with pytest.raises(DropboxNamingConflictError) as e:
            await provider.create_folder(path)

        assert e.value.code == 409
        assert e.value.message == ('Cannot complete action: file or folder already exists at '
                                   '/newfolder')

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_already_exists_unhandled_conflict(self, provider, root_provider_fixtures):
        # This test is just to hit the last line of dropbox_conflict_error_handler and not much else
        path = WaterButlerPath('/newfolder/', prepend=provider.folder)
        url = provider.build_url('files', 'create_folder')
        data = build_folder_metadata_data(path)

        aiohttpretty.register_json_uri('POST', url, data=data, status=409,
                                       body=root_provider_fixtures['folder_metadata'])

        with pytest.raises(DropboxUnhandledConflictError) as e:
            await provider.create_folder(path)

        assert e.value.code == 409

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_forbidden(self, provider):
        path = WaterButlerPath('/newfolder/', prepend=provider.folder)
        url = provider.build_url('files', 'create_folder')
        data = build_folder_metadata_data(path)

        aiohttpretty.register_json_uri('POST', url, data=data, status=403,
                                       body={'error': 'because I hate you'})

        with pytest.raises(exceptions.CreateFolderError) as e:
            await provider.create_folder(path)

        assert e.value.code == 403
        assert e.value.data['error'] == 'because I hate you'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_raises_on_errors(self, provider):
        path = WaterButlerPath('/newfolder/', prepend=provider.folder)
        url = provider.build_url('files', 'create_folder')
        data = build_folder_metadata_data(path)

        aiohttpretty.register_json_uri('POST', url, data=data, status=418, body={})

        with pytest.raises(exceptions.CreateFolderError) as e:
            await provider.create_folder(path)

        assert e.value.code == 418

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_returns_metadata(self, provider, root_provider_fixtures):
        path = WaterButlerPath('/newfolder/', prepend=provider.folder)
        url = provider.build_url('files', 'create_folder')
        data = build_folder_metadata_data(path)

        aiohttpretty.register_json_uri('POST', url, data=data, status=200,
                                       body=root_provider_fixtures['folder_metadata'])

        resp = await provider.create_folder(path)

        assert resp.kind == 'folder'
        assert resp.name == 'newfolder'
        assert resp.path == '/newfolder/'


class TestIntra:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_copy_file(self, provider, root_provider_fixtures):
        src_path = WaterButlerPath('/pfile', prepend=provider.folder)
        dest_path = WaterButlerPath('/pfile_renamed', prepend=provider.folder)

        url = provider.build_url('files', 'copy')
        data = {'from_path': src_path.full_path.rstrip('/'),
                'to_path': dest_path.full_path.rstrip('/')},
        aiohttpretty.register_json_uri('POST', url, data=data,
                                       body=root_provider_fixtures['file_metadata'])

        result = await provider.intra_copy(provider, src_path, dest_path)
        expected = (DropboxFileMetadata(root_provider_fixtures['file_metadata'], provider.folder),
                    True)

        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_copy_replace_file(self, provider, root_provider_fixtures, error_fixtures):
        url = provider.build_url('files', 'delete')
        path = await provider.validate_path('/The past')
        data = {'path': path.full_path}
        aiohttpretty.register_json_uri('POST', url, data=data, status=200)

        src_path = WaterButlerPath('/pfile', prepend=provider.folder)
        dest_path = WaterButlerPath('/pfile_renamed', prepend=provider.folder)

        url = provider.build_url('files', 'copy')
        data = {'from_path': src_path.full_path.rstrip('/'),
                'to_path': dest_path.full_path.rstrip('/')}
        aiohttpretty.register_json_uri('POST', url, **{
            "responses": [
                {
                    'headers': {'Content-Type': 'application/json'},
                    'data': data,
                    'body': json.dumps(error_fixtures['rename_conflict_folder_metadata']).encode('utf-8'),
                    'status': 409
                },
                {
                    'headers': {'Content-Type': 'application/json'},
                    'data': data,
                    'body': json.dumps(root_provider_fixtures['file_metadata']).encode('utf-8')
                },
            ]})

        result = await provider.intra_copy(provider, src_path, dest_path)
        expected = (DropboxFileMetadata(root_provider_fixtures['file_metadata'], provider.folder),
                    False)

        assert expected == result

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_copy_file_different_provider(self, provider, other_provider,
                                                      intra_copy_fixtures):
        src_path = WaterButlerPath('/pfile', prepend=provider.folder)
        dest_path = WaterButlerPath('/pfile_renamed', prepend=other_provider.folder)

        url = provider.build_url('files', 'copy_reference', 'get')
        data = {'path': src_path.full_path.rstrip('/')},
        aiohttpretty.register_json_uri('POST', url, data=data,
                                       body=intra_copy_fixtures['intra_copy_file_metadata'])

        url1 = provider.build_url('files', 'copy_reference', 'save')
        data1 = {'copy_reference': 'test', 'path': dest_path.full_path.rstrip('/')}
        aiohttpretty.register_json_uri('POST', url1, data=data1,
                                       body=intra_copy_fixtures['intra_copy_other_provider_file_metadata'])

        result = await provider.intra_copy(other_provider, src_path, dest_path)
        expected = (DropboxFileMetadata(
            intra_copy_fixtures['intra_copy_other_provider_file_metadata']['metadata'],
            provider.folder
        ), True)

        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_copy_folder(self, provider, root_provider_fixtures):
        src_path = WaterButlerPath('/pfile/', prepend=provider.folder)
        dest_path = WaterButlerPath('/pfile_renamed/', prepend=provider.folder)

        url = provider.build_url('files', 'copy')
        data = {'from_path': src_path.full_path.rstrip('/'),
                'to_path': dest_path.full_path.rstrip('/')}
        aiohttpretty.register_json_uri('POST', url, data=data,
                                       body=root_provider_fixtures['folder_metadata'])

        url = provider.build_url('files', 'list_folder')
        data = {'path': dest_path.full_path}
        aiohttpretty.register_json_uri('POST', url, data=data,
                                       body=root_provider_fixtures['folder_children'], status=200)

        result = await provider.intra_copy(provider, src_path, dest_path)
        expected = DropboxFolderMetadata(root_provider_fixtures['folder_metadata'], provider.folder)
        expected.children = [
            DropboxFileMetadata(item, provider.folder)
            for item in root_provider_fixtures['folder_children']['entries']
        ]

        assert expected == result[0]

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_move_file(self, provider, root_provider_fixtures):
        src_path = WaterButlerPath('/pfile', prepend=provider.folder)
        dest_path = WaterButlerPath('/pfile_renamed', prepend=provider.folder)

        url = provider.build_url('files', 'move')
        data = {'from_path': src_path.full_path.rstrip('/'),
                'to_path': dest_path.full_path.rstrip('/')}
        aiohttpretty.register_json_uri('POST', url, data=data,
                                       body=root_provider_fixtures['file_metadata'])

        result = await provider.intra_move(provider, src_path, dest_path)
        expected = (DropboxFileMetadata(root_provider_fixtures['file_metadata'], provider.folder),
                    True)

        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_move_replace_file(self, provider, root_provider_fixtures, error_fixtures):
        url = provider.build_url('files', 'delete')
        path = await provider.validate_path('/The past')
        data = {'path': path.full_path}
        aiohttpretty.register_json_uri('POST', url, data=data, status=200)

        src_path = WaterButlerPath('/pfile', prepend=provider.folder)
        dest_path = WaterButlerPath('/pfile_renamed', prepend=provider.folder)

        url = provider.build_url('files', 'move')
        data = {'from_path': src_path.full_path.rstrip('/'),
                'to_path': dest_path.full_path.rstrip('/')},
        aiohttpretty.register_json_uri('POST', url, **{
            "responses": [
                {
                    'headers': {'Content-Type': 'application/json'},
                    'data': data,
                    'body': json.dumps(error_fixtures['rename_conflict_file_metadata']).encode('utf-8'),
                    'status': 409
                },
                {
                    'headers': {'Content-Type': 'application/json'},
                    'data': data,
                    'body': json.dumps(root_provider_fixtures['file_metadata']).encode('utf-8')
                },
            ]})

        result = await provider.intra_move(provider, src_path, dest_path)
        expected = (DropboxFileMetadata(root_provider_fixtures['file_metadata'], provider.folder),
                    False)

        assert expected == result

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_move_replace_folder(self, provider, root_provider_fixtures,
                                             error_fixtures):
        url = provider.build_url('files', 'delete')
        path = await provider.validate_path('/newfolder/')
        data = {'path': path.full_path}
        aiohttpretty.register_json_uri('POST', url, data=data, status=200)

        url = provider.build_url('files', 'list_folder')
        data = {'path': path.full_path}
        aiohttpretty.register_json_uri('POST', url, data=data,
                                       body=root_provider_fixtures['folder_children'], status=200)

        src_path = WaterButlerPath('/pfile/', prepend=provider.folder)
        dest_path = WaterButlerPath('/pfile_renamed/', prepend=provider.folder)

        url = provider.build_url('files', 'move')
        data = {'from_path': src_path.full_path.rstrip('/'),
                'to_path': dest_path.full_path.rstrip('/')}
        aiohttpretty.register_json_uri('POST', url, **{
            "responses": [
                {
                    'headers': {'Content-Type': 'application/json'},
                    'data': data,
                    'body': json.dumps(error_fixtures['rename_conflict_folder_metadata']).encode('utf-8'),
                    'status': 409
                },
                {
                    'headers': {'Content-Type': 'application/json'},
                    'data': data,
                    'body': json.dumps(root_provider_fixtures['folder_metadata']).encode('utf-8')
                },
            ]})

        result = await provider.intra_move(provider, src_path, dest_path)
        expected = DropboxFolderMetadata(root_provider_fixtures['folder_metadata'], provider.folder)
        expected.children = [
            DropboxFileMetadata(item, provider.folder)
            for item in root_provider_fixtures['folder_children']['entries']
        ]

        assert expected == result[0]

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_move_casing_change(self, provider):
        src_path = WaterButlerPath('/pfile/', prepend=provider.folder)
        dest_path = WaterButlerPath('/PFile/', prepend=provider.folder)

        with pytest.raises(exceptions.InvalidPathError) as e:
            await provider.intra_move(provider, src_path, dest_path)

        assert e.value.code == 400


class TestOperations:

    def test_can_intra_copy(self, provider):
        assert provider.can_intra_copy(provider)

    def test_can_intra_copy_other(self, provider, other_provider):
        assert provider.can_intra_copy(other_provider)

    def test_can_intra_move(self, provider):
        assert provider.can_intra_move(provider)

    def test_cannot_intra_move_other(self, provider, other_provider):
        assert provider.can_intra_move(other_provider) == False

    def test_conflict_error_handler_not_found(self, provider, error_fixtures):
        error_path = '/Photos/folder/file'
        with pytest.raises(exceptions.NotFoundError) as exc:
            provider.dropbox_conflict_error_handler(error_fixtures['not_found_metadata_data'],
                                                    error_path=error_path)
        assert str(exc.value).endswith(' /folder/file')

    def test_can_duplicate_names(self, provider):
        assert provider.can_duplicate_names() is False

    def test_shares_storage_root(self, provider, other_provider):
        assert provider.shares_storage_root(other_provider) is False
        assert provider.shares_storage_root(provider)
