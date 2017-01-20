import pytest

import io
from http import client

import aiohttpretty

from waterbutler.core import streams
from waterbutler.core import metadata
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.dropbox import DropboxProvider
from waterbutler.providers.dropbox.metadata import DropboxFileMetadata
from waterbutler.providers.dropbox.exceptions import DropboxNamingConflictError


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
def settings():
    return {'folder': '/Photos'}


@pytest.fixture
def provider(auth, credentials, settings):
    return DropboxProvider(auth, credentials, settings)


@pytest.fixture
def file_content():
    return b'SLEEP IS FOR THE WEAK GO SERVE STREAMS'


@pytest.fixture
def file_like(file_content):
    return io.BytesIO(file_content)


@pytest.fixture
def file_stream(file_like):
    return streams.FileStreamReader(file_like)


@pytest.fixture
def folder_children():
    return {"entries":
               [
                   {".tag": "file",
                    "name": "flower.jpg",
                    "path_lower": "/photos/flower.jpg",
                    "path_display": "/Photos/flower.jpg",
                    "id": "id:8y8sAJlrhuAAAAAAAAAAAQ",
                    "client_modified": "2016-06-13T19:08:17Z",
                    "server_modified": "2016-06-13T19:08:17Z",
                    "rev": "38af1b183490",
                    "size": 124778}
               ],
            "cursor": "AAGFHXqUgavlrBd2TBDxKNdV2rnu48QeThbxccGEvaSwiAAIt5-iho9P8EJIIVdSh6RKRNHq-An2lyyjJ34yCOhyBcIa6Gh6tYOko_okZgZTP_Ga0-kqHtm1HaQOQNdOmPPoNwiXB_rflzSLwq6AXi_F",
            "has_more": False
           }


@pytest.fixture
def folder_metadata():
    return {
        ".tag": "folder",
        "name": "newfolder",
        "path_lower": "/newfolder",
        "path_display": "/newfolder",
        "id": "id:67BLXqRKo-gAAAAAAAADZg"
    }


@pytest.fixture
def file_metadata():
    return {
        ".tag": "file",
        "name": "Getting_Started.pdf",
        "path_lower": "/photos/getting_started.pdf",
        "path_display": "/Photos/Getting_Started.pdf",
        "id": "id:8y8sAJlrhuAAAAAAAAAAAQ",
        "client_modified": "2016-06-13T19:08:17Z",
        "server_modified": "2016-06-13T19:08:17Z",
        "rev": "2ba1017a0c1e",
        "size": 124778
    }


def build_folder_metadata_data(path):
    return {'path': path.full_path}


@pytest.fixture
def not_found_metadata_data():
    return {"error_summary": "path/not_found/",
            "error": {".tag": "path",
                      "path": {".tag": "not_found"}
                     }
           }


class TestValidatePath:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('settings', [{'folder': '/'}])
    async def test_validate_v1_path_file(self, provider, file_metadata):
        file_path = '/Photos/Getting_Started.pdf'
        data = {"path": file_path}

        metadata_url = provider.build_url('files', 'get_metadata')
        aiohttpretty.register_json_uri('POST', metadata_url, data=data,
                                       body=file_metadata)

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
    async def test_validate_v1_path_folder(self, provider, folder_metadata):
        folder_path = '/Photos'
        data = {"path": folder_path}

        metadata_url = provider.build_url('files', 'get_metadata')
        aiohttpretty.register_json_uri('POST', metadata_url, data=data,
                                       body=folder_metadata)

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
    async def test_download_not_found(self, provider, not_found_metadata_data):
        path = await provider.validate_path('/vectors.txt')
        url = provider._build_content_url('files', 'download')
        aiohttpretty.register_json_uri('POST', url, status=409,
                                       body=not_found_metadata_data)

        with pytest.raises(exceptions.NotFoundError) as e:
            await provider.download(path)

        assert e.value.code == 404

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload(self, provider, file_metadata,
                          not_found_metadata_data, file_stream, settings):
        path = await provider.validate_path('/phile')

        metadata_url = provider.build_url('files', 'get_metadata')
        data = {'path': path.full_path}
        url = provider._build_content_url('files', 'upload')

        aiohttpretty.register_json_uri('POST', metadata_url, data=data,
                                       status=409, body=not_found_metadata_data)
        aiohttpretty.register_json_uri('POST', url, status=200,
                                       body=file_metadata)

        metadata, created = await provider.upload(file_stream, path)
        expected = DropboxFileMetadata(file_metadata, provider.folder)

        assert created is True
        assert metadata == expected
        assert aiohttpretty.has_call(method='POST', uri=url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_file(self, provider, file_metadata):
        url = provider.build_url('files', 'delete')
        path = await provider.validate_path('/The past')
        data = {'path': path.full_path}

        aiohttpretty.register_json_uri('POST', url, data=data, status=200)

        await provider.delete(path)

        assert aiohttpretty.has_call(method='POST', uri=url)


class TestMetadata:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata(self, provider, folder_children):
        path = await provider.validate_path('/')
        url = provider.build_url('files', 'list_folder')
        data = {'path': path.full_path}
        aiohttpretty.register_json_uri('POST', url, data=data,
                                       body=folder_children)
        result = await provider.metadata(path)

        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0].kind == 'file'
        assert result[0].name == 'flower.jpg'
        assert result[0].path == '/flower.jpg'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_root_file(self, provider, file_metadata):
        path = WaterButlerPath('/pfile', prepend=provider.folder)
        url = provider.build_url('files', 'get_metadata')
        data = {'path': path.full_path}
        aiohttpretty.register_json_uri('POST', url, data=data,
                                       body=file_metadata)
        result = await provider.metadata(path)

        assert isinstance(result, metadata.BaseMetadata)
        assert result.kind == 'file'
        assert result.name == 'Getting_Started.pdf'
        assert result.path == '/Getting_Started.pdf'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_missing(self, provider, not_found_metadata_data):
        path = WaterButlerPath('/pfile', prepend=provider.folder)
        url = provider.build_url('files', 'get_metadata')
        data = {"path": "/pfile"}
        aiohttpretty.register_json_uri('POST', url, data=data, status=409,
                                       body=not_found_metadata_data)

        with pytest.raises(exceptions.NotFoundError):
            await provider.metadata(path)


class TestCreateFolder:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_already_exists(self, provider):
        path = WaterButlerPath('/newfolder/', prepend=provider.folder)
        url = provider.build_url('files', 'create_folder')
        data = build_folder_metadata_data(path)
        body = {"error_summary": "path/conflict/folder/...",
                "error": {".tag": "path",
                          "path": {".tag": "conflict",
                                   "conflict": {".tag": "folder"}
                                  }
                         }
               }
        aiohttpretty.register_json_uri('POST', url, data=data, status=409,
                                       body=body)

        with pytest.raises(DropboxNamingConflictError) as e:
            await provider.create_folder(path)

        assert e.value.code == 409
        assert e.value.message == 'Cannot complete action: file or folder already exists in this location'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_forbidden(self, provider):
        path = WaterButlerPath('/newfolder/', prepend=provider.folder)
        url = provider.build_url('files', 'create_folder')
        data = build_folder_metadata_data(path)

        aiohttpretty.register_json_uri('POST', url, data=data, status=403, body={ 'error': 'because I hate you' })

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
    async def test_returns_metadata(self, provider, folder_metadata):
        path = WaterButlerPath('/newfolder/', prepend=provider.folder)
        url = provider.build_url('files', 'create_folder')
        data = build_folder_metadata_data(path)

        aiohttpretty.register_json_uri('POST', url, data=data, status=200, body=folder_metadata)

        resp = await provider.create_folder(path)

        assert resp.kind == 'folder'
        assert resp.name == 'newfolder'


class TestOperations:

    async def test_can_intra_copy(self, provider):
        assert provider.can_intra_copy(provider)

    async def test_can_intra_move(self, provider):
        assert provider.can_intra_move(provider)
