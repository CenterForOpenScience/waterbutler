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
def folder_metadata():
    return {
        "size": "0 bytes",
        "hash": "37eb1ba1849d4b0fb0b28caf7ef3af52",
        "bytes": 0,
        "thumb_exists": False,
        "rev": "714f029684fe",
        "modified": "Wed, 27 Apr 2011 22:18:51 +0000",
        "path": "/Photos",
        "is_dir": True,
        "icon": "folder",
        "root": "dropbox",
        "contents": [
            {
                "size": "2.3 MB",
                "rev": "38af1b183490",
                "thumb_exists": True,
                "bytes": 2453963,
                "modified": "Mon, 07 Apr 2014 23:13:16 +0000",
                "client_mtime": "Thu, 29 Aug 2013 01:12:02 +0000",
                "path": "/Photos/flower.jpg",
                "photo_info": {
                "lat_long": [
                    37.77256666666666,
                    -122.45934166666667
                ],
                "time_taken": "Wed, 28 Aug 2013 18:12:02 +0000"
                },
                "is_dir": False,
                "icon": "page_white_picture",
                "root": "dropbox",
                "mime_type": "image/jpeg",
                "revision": 14511
            }
        ],
        "revision": 29007
    }


@pytest.fixture
def file_metadata():
    return {
        "size": "225.4KB",
        "rev": "35e97029684fe",
        "thumb_exists": False,
        "bytes": 230783,
        "modified": "Tue, 19 Jul 2011 21:55:38 +0000",
        "client_mtime": "Mon, 18 Jul 2011 18:04:35 +0000",
        "path": "/Photos/Getting_Started.pdf",
        "is_dir": False,
        "icon": "page_white_acrobat",
        "root": "dropbox",
        "mime_type": "application/pdf",
        "revision": 220823
    }


def build_folder_metadata_params(path):
    return {'root': 'auto', 'path': path.full_path}


class TestValidatePath:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('settings', [{'folder': '/'}])
    async def test_validate_v1_path_file(self, provider, file_metadata):
        file_path = 'Photos/Getting_Started.pdf'

        metadata_url = provider.build_url('metadata', 'auto', file_path)
        aiohttpretty.register_json_uri('GET', metadata_url, body=file_metadata)

        try:
            wb_path_v1 = await provider.validate_v1_path('/' + file_path)
        except Exception as exc:
            pytest.fail(str(exc))

        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider.validate_v1_path('/' + file_path + '/')

        assert exc.value.code == client.NOT_FOUND

        wb_path_v0 = await provider.validate_path('/' + file_path)

        assert wb_path_v1 == wb_path_v0

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('settings', [{'folder': '/'}])
    async def test_validate_v1_path_folder(self, provider, folder_metadata):
        folder_path = 'Photos'

        metadata_url = provider.build_url('metadata', 'auto', folder_path)
        aiohttpretty.register_json_uri('GET', metadata_url, body=folder_metadata)

        try:
            wb_path_v1 = await provider.validate_v1_path('/' + folder_path + '/')
        except Exception as exc:
            pytest.fail(str(exc))

        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider.validate_v1_path('/' + folder_path)

        assert exc.value.code == client.NOT_FOUND

        wb_path_v0 = await provider.validate_path('/' + folder_path + '/')

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
        url = provider._build_content_url('files', 'auto', path.full_path)
        aiohttpretty.register_uri('GET', url, body=b'better', auto_length=True)
        result = await provider.download(path)
        content = await result.response.read()

        assert content == b'better'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_not_found(self, provider):
        path = await provider.validate_path('/vectors.txt')
        url = provider._build_content_url('files', 'auto', path.full_path)
        aiohttpretty.register_uri('GET', url, status=404)

        with pytest.raises(exceptions.DownloadError) as e:
            await provider.download(path)

        assert e.value.code == 404

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload(self, provider, file_metadata, file_stream, settings):
        path = await provider.validate_path('/phile')

        metadata_url = provider.build_url('metadata', 'auto', path.full_path)
        url = provider._build_content_url('files_put', 'auto', path.full_path)

        aiohttpretty.register_uri('GET', metadata_url, status=404)
        aiohttpretty.register_json_uri('PUT', url, status=200, body=file_metadata)

        metadata, created = await provider.upload(file_stream, path)
        expected = DropboxFileMetadata(file_metadata, provider.folder)

        assert created is True
        assert metadata == expected
        assert aiohttpretty.has_call(method='PUT', uri=url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_file(self, provider, file_metadata):
        url = provider.build_url('fileops', 'delete')
        path = await provider.validate_path('/The past')

        aiohttpretty.register_uri('POST', url, status=200)

        await provider.delete(path)

        data = {'root': 'auto', 'path': path.full_path}
        assert aiohttpretty.has_call(method='POST', uri=url, data=data)


class TestMetadata:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata(self, provider, folder_metadata):
        path = await provider.validate_path('/')
        url = provider.build_url('metadata', 'auto', path.full_path)
        aiohttpretty.register_json_uri('GET', url, body=folder_metadata)
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
        url = provider.build_url('metadata', 'auto', path.full_path)
        aiohttpretty.register_json_uri('GET', url, body=file_metadata)
        result = await provider.metadata(path)

        assert isinstance(result, metadata.BaseMetadata)
        assert result.kind == 'file'
        assert result.name == 'Getting_Started.pdf'
        assert result.path == '/Getting_Started.pdf'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_missing(self, provider):
        path = WaterButlerPath('/pfile', prepend=provider.folder)
        url = provider.build_url('metadata', 'auto', path.full_path)
        aiohttpretty.register_uri('GET', url, status=404)

        with pytest.raises(exceptions.MetadataError):
            await provider.metadata(path)


class TestCreateFolder:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_already_exists(self, provider):
        path = WaterButlerPath('/newfolder/', prepend=provider.folder)
        url = provider.build_url('fileops', 'create_folder')
        params = build_folder_metadata_params(path)

        aiohttpretty.register_json_uri('POST', url, params=params, status=403, body={
            'error': 'because a file or folder already exists at path'
        })

        with pytest.raises(exceptions.FolderNamingConflict) as e:
            await provider.create_folder(path)

        assert e.value.code == 409
        assert e.value.message == 'Cannot create folder "newfolder" because a file or folder already exists at path "/newfolder/"'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_forbidden(self, provider):
        path = WaterButlerPath('/newfolder/', prepend=provider.folder)
        url = provider.build_url('fileops', 'create_folder')
        params = build_folder_metadata_params(path)

        aiohttpretty.register_json_uri('POST', url, params=params, status=403, body={
            'error': 'because I hate you'
        })

        with pytest.raises(exceptions.CreateFolderError) as e:
            await provider.create_folder(path)

        assert e.value.code == 403
        assert e.value.data['error'] == 'because I hate you'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_raises_on_errors(self, provider):
        path = WaterButlerPath('/newfolder/', prepend=provider.folder)
        url = provider.build_url('fileops', 'create_folder')
        params = build_folder_metadata_params(path)

        aiohttpretty.register_json_uri('POST', url, params=params, status=418, body={})

        with pytest.raises(exceptions.CreateFolderError) as e:
            await provider.create_folder(path)

        assert e.value.code == 418

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_returns_metadata(self, provider, file_metadata):
        file_metadata['path'] = '/newfolder'
        path = WaterButlerPath('/newfolder/', prepend=provider.folder)
        url = provider.build_url('fileops', 'create_folder')
        params = build_folder_metadata_params(path)

        aiohttpretty.register_json_uri('POST', url, params=params, status=200, body=file_metadata)

        resp = await provider.create_folder(path)

        assert resp.kind == 'folder'
        assert resp.name == 'newfolder'


class TestOperations:

    async def test_can_intra_copy(self, provider):
        assert provider.can_intra_copy(provider)

    async def test_can_intra_move(self, provider):
        assert provider.can_intra_move(provider)
