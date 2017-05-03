import pytest

import io
import time
import base64
import hashlib
from http import client
from unittest import mock

import aiohttpretty

from waterbutler.core import streams
from waterbutler.core import metadata
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.swift import SwiftProvider
from waterbutler.providers.swift.metadata import SwiftFileMetadata
from waterbutler.providers.swift.metadata import SwiftFolderMetadata
from swiftclient import quote


@pytest.fixture
def auth():
    return {
        'name': 'cat',
        'email': 'cat@cat.com',
    }


@pytest.fixture
def credentials():
    return {
        'username': 'Dont dead',
        'password': 'open inside',
        'tenant_name': 'test',
        'auth_url': 'http://test_url/v2.0'
    }


@pytest.fixture
def settings():
    return {
        'container': 'that kerning'
    }

@pytest.fixture
def mock_time(monkeypatch):
    mock_time = mock.Mock(return_value=1454684930.0)
    monkeypatch.setattr(time, 'time', mock_time)


@pytest.fixture
def provider(auth, credentials, settings):
    provider = SwiftProvider(auth, credentials, settings)
    return provider


@pytest.fixture
def file_content():
    return b'sleepy'


@pytest.fixture
def file_like(file_content):
    return io.BytesIO(file_content)


@pytest.fixture
def file_stream(file_like):
    return streams.FileStreamReader(file_like)


@pytest.fixture
def folder_metadata():
    return b'''[
  {
    "hash": "3def40db06680692d01f44f2fd12066c",
    "last_modified": "2017-02-09T08:10:51.828100",
    "bytes": 70227,
    "name": "mendeley_cistyle_osf_nii_ac_jp_2.png",
    "content_type": "image/png"
  },
  {
    "hash": "d9a3fdfc7ca17c47ed007bed5d2eb873",
    "last_modified": "2017-02-07T23:09:24.057080",
    "bytes": 9,
    "name": "Photos/test.txt",
    "content_type": "text/plain"
  },
  {
    "hash": "d9a3fdfc7ca17c47ed007bed5d2eb873",
    "last_modified": "2017-02-07T23:09:24.057080",
    "bytes": 9,
    "name": "Photos/a/test.txt",
    "content_type": "text/plain"
  },
  {
    "hash": "d9a3fdfc7ca17c47ed007bed5d2eb873",
    "last_modified": "2017-02-07T23:09:24.057080",
    "bytes": 9,
    "name": "Photos/.osfkeep",
    "content_type": "text/plain"
  },
  {
    "hash": "d9a3fdfc7ca17c47ed007bed5d2eb873",
    "last_modified": "2017-02-07T14:09:55.351480",
    "bytes": 9,
    "name": "test.txt",
    "content_type": "text/plain"
  }
]'''


@pytest.fixture
def file_metadata():
    return {
            'Content-Length': '9',
            'Content-Type': 'text/plain',
            'Last-Modified': 'Tue, 07 Feb 2017 14:09:56 GMT',
            'Etag': 'd9a3fdfc7ca17c47ed007bed5d2eb873',
            'X-Timestamp': '1486476595.35148',
            'X-Object-Meta-Mtime': '1486467323.087638',
            'X-Trans-Id': 'txb0f6ed12846f422d9afd0-0058a5ac17'
    }


class TestValidatePath:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_file(self, provider, file_metadata,
                                         mock_time):
        file_path = 'foobah'
        provider.url = 'http://test_url'
        provider.token = 'test'
        good_metadata_url = provider.generate_url(file_path)
        bad_metadata_url = provider.generate_url()
        aiohttpretty.register_uri('HEAD', good_metadata_url, headers=file_metadata)
        aiohttpretty.register_uri('GET', bad_metadata_url, params={'format': 'json'}, status=404)

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
    async def test_validate_v1_path_folder(self, provider, folder_metadata, mock_time):
        folder_path = 'Photos'

        provider.url = 'http://test_url'
        provider.token = 'test'
        good_metadata_url = provider.generate_url()
        bad_metadata_url = provider.generate_url(folder_path)
        aiohttpretty.register_uri(
            'GET', good_metadata_url, params={'format': 'json'},
            body=folder_metadata, headers={'Content-Type': 'application/json'}
        )
        aiohttpretty.register_uri('HEAD', bad_metadata_url, status=404)

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
    async def test_normal_name(self, provider, mock_time):
        path = await provider.validate_path('/this/is/a/path.txt')
        assert path.name == 'path.txt'
        assert path.parent.name == 'a'
        assert path.is_file
        assert not path.is_dir
        assert not path.is_root

    @pytest.mark.asyncio
    async def test_folder(self, provider, mock_time):
        path = await provider.validate_path('/this/is/a/folder/')
        assert path.name == 'folder'
        assert path.parent.name == 'a'
        assert not path.is_file
        assert path.is_dir
        assert not path.is_root

    @pytest.mark.asyncio
    async def test_root(self, provider, mock_time):
        path = await provider.validate_path('/this/is/a/folder/')
        assert path.name == 'folder'
        assert path.parent.name == 'a'
        assert not path.is_file
        assert path.is_dir
        assert not path.is_root


class TestCRUD:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download(self, provider, mock_time):
        path = WaterButlerPath('/muhtriangle')
        provider.url = 'http://test_url'
        provider.token = 'test'
        url = provider.generate_url(path.path)
        aiohttpretty.register_uri('GET', url, body=b'delicious', auto_length=True)

        result = await provider.download(path)
        content = await result.read()

        assert content == b'delicious'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_folder_400s(self, provider, mock_time):
        with pytest.raises(exceptions.DownloadError) as e:
            await provider.download(WaterButlerPath('/cool/folder/mom/'))
        assert e.value.code == 400

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete(self, provider, mock_time):
        path = WaterButlerPath('/some-file')
        provider.url = 'http://test_url'
        provider.token = 'test'
        url = provider.generate_url(path.path)
        aiohttpretty.register_uri('DELETE', url, status=200)

        await provider.delete(path)

        assert aiohttpretty.has_call(method='DELETE', uri=url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_folder_delete(self, provider, folder_metadata, mock_time):
        path = WaterButlerPath('/Photos/')

        provider.url = 'http://test_url'
        provider.token = 'test'
        url = provider.generate_url()
        aiohttpretty.register_uri(
            'GET', url, params={'format': 'json'},
            body=folder_metadata, headers={'Content-Type': 'application/json'}
        )
        delete_urls = [provider.generate_url(path.path + "test.txt"),
                       provider.generate_url(path.path + ".osfkeep"),
                       provider.generate_url(path.path + "a/test.txt")]
        for delete_url in delete_urls:
            aiohttpretty.register_uri('DELETE', delete_url, status=200)

        await provider.delete(path)

        assert aiohttpretty.has_call(method='DELETE', uri=delete_urls[0])
        assert aiohttpretty.has_call(method='DELETE', uri=delete_urls[1])
        assert aiohttpretty.has_call(method='DELETE', uri=delete_urls[2])

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload(self, provider, file_content, file_stream, file_metadata, mock_time):
        path = WaterButlerPath('/foobah')
        provider.url = 'http://test_url'
        provider.token = 'test'
        url = provider.generate_url(path.path)
        aiohttpretty.register_uri('PUT', url, status=200)
        metadata_url = provider.generate_url(path.path)
        aiohttpretty.register_uri(
            'HEAD',
            metadata_url,
            responses=[
                {'status': 404},
                {'headers': file_metadata},
            ],
        )

        metadata, created = await provider.upload(file_stream, path)

        assert metadata.kind == 'file'
        assert created
        assert aiohttpretty.has_call(method='PUT', uri=url)
        assert aiohttpretty.has_call(method='HEAD', uri=metadata_url)


class TestMetadata:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_root(self, provider, folder_metadata, mock_time):
        path = WaterButlerPath('/')
        assert path.is_root
        provider.url = 'http://test_url'
        provider.token = 'test'
        url = provider.generate_url()
        aiohttpretty.register_uri('GET', url, params={'format': 'json'}, body=folder_metadata,
                                  headers={'Content-Type': 'application/json'})

        result = await provider.metadata(path)

        assert isinstance(result, list)
        assert len(result) == 3
        assert result[0].path == '/Photos/'
        assert result[0].name == 'Photos'
        assert result[0].is_folder

        assert result[1].path == '/mendeley_cistyle_osf_nii_ac_jp_2.png'
        assert result[1].name == 'mendeley_cistyle_osf_nii_ac_jp_2.png'
        assert not result[1].is_folder
        assert result[1].extra['md5'] == '3def40db06680692d01f44f2fd12066c'

        assert result[2].path == '/test.txt'
        assert result[2].name == 'test.txt'
        assert not result[2].is_folder
        assert result[2].extra['md5'] == 'd9a3fdfc7ca17c47ed007bed5d2eb873'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder(self, provider, folder_metadata, mock_time):
        path = WaterButlerPath('/Photos/')
        provider.url = 'http://test_url'
        provider.token = 'test'
        url = provider.generate_url()
        aiohttpretty.register_uri('GET', url, params={'format': 'json'}, body=folder_metadata,
                                  headers={'Content-Type': 'application/json'})

        result = await provider.metadata(path)

        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0].path == '/Photos/a/'
        assert result[0].name == 'a'
        assert result[0].is_folder

        assert result[1].path == '/Photos/test.txt'
        assert result[1].name == 'test.txt'
        assert not result[1].is_folder
        assert result[1].extra['md5'] == 'd9a3fdfc7ca17c47ed007bed5d2eb873'


    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file(self, provider, file_metadata, mock_time):
        path = WaterButlerPath('/Foo/Bar/my-image.jpg')
        provider.url = 'http://test_url'
        provider.token = 'test'
        url = provider.generate_url(path.path)
        aiohttpretty.register_uri('HEAD', url, headers=file_metadata)

        result = await provider.metadata(path)

        assert isinstance(result, metadata.BaseFileMetadata)
        assert result.path == str(path)
        assert result.name == 'my-image.jpg'
        assert result.extra['md5'] == 'd9a3fdfc7ca17c47ed007bed5d2eb873'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file_missing(self, provider, mock_time):
        path = WaterButlerPath('/notfound.txt')
        provider.url = 'http://test_url'
        provider.token = 'test'
        url = provider.generate_url(path.path)
        aiohttpretty.register_uri('HEAD', url, status=404)

        with pytest.raises(exceptions.MetadataError):
            await provider.metadata(path)


class TestCreateFolder:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_must_start_with_slash(self, provider, mock_time):
        path = WaterButlerPath('/alreadyexists')

        with pytest.raises(exceptions.CreateFolderError) as e:
            await provider.create_folder(path)

        assert e.value.code == 400
        assert e.value.message == 'Path must be a directory'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_errors_conflict(self, provider, folder_metadata, mock_time):
        path = WaterButlerPath('/alreadyexists/')
        provider.url = 'http://test_url'
        provider.token = 'test'
        url = provider.generate_url()
        aiohttpretty.register_uri('GET', url,
                                  params={'format': 'json'},
                                  body=folder_metadata,
                                  headers={'Content-Type': 'application/json'})
        url = provider.generate_url('alreadyexists')
        aiohttpretty.register_uri('HEAD', url, status=200)
        url = provider.generate_url('alreadyexists/.osfkeep')
        aiohttpretty.register_uri('PUT', url, status=200)

        with pytest.raises(exceptions.FolderNamingConflict) as e:
            await provider.create_folder(path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_creates(self, provider, folder_metadata, mock_time):
        path = WaterButlerPath('/doesntalreadyexists/')
        provider.url = 'http://test_url'
        provider.token = 'test'
        url = provider.generate_url()
        aiohttpretty.register_uri('GET', url,
                                  params={'format': 'json'},
                                  body=folder_metadata,
                                  headers={'Content-Type': 'application/json'})
        url = provider.generate_url('doesntalreadyexists')
        aiohttpretty.register_uri('HEAD', url, status=404)
        url = provider.generate_url('doesntalreadyexists/.osfkeep')
        aiohttpretty.register_uri('PUT', url, status=200)

        resp = await provider.create_folder(path)

        assert resp.kind == 'folder'
        assert resp.name == 'doesntalreadyexists'
        assert resp.path == '/doesntalreadyexists/'


class TestOperations:

    async def test_equality(self, provider, mock_time):
        assert provider.can_intra_copy(provider)
        assert provider.can_intra_move(provider)
