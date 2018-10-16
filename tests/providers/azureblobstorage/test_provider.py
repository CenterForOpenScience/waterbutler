import random
import string
import math

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

from waterbutler.providers.azureblobstorage import AzureBlobStorageProvider
from waterbutler.providers.azureblobstorage.metadata import AzureBlobStorageFileMetadata
from waterbutler.providers.azureblobstorage.metadata import AzureBlobStorageFolderMetadata
from waterbutler.providers.azureblobstorage.provider import (
    MAX_UPLOAD_BLOCK_SIZE,
    MAX_UPLOAD_ONCE_SIZE,
)


@pytest.fixture
def auth():
    return {
        'name': 'cat',
        'email': 'cat@cat.com',
    }


@pytest.fixture
def credentials():
    return {
        'account_name': 'dontdead',
        'account_key': base64.b64encode(b'open inside'),
    }


@pytest.fixture
def settings():
    return {
        'container': 'thatkerning'
    }

@pytest.fixture
def mock_time(monkeypatch):
    mock_time = mock.Mock(return_value=1454684930.0)
    monkeypatch.setattr(time, 'time', mock_time)


@pytest.fixture
def provider(auth, credentials, settings):
    provider = AzureBlobStorageProvider(auth, credentials, settings)
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
def large_file_content():
    # 71MB (4MB * 17 + 3MB)
    return b'a' * (71 * (2 ** 20))


@pytest.fixture
def large_file_like(large_file_content):
    return io.BytesIO(large_file_content)


@pytest.fixture
def large_file_stream(large_file_like):
    return streams.FileStreamReader(large_file_like)


@pytest.fixture
def folder_metadata():
    return b'''<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults ServiceEndpoint="https://vrosf.blob.core.windows.net/" ContainerName="sample-container1">
  <Blobs>
    <Blob>
      <Name>Photos/test-text.txt</Name>
      <Properties>
        <Last-Modified>Thu, 10 Nov 2016 11:04:45 GMT</Last-Modified>
        <Etag>0x8D40959613D32F6</Etag>
        <Content-Length>0</Content-Length>
        <Content-Type>text/plain</Content-Type>
        <Content-Encoding />
        <Content-Language />
        <Content-MD5 />
        <Cache-Control />
        <Content-Disposition />
        <BlobType>BlockBlob</BlobType>
        <LeaseStatus>unlocked</LeaseStatus>
        <LeaseState>available</LeaseState>
      </Properties>
    </Blob>
    <Blob>
      <Name>Photos/a/test.txt</Name>
      <Properties>
        <Last-Modified>Thu, 10 Nov 2016 11:04:45 GMT</Last-Modified>
        <Etag>0x8D40959613D32F6</Etag>
        <Content-Length>0</Content-Length>
        <Content-Type>text/plain</Content-Type>
        <Content-Encoding />
        <Content-Language />
        <Content-MD5 />
        <Cache-Control />
        <Content-Disposition />
        <BlobType>BlockBlob</BlobType>
        <LeaseStatus>unlocked</LeaseStatus>
        <LeaseState>available</LeaseState>
      </Properties>
    </Blob>
    <Blob>
      <Name>top.txt</Name>
      <Properties>
        <Last-Modified>Thu, 10 Nov 2016 11:04:45 GMT</Last-Modified>
        <Etag>0x8D40959613D32F6</Etag>
        <Content-Length>0</Content-Length>
        <Content-Type>text/plain</Content-Type>
        <Content-Encoding />
        <Content-Language />
        <Content-MD5 />
        <Cache-Control />
        <Content-Disposition />
        <BlobType>BlockBlob</BlobType>
        <LeaseStatus>unlocked</LeaseStatus>
        <LeaseState>available</LeaseState>
      </Properties>
    </Blob>
  </Blobs>
  <NextMarker />
</EnumerationResults>'''


@pytest.fixture
def file_metadata():
    return {
        'CONTENT-LENGTH': '0',
        'CONTENT-TYPE': 'text/plain',
        'LAST-MODIFIED': 'Thu, 10 Nov 2016 11:04:45 GMT',
        'ACCEPT-RANGES': 'bytes',
        'ETAG': '"0x8D40959613D32F6"',
        'SERVER': 'Windows-Azure-Blob/1.0 Microsoft-HTTPAPI/2.0',
        'X-MS-REQUEST-ID': '5b4a3cb6-0001-00ea-4575-895e2c000000',
        'X-MS-VERSION': '2015-07-08',
        'X-MS-LEASE-STATUS': 'unlocked',
        'X-MS-LEASE-STATE': 'available',
        'X-MS-BLOB-TYPE': 'BlockBlob',
        'DATE': 'Fri, 17 Feb 2017 23:28:33 GMT'
    }


@pytest.fixture
def large_file_metadata(large_file_content):
    return {
        'CONTENT-LENGTH': str(len(large_file_content)),
        'CONTENT-TYPE': 'text/plain',
        'LAST-MODIFIED': 'Thu, 10 Nov 2016 11:04:45 GMT',
        'ACCEPT-RANGES': 'bytes',
        'ETAG': '"0x8D40959613D32F6"',
        'SERVER': 'Windows-Azure-Blob/1.0 Microsoft-HTTPAPI/2.0',
        'X-MS-REQUEST-ID': '5b4a3cb6-0001-00ea-4575-895e2c000000',
        'X-MS-VERSION': '2015-07-08',
        'X-MS-LEASE-STATUS': 'unlocked',
        'X-MS-LEASE-STATE': 'available',
        'X-MS-BLOB-TYPE': 'BlockBlob',
        'DATE': 'Fri, 17 Feb 2017 23:28:33 GMT'
    }


class TestValidatePath:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_file(self, provider, file_metadata,
                                         mock_time):
        file_path = 'foobah'
        for good_metadata_url in provider.generate_urls(file_path, secondary=True):
            aiohttpretty.register_uri('HEAD', good_metadata_url, headers=file_metadata)
        for bad_metadata_url in provider.generate_urls(secondary=True):
            aiohttpretty.register_uri('GET', bad_metadata_url,
                                      params={'restype': 'container', 'comp': 'list'}, status=404)

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

        for good_metadata_url in provider.generate_urls(secondary=True):
            aiohttpretty.register_uri(
                'GET', good_metadata_url, params={'restype': 'container', 'comp': 'list'},
                body=folder_metadata, headers={'Content-Type': 'application/xml'}
            )
        for bad_metadata_url in provider.generate_urls(folder_path, secondary=True):
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
        for url in provider.generate_urls(path.path, secondary=True):
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
        for url in provider.generate_urls(path.path):
            aiohttpretty.register_uri('DELETE', url, status=200)

        await provider.delete(path)

        assert aiohttpretty.has_call(method='DELETE', uri=url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_folder_delete(self, provider, folder_metadata, mock_time):
        path = WaterButlerPath('/Photos/')

        for url in provider.generate_urls(secondary=True):
            aiohttpretty.register_uri(
                'GET', url, params={'restype': 'container', 'comp': 'list'},
                body=folder_metadata, headers={'Content-Type': 'application/xml'}
            )
        delete_urls = []
        for url in provider.generate_urls(path.path + "test-text.txt"):
            aiohttpretty.register_uri('DELETE', url, status=200)
            delete_urls.append(url)
        for url in provider.generate_urls(path.path + "a/test.txt"):
            aiohttpretty.register_uri('DELETE', url, status=200)
            delete_urls.append(url)

        await provider.delete(path)

        assert aiohttpretty.has_call(method='DELETE', uri=delete_urls[0])
        assert aiohttpretty.has_call(method='DELETE', uri=delete_urls[1])


class TestMetadata:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_root(self, provider, folder_metadata, mock_time):
        path = WaterButlerPath('/')
        assert path.is_root
        for url in provider.generate_urls(secondary=True):
            aiohttpretty.register_uri('GET', url,
                                      params={'restype': 'container', 'comp': 'list'},
                                      body=folder_metadata,
                                      headers={'Content-Type': 'application/xml'})

        result = await provider.metadata(path)

        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0].path == '/Photos/'
        assert result[0].name == 'Photos'
        assert result[0].is_folder

        assert result[1].path == '/top.txt'
        assert result[1].name == 'top.txt'
        assert not result[1].is_folder
        assert result[1].extra['md5'] == None

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder(self, provider, folder_metadata, mock_time):
        path = WaterButlerPath('/Photos/')
        for url in provider.generate_urls(secondary=True):
            aiohttpretty.register_uri('GET', url,
                                      params={'restype': 'container', 'comp': 'list'},
                                      body=folder_metadata,
                                      headers={'Content-Type': 'application/xml'})

        result = await provider.metadata(path)

        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0].path == '/Photos/a/'
        assert result[0].name == 'a'
        assert result[0].is_folder

        assert result[1].path == '/Photos/test-text.txt'
        assert result[1].name == 'test-text.txt'
        assert not result[1].is_folder
        assert result[1].extra['md5'] == None

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file(self, provider, file_metadata, mock_time):
        path = WaterButlerPath('/Foo/Bar/my-image.jpg')
        provider.url = 'http://test_url'
        provider.token = 'test'
        for url in provider.generate_urls(path.path, secondary=True):
            aiohttpretty.register_uri('HEAD', url, headers=file_metadata)

        result = await provider.metadata(path)

        assert isinstance(result, metadata.BaseFileMetadata)
        assert result.path == str(path)
        assert result.name == 'my-image.jpg'
        assert result.modified is not None
        assert result.extra['md5'] == None

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file_missing(self, provider, mock_time):
        path = WaterButlerPath('/notfound.txt')
        provider.url = 'http://test_url'
        provider.token = 'test'
        for url in provider.generate_urls(path.path, secondary=True):
            aiohttpretty.register_uri('HEAD', url, status=404)

        with pytest.raises(exceptions.MetadataError):
            await provider.metadata(path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload(self, provider, file_content, file_stream, file_metadata, mock_time):
        path = WaterButlerPath('/foobah')
        for url in provider.generate_urls(path.path):
            aiohttpretty.register_uri('PUT', url, status=200)
        for metadata_url in provider.generate_urls(path.path):
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

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_large(self, provider, large_file_content, large_file_stream, large_file_metadata, mock_time):
        # upload 4MB data 17 times and 3MB once, and request block_list
        upload_times = math.floor(len(large_file_content) / MAX_UPLOAD_BLOCK_SIZE)
        block_id_prefix = 'hogefuga'
        block_id_list = [AzureBlobStorageProvider._format_block_id(block_id_prefix, i) for i in range(upload_times)]
        block_req_params_list = [{'comp': 'block', 'blockid': block_id} for block_id in block_id_list]
        block_list_req_params = {'comp': 'blocklist'}

        path = WaterButlerPath('/large_foobah')

        for url in provider.generate_urls(path.path):
            for block_req_params in block_req_params_list:
                aiohttpretty.register_uri('PUT', url, status=200, params=block_req_params)
            aiohttpretty.register_uri('PUT', url, status=200, params=block_list_req_params)
        for metadata_url in provider.generate_urls(path.path):
            aiohttpretty.register_uri(
                'HEAD',
                metadata_url,
                responses=[
                    {'status': 404},
                    {'headers': large_file_metadata},
                ],
            )

        metadata, created = await provider.upload(large_file_stream, path, block_id_list=block_id_list)

        assert metadata.kind == 'file'
        assert created
        for block_req_params in block_req_params_list:
            assert aiohttpretty.has_call(method='PUT', uri=url, params=block_req_params)
        assert aiohttpretty.has_call(method='PUT', uri=url, params=block_list_req_params)
        assert aiohttpretty.has_call(method='HEAD', uri=metadata_url)


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
        for url in provider.generate_urls(secondary=True):
            aiohttpretty.register_uri('GET', url,
                                      params={'restype': 'container', 'comp': 'list'},
                                      body=folder_metadata,
                                      headers={'Content-Type': 'application/xml'})
        for url in provider.generate_urls('alreadyexists', secondary=True):
            aiohttpretty.register_uri('HEAD', url, status=200)
        for url in provider.generate_urls('alreadyexists/.osfkeep'):
            aiohttpretty.register_uri('PUT', url, status=200)

        with pytest.raises(exceptions.FolderNamingConflict) as e:
            await provider.create_folder(path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_creates(self, provider, folder_metadata, mock_time):
        path = WaterButlerPath('/doesntalreadyexists/')
        for url in provider.generate_urls(secondary=True):
            aiohttpretty.register_uri('GET', url,
                                      params={'restype': 'container', 'comp': 'list'},
                                      body=folder_metadata,
                                      headers={'Content-Type': 'application/xml'})
        for url in provider.generate_urls('doesntalreadyexists', secondary=True):
            aiohttpretty.register_uri('HEAD', url, status=404)
        for url in provider.generate_urls('doesntalreadyexists/.osfkeep'):
            aiohttpretty.register_uri('PUT', url, status=200)

        resp = await provider.create_folder(path)

        assert resp.kind == 'folder'
        assert resp.name == 'doesntalreadyexists'
        assert resp.path == '/doesntalreadyexists/'


class TestOperations:

    async def test_equality(self, provider, mock_time):
        assert provider.can_intra_copy(provider)
        assert provider.can_intra_move(provider)
