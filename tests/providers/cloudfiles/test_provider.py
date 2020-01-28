import io
import time
import hashlib
from unittest import mock

import furl
import pytest
import multidict
import aiohttpretty

from waterbutler.core import streams, exceptions
from waterbutler.core.path import WaterButlerPath
from waterbutler.providers.cloudfiles import CloudFilesProvider
from waterbutler.providers.cloudfiles import settings as cloud_settings


@pytest.fixture
def auth():
    return {
        'name': 'cat',
        'email': 'cat@cat.com',
    }


@pytest.fixture
def credentials():
    return {
        'username': 'prince',
        'token': 'revolutionary',
        'region': 'iad',
    }


@pytest.fixture
def settings():
    return {'container': 'purple rain'}


@pytest.fixture
def provider(auth, credentials, settings):
    return CloudFilesProvider(auth, credentials, settings)


@pytest.fixture
def auth_json():
    return {
        "access": {
            "serviceCatalog": [
                {
                    "name": "cloudFiles",
                    "type": "object-store",
                    "endpoints": [
                        {
                            "publicURL": "https://fakestorage",
                            "internalURL": "https://internal_fake_storage",
                            "region": "IAD",
                            "tenantId": "someid_123456"
                        },
                    ]
                }
            ],
            "token": {
                "RAX-AUTH:authenticatedBy": [
                    "APIKEY"
                ],
                "tenant": {
                    "name": "12345",
                    "id": "12345"
                },
                "id": "2322f6b2322f4dbfa69802baf50b0832",
                "expires": "2014-12-17T09:12:26.069Z"
            },
            "user": {
                "name": "osf-production",
                "roles": [
                    {
                        "name": "object-store:admin",
                        "id": "10000256",
                        "description": "Object Store Admin Role for Account User"
                    },
                    {
                        "name": "compute:default",
                        "description": "A Role that allows a user access to keystone Service methods",
                        "id": "6",
                        "tenantId": "12345"
                    },
                    {
                        "name": "object-store:default",
                        "description": "A Role that allows a user access to keystone Service methods",
                        "id": "5",
                        "tenantId": "some_id_12345"
                    },
                    {
                        "name": "identity:default",
                        "id": "2",
                        "description": "Default Role."
                    }
                ],
                "id": "secret",
                "RAX-AUTH:defaultRegion": "IAD"
            }
        }
    }


@pytest.fixture
def token(auth_json):
    return auth_json['access']['token']['id']


@pytest.fixture
def endpoint(auth_json):
    return auth_json['access']['serviceCatalog'][0]['endpoints'][0]['publicURL']


@pytest.fixture
def temp_url_key():
    return 'temporary beret'


@pytest.fixture
def mock_auth(auth_json):
    aiohttpretty.register_json_uri(
        'POST',
        settings.AUTH_URL,
        body=auth_json,
    )


@pytest.fixture
def mock_temp_key(endpoint, temp_url_key):
    aiohttpretty.register_uri(
        'HEAD',
        endpoint,
        status=204,
        headers={'X-Account-Meta-Temp-URL-Key': temp_url_key},
    )


@pytest.fixture
def mock_time(monkeypatch):
    mock_time = mock.Mock()
    mock_time.return_value = 10
    monkeypatch.setattr(time, 'time', mock_time)


@pytest.fixture
def connected_provider(provider, token, endpoint, temp_url_key, mock_time):
    provider.token = token
    provider.endpoint = endpoint
    provider.temp_url_key = temp_url_key.encode()
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
def file_metadata():
    return multidict.CIMultiDict([
        ('LAST-MODIFIED', 'Thu, 25 Dec 2014 02:54:35 GMT'),
        ('CONTENT-LENGTH', '0'),
        ('ETAG', 'edfa12d00b779b4b37b81fe5b61b2b3f'),
        ('CONTENT-TYPE', 'text/html; charset=UTF-8'),
        ('X-TRANS-ID', 'txf876a4b088e3451d94442-00549b7c6aiad3'),
        ('DATE', 'Thu, 25 Dec 2014 02:54:34 GMT')
    ])


# Metadata Test Scenarios
# / (folder_root_empty)
# / (folder_root)
# /level1/  (folder_root_level1)
# /level1/level2/ (folder_root_level1_level2)
# /level1/level2/file2.file - (file_root_level1_level2_file2_txt)
# /level1_empty/ (folder_root_level1_empty)
# /similar (file_similar)
# /similar.name (file_similar_name)
# /does_not_exist (404)
# /does_not_exist/ (404)


@pytest.fixture
def folder_root_empty():
    return []


@pytest.fixture
def folder_root():
    return [
        {
            'last_modified': '2014-12-19T22:08:23.006360',
            'content_type': 'application/directory',
            'hash': 'd41d8cd98f00b204e9800998ecf8427e',
            'name': 'level1',
            'bytes': 0
        },
        {
            'subdir': 'level1/'
        },
        {
            'last_modified': '2014-12-19T23:22:23.232240',
            'content_type': 'application/x-www-form-urlencoded;charset=utf-8',
            'hash': 'edfa12d00b779b4b37b81fe5b61b2b3f',
            'name': 'similar',
            'bytes': 190
        },
        {
            'last_modified': '2014-12-19T23:22:14.728640',
            'content_type': 'application/x-www-form-urlencoded;charset=utf-8',
            'hash': 'edfa12d00b779b4b37b81fe5b61b2b3f',
            'name': 'similar.file',
            'bytes': 190
        },
        {
            'last_modified': '2014-12-19T23:20:16.718860',
            'content_type': 'application/directory',
            'hash': 'd41d8cd98f00b204e9800998ecf8427e',
            'name': 'level1_empty',
            'bytes': 0
        }
    ]


@pytest.fixture
def folder_root_level1():
    return [
        {
            'last_modified': '2014-12-19T22:08:26.958830',
            'content_type': 'application/directory',
            'hash': 'd41d8cd98f00b204e9800998ecf8427e',
            'name': 'level1/level2',
            'bytes': 0
        },
        {
            'subdir': 'level1/level2/'
        }
    ]


@pytest.fixture
def folder_root_level1_level2():
    return [
        {
            'name': 'level1/level2/file2.txt',
            'content_type': 'application/x-www-form-urlencoded;charset=utf-8',
            'last_modified': '2014-12-19T23:25:22.497420',
            'bytes': 1365336,
            'hash': 'ebc8cdd3f712fd39476fb921d43aca1a'
        }
    ]


@pytest.fixture
def file_root_level1_level2_file2_txt():
    return multidict.CIMultiDict([
        ('ORIGIN', 'https://mycloud.rackspace.com'),
        ('CONTENT-LENGTH', '216945'),
        ('ACCEPT-RANGES', 'bytes'),
        ('LAST-MODIFIED', 'Mon, 22 Dec 2014 19:01:02 GMT'),
        ('ETAG', '44325d4f13b09f3769ede09d7c20a82c'),
        ('X-TIMESTAMP', '1419274861.04433'),
        ('CONTENT-TYPE', 'text/plain'),
        ('X-TRANS-ID', 'tx836375d817a34b558756a-0054987deeiad3'),
        ('DATE', 'Mon, 22 Dec 2014 20:24:14 GMT')
    ])


@pytest.fixture
def folder_root_level1_empty():
    return multidict.CIMultiDict([
        ('ORIGIN', 'https://mycloud.rackspace.com'),
        ('CONTENT-LENGTH', '0'),
        ('ACCEPT-RANGES', 'bytes'),
        ('LAST-MODIFIED', 'Mon, 22 Dec 2014 18:58:56 GMT'),
        ('ETAG', 'd41d8cd98f00b204e9800998ecf8427e'),
        ('X-TIMESTAMP', '1419274735.03160'),
        ('CONTENT-TYPE', 'application/directory'),
        ('X-TRANS-ID', 'txd78273e328fc4ba3a98e3-0054987eeeiad3'),
        ('DATE', 'Mon, 22 Dec 2014 20:28:30 GMT')
    ])


@pytest.fixture
def file_root_similar():
    return multidict.CIMultiDict([
        ('ORIGIN', 'https://mycloud.rackspace.com'),
        ('CONTENT-LENGTH', '190'),
        ('ACCEPT-RANGES', 'bytes'),
        ('LAST-MODIFIED', 'Fri, 19 Dec 2014 23:22:24 GMT'),
        ('ETAG', 'edfa12d00b779b4b37b81fe5b61b2b3f'),
        ('X-TIMESTAMP', '1419031343.23224'),
        ('CONTENT-TYPE', 'application/x-www-form-urlencoded;charset=utf-8'),
        ('X-TRANS-ID', 'tx7cfeef941f244807aec37-005498754diad3'),
        ('DATE', 'Mon, 22 Dec 2014 19:47:25 GMT')
    ])


@pytest.fixture
def file_root_similar_name():
    return multidict.CIMultiDict([
        ('ORIGIN', 'https://mycloud.rackspace.com'),
        ('CONTENT-LENGTH', '190'),
        ('ACCEPT-RANGES', 'bytes'),
        ('LAST-MODIFIED', 'Mon, 22 Dec 2014 19:07:12 GMT'),
        ('ETAG', 'edfa12d00b779b4b37b81fe5b61b2b3f'),
        ('X-TIMESTAMP', '1419275231.66160'),
        ('CONTENT-TYPE', 'application/x-www-form-urlencoded;charset=utf-8'),
        ('X-TRANS-ID', 'tx438cbb32b5344d63b267c-0054987f3biad3'),
        ('DATE', 'Mon, 22 Dec 2014 20:29:47 GMT')
    ])


class TestCRUD:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download(self, connected_provider):
        body = b'dearly-beloved'
        path = WaterButlerPath('/lets-go-crazy')
        url = connected_provider.sign_url(path)

        aiohttpretty.register_uri('GET', url, body=body, auto_length=True)

        result = await connected_provider.download(path)
        content = await result.read()

        assert content == body

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_accept_url(self, connected_provider):
        path = WaterButlerPath('/lets-go-crazy')

        url = connected_provider.sign_url(path)
        parsed_url = furl.furl(url)
        parsed_url.args['filename'] = 'lets-go-crazy'

        result = await connected_provider.download(path, accept_url=True)

        assert result == parsed_url.url

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize("display_name_arg,expected_name", [
        ('meow.txt', 'meow.txt'),
        ('',         'lets-go-crazy'),
        (None,       'lets-go-crazy'),
    ])
    async def test_download_file_with_display_name(self, connected_provider, display_name_arg,
                                                   expected_name):
        path = WaterButlerPath('/lets-go-crazy')

        url = connected_provider.sign_url(path)
        parsed_url = furl.furl(url)
        parsed_url.args['filename'] = expected_name

        result = await connected_provider.download(path, accept_url=True,
                                                   display_name=display_name_arg)

        assert result == parsed_url.url

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_not_found(self, connected_provider):
        path = WaterButlerPath('/lets-go-crazy')
        url = connected_provider.sign_url(path)
        aiohttpretty.register_uri('GET', url, status=404)
        with pytest.raises(exceptions.DownloadError):
            await connected_provider.download(path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload(self, connected_provider, file_content, file_stream, file_metadata):
        path = WaterButlerPath('/foo.bar')
        content_md5 = hashlib.md5(file_content).hexdigest()
        metadata_url = connected_provider.build_url(path.path)
        url = connected_provider.sign_url(path, 'PUT')
        aiohttpretty.register_uri(
            'HEAD',
            metadata_url,
            responses=[
                {'status': 404},
                {'headers': file_metadata},
            ]
        )
        aiohttpretty.register_uri('PUT', url, status=200,
                                  headers={'ETag': '"{}"'.format(content_md5)})
        metadata, created = await connected_provider.upload(file_stream, path)

        assert created is True
        assert metadata.kind == 'file'
        assert aiohttpretty.has_call(method='PUT', uri=url)
        assert aiohttpretty.has_call(method='HEAD', uri=metadata_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_check_none(self, connected_provider,
                                    file_content, file_stream, file_metadata):
        path = WaterButlerPath('/foo.bar')
        content_md5 = hashlib.md5(file_content).hexdigest()
        metadata_url = connected_provider.build_url(path.path)
        url = connected_provider.sign_url(path, 'PUT')
        aiohttpretty.register_uri(
            'HEAD',
            metadata_url,
            responses=[
                {'status': 404},
                {'headers': file_metadata},
            ]
        )
        aiohttpretty.register_uri('PUT', url, status=200,
                                  headers={'ETag': '"{}"'.format(content_md5)})
        metadata, created = await connected_provider.upload(
            file_stream, path, check_created=False, fetch_metadata=False)

        assert created is None
        assert metadata is None
        assert aiohttpretty.has_call(method='PUT', uri=url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_checksum_mismatch(self, connected_provider, file_stream, file_metadata):
        path = WaterButlerPath('/foo.bar')
        metadata_url = connected_provider.build_url(path.path)
        url = connected_provider.sign_url(path, 'PUT')
        aiohttpretty.register_uri(
            'HEAD',
            metadata_url,
            responses=[
                {'status': 404},
                {'headers': file_metadata},
            ]
        )
        aiohttpretty.register_uri('PUT', url, status=200, headers={'ETag': '"Bogus MD5"'})

        with pytest.raises(exceptions.UploadChecksumMismatchError):
            await connected_provider.upload(file_stream, path)

        assert aiohttpretty.has_call(method='PUT', uri=url)
        assert aiohttpretty.has_call(method='HEAD', uri=metadata_url)

    # @pytest.mark.asyncio
    # @pytest.mark.aiohttpretty
    # async def test_delete_folder(self, connected_provider, folder_root_empty, file_metadata):
    #     # This test will probably fail on a live
    #     # version of the provider because build_url is called wrong.
    #     # Will comment out parts of this test till that is fixed.
    #     path = WaterButlerPath('/delete/')
    #     query = {'prefix': path.path}
    #     url = connected_provider.build_url('', **query)
    #     body = json.dumps(folder_root_empty).encode('utf-8')

    #     delete_query = {'bulk-delete': ''}
    #     delete_url = connected_provider.build_url('', **delete_query)

    #     file_url = connected_provider.build_url(path.path)

    #     aiohttpretty.register_uri('GET', url, body=body)
    #     aiohttpretty.register_uri('HEAD', file_url, headers=file_metadata)

    #     aiohttpretty.register_uri('DELETE', delete_url)

    #     await connected_provider.delete(path)

    #     assert aiohttpretty.has_call(method='DELETE', uri=delete_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_file(self, connected_provider):
        path = WaterButlerPath('/delete.file')
        url = connected_provider.build_url(path.path)
        aiohttpretty.register_uri('DELETE', url, status=204)
        await connected_provider.delete(path)

        assert aiohttpretty.has_call(method='DELETE', uri=url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_copy(self, connected_provider, file_metadata):
        src_path = WaterButlerPath('/delete.file')
        dest_path = WaterButlerPath('/folder1/delete.file')

        dest_url = connected_provider.build_url(dest_path.path)

        aiohttpretty.register_uri('HEAD', dest_url, headers=file_metadata)
        aiohttpretty.register_uri('PUT', dest_url, status=201)

        result = await connected_provider.intra_copy(connected_provider, src_path, dest_path)

        assert result[0].path == '/folder1/delete.file'
        assert result[0].name == 'delete.file'
        assert result[0].etag == 'edfa12d00b779b4b37b81fe5b61b2b3f'


class TestMetadata:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder_root_empty(self, connected_provider, folder_root_empty):
        path = WaterButlerPath('/')
        body = folder_root_empty
        url = connected_provider.build_url(path.path, prefix=path.path, delimiter='/')
        aiohttpretty.register_json_uri('GET', url, status=200, body=body)
        result = await connected_provider.metadata(path)

        assert len(result) == 0
        assert result == []

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder_root(self, connected_provider, folder_root):
        path = WaterButlerPath('/')
        body = folder_root
        url = connected_provider.build_url('', prefix=path.path, delimiter='/')
        aiohttpretty.register_json_uri('GET', url, status=200, body=body)
        result = await connected_provider.metadata(path)

        assert len(result) == 4
        assert result[0].name == 'level1'
        assert result[0].path == '/level1/'
        assert result[0].kind == 'folder'
        assert result[1].name == 'similar'
        assert result[1].path == '/similar'
        assert result[1].kind == 'file'
        assert result[2].name == 'similar.file'
        assert result[2].path == '/similar.file'
        assert result[2].kind == 'file'
        assert result[3].name == 'level1_empty'
        assert result[3].path == '/level1_empty/'
        assert result[3].kind == 'folder'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder_root_level1(self, connected_provider, folder_root_level1):
        path = WaterButlerPath('/level1/')
        body = folder_root_level1
        url = connected_provider.build_url('', prefix=path.path, delimiter='/')
        aiohttpretty.register_json_uri('GET', url, status=200, body=body)
        result = await connected_provider.metadata(path)

        assert len(result) == 1
        assert result[0].name == 'level2'
        assert result[0].path == '/level1/level2/'
        assert result[0].kind == 'folder'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder_root_level1_level2(self, connected_provider,
                                                      folder_root_level1_level2):
        path = WaterButlerPath('/level1/level2/')
        body = folder_root_level1_level2
        url = connected_provider.build_url('', prefix=path.path, delimiter='/')
        aiohttpretty.register_json_uri('GET', url, status=200, body=body)
        result = await connected_provider.metadata(path)

        assert len(result) == 1
        assert result[0].name == 'file2.txt'
        assert result[0].path == '/level1/level2/file2.txt'
        assert result[0].kind == 'file'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file_root_level1_level2_file2_txt(self, connected_provider,
                                                              file_root_level1_level2_file2_txt):
        path = WaterButlerPath('/level1/level2/file2.txt')
        url = connected_provider.build_url(path.path)
        aiohttpretty.register_uri('HEAD', url, status=200,
                        headers=file_root_level1_level2_file2_txt)
        result = await connected_provider.metadata(path)

        assert result.name == 'file2.txt'
        assert result.path == '/level1/level2/file2.txt'
        assert result.kind == 'file'
        assert result.content_type == 'text/plain'
        assert result.extra == {'hashes': {'md5': '44325d4f13b09f3769ede09d7c20a82c'}}

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder_root_level1_empty(self, connected_provider,
                                                        folder_root_level1_empty):
        path = WaterButlerPath('/level1_empty/')
        folder_url = connected_provider.build_url('', prefix=path.path, delimiter='/')
        folder_body = []
        file_url = connected_provider.build_url(path.path.rstrip('/'))
        aiohttpretty.register_json_uri('GET', folder_url, status=200, body=folder_body)
        aiohttpretty.register_uri('HEAD', file_url, status=200, headers=folder_root_level1_empty)
        result = await connected_provider.metadata(path)

        assert result == []

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file_root_similar(self, connected_provider, file_root_similar):
        path = WaterButlerPath('/similar')
        url = connected_provider.build_url(path.path)
        aiohttpretty.register_uri('HEAD', url, status=200, headers=file_root_similar)
        result = await connected_provider.metadata(path)

        assert result.name == 'similar'
        assert result.path == '/similar'
        assert result.kind == 'file'
        assert result.extra == {'hashes': {'md5': 'edfa12d00b779b4b37b81fe5b61b2b3f'}}

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file_root_similar_name(self, connected_provider,
                                                   file_root_similar_name):
        path = WaterButlerPath('/similar.file')
        url = connected_provider.build_url(path.path)
        aiohttpretty.register_uri('HEAD', url, status=200, headers=file_root_similar_name)
        result = await connected_provider.metadata(path)

        assert result.name == 'similar.file'
        assert result.path == '/similar.file'
        assert result.kind == 'file'
        assert result.extra == {'hashes': {'md5': 'edfa12d00b779b4b37b81fe5b61b2b3f'}}

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file_does_not_exist(self, connected_provider):
        path = WaterButlerPath('/does_not.exist')
        url = connected_provider.build_url(path.path)
        aiohttpretty.register_uri('HEAD', url, status=404)
        with pytest.raises(exceptions.MetadataError):
            await connected_provider.metadata(path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder_does_not_exist(self, connected_provider):
        path = WaterButlerPath('/does_not_exist/')
        folder_url = connected_provider.build_url('', prefix=path.path, delimiter='/')
        folder_body = []
        file_url = connected_provider.build_url(path.path.rstrip('/'))
        aiohttpretty.register_json_uri('GET', folder_url, status=200, body=folder_body)
        aiohttpretty.register_uri('HEAD', file_url, status=404)
        with pytest.raises(exceptions.MetadataError):
            await connected_provider.metadata(path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file_bad_content_type(self, connected_provider, file_metadata):
        item = file_metadata
        item['Content-Type'] = 'application/directory'
        path = WaterButlerPath('/does_not.exist')
        url = connected_provider.build_url(path.path)
        aiohttpretty.register_uri('HEAD', url, headers=item)
        with pytest.raises(exceptions.MetadataError):
            await connected_provider.metadata(path)


class TestV1ValidatePath:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_v1_validate_path(self, connected_provider):
        path = '/ab4x3'
        result = await connected_provider.validate_v1_path(path)

        assert result.path == path.strip('/')


class TestOperations:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_ensure_connection(self, provider, auth_json, mock_temp_key):
        token_url = cloud_settings.AUTH_URL

        aiohttpretty.register_json_uri('POST', token_url, body=auth_json)

        await provider._ensure_connection()
        assert aiohttpretty.has_call(method='POST', uri=token_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_ensure_connection_not_public(self, provider, auth_json, temp_url_key):

        token_url = cloud_settings.AUTH_URL
        provider.use_public = False
        internal_endpoint = "https://internal_fake_storage"

        aiohttpretty.register_json_uri('POST', token_url, body=auth_json)
        aiohttpretty.register_uri(
            'HEAD',
            internal_endpoint,
            status=204,
            headers={'X-Account-Meta-Temp-URL-Key': temp_url_key},
        )
        await provider._ensure_connection()
        assert aiohttpretty.has_call(method='POST', uri=token_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_ensure_connection_bad_url(self, provider, auth_json, endpoint):
        token_url = cloud_settings.AUTH_URL

        aiohttpretty.register_json_uri('POST', token_url, body=auth_json)
        aiohttpretty.register_uri(
            'HEAD',
            endpoint,
            status=204,
            headers={'bad': 'yes'}
        )
        with pytest.raises(exceptions.ProviderError) as e:
            await provider._ensure_connection()

        assert e.value.code == 503
        assert aiohttpretty.has_call(method='POST', uri=token_url)
        assert aiohttpretty.has_call(method='HEAD', uri=endpoint)

    def test_can_duplicate_names(self, connected_provider):
        assert connected_provider.can_duplicate_names() is False

    def test_can_intra_copy(self, connected_provider):
        assert connected_provider.can_intra_copy(connected_provider)

    def test_can_intra_move(self, connected_provider):
        assert connected_provider.can_intra_move(connected_provider)
