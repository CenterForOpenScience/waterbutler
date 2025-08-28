import pytest


import aiohttpretty

from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath
from waterbutler.providers.azureblobstorage.provider import AzureBlobStorageProvider

from tests.providers.azureblobstorage.fixtures import (
    auth, credentials, settings, provider, file_content,
    file_stream, large_file_stream, large_file_content,
    file_like, large_file_like, blob_list_xml, root_list_xml,
    empty_folder_list_xml, folder_placeholder_headers,
    folder_exists_xml, folder_validation_response_xml,
    error_authentication_failed_xml, error_authorization_failure_xml,
    error_not_found_xml, error_internal_error_xml, folder_not_found_response_xml
)


class TestValidatePath:
    """Test path validation and parsing"""

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_file(self, provider):
        """Test file path validation"""
        blob_url = provider.build_url(provider.container, provider._get_blob_path('test-file.txt'))
        
        aiohttpretty.register_uri(
            'HEAD',
            blob_url,
            status=200,
            headers={'Content-Type': 'text/plain', 'Content-Length': '100'}
        )
        
        wb_path_v1 = await provider.validate_v1_path('/test-file.txt')
        wb_path_v0 = await provider.validate_path('/test-file.txt')

        assert wb_path_v1 == wb_path_v0
        assert not wb_path_v1.is_dir

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty  
    async def test_validate_v1_path_folder(self, provider, folder_validation_response_xml):
        """Test folder path validation"""
        list_url = provider.build_url(provider.container)
        
        aiohttpretty.register_uri(
            'GET',
            list_url,
            params={
                'restype': 'container',
                'comp': 'list',
                'prefix': 'test/test-folder/',
                'delimiter': '/',
                'maxresults': '1'
            },
            body=folder_validation_response_xml,
            headers={'Content-Type': 'application/xml'},
            status=200
        )
        
        wb_path_v1 = await provider.validate_v1_path('/test-folder/')
        wb_path_v0 = await provider.validate_path('/test-folder/')

        assert wb_path_v1.is_dir
        assert wb_path_v0.is_dir
        assert wb_path_v1.path == 'test-folder/'
        assert wb_path_v0.path == 'test-folder/'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty  
    async def test_validate_v1_path_folder_not_found(self, provider, folder_not_found_response_xml):
        """Test folder path validation for non-existent folder"""
        list_url = provider.build_url(provider.container)
        
        aiohttpretty.register_uri(
            'GET',
            list_url,
            params={
                'restype': 'container',
                'comp': 'list',
                'prefix': 'test/nonexistent-folder/',
                'delimiter': '/',
                'maxresults': '1'
            },
            body=folder_not_found_response_xml,
            headers={'Content-Type': 'application/xml'},
            status=200
        )

        with pytest.raises(exceptions.NotFoundError):
            await provider.validate_v1_path('/nonexistent-folder/')

        wb_path_v0 = await provider.validate_path('/nonexistent-folder/')
        assert wb_path_v0.is_dir
        assert wb_path_v0.path == 'nonexistent-folder/'


class TestDownload:
    """Test file download operations"""

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_file(self, provider):
        """Test basic file download"""
        blob_url = provider.build_url(provider.container, provider._get_blob_path('test-file.txt'))

        aiohttpretty.register_uri(
            'GET',
            blob_url,
            body=b'test file content',
            headers={
                'Content-Type': 'text/plain',
                'Content-Length': '17',
                'ETag': '"0x8D1A2B3C4D5E6F7"',
                'Last-Modified': 'Mon, 15 Jul 2025 07:28:00 GMT'
            },
            status=200
        )

        path = WaterButlerPath('/test-file.txt')
        result = await provider.download(path)
        content = await result.read()
        assert content == b'test file content'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty  
    async def test_download_range(self, provider):
        """Test download with byte range"""
        blob_url = provider.build_url(provider.container, provider._get_blob_path('test-file.txt'))

        aiohttpretty.register_uri(
            'GET',
            blob_url,
            body=b'te',
            headers={
                'Content-Range': 'bytes 0-1/17',
                'Content-Type': 'text/plain',
                'Content-Length': '2'
            },
            status=206
        )

        path = WaterButlerPath('/test-file.txt')
        result = await provider.download(path, range=(0, 1))
        content = await result.read()
        assert content == b'te'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_unauthorized(self, provider, error_authentication_failed_xml):
        """Test download with invalid OAuth token"""
        blob_url = provider.build_url(provider.container, provider._get_blob_path('test-file.txt'))

        aiohttpretty.register_uri(
            'GET',
            blob_url,
            status=401,
            headers={'Content-Type': 'application/xml'},
            body=error_authentication_failed_xml
        )

        path = WaterButlerPath('/test-file.txt')
        with pytest.raises(exceptions.DownloadError):
            await provider.download(path)


class TestUpload:
    """Test file upload operations"""

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_file(self, provider, file_stream):
        """Test basic file upload"""
        blob_url = provider.build_url(provider.container, provider._get_blob_path('test-upload.txt'))

        aiohttpretty.register_uri(
            'HEAD',
            blob_url,
            responses=[
                {'status': 404},
                {
                    'status': 200,
                    'headers': {
                        'Content-Type': 'application/octet-stream',
                        'Content-Length': '39',
                        'ETag': '"0x8D1A2B3C4D5E6F8"',
                        'Last-Modified': 'Mon, 15 Jul 2025 08:00:00 GMT'
                    }
                }
            ]
        )

        aiohttpretty.register_uri(
            'PUT',
            blob_url,
            headers={
                'ETag': '"0x8D1A2B3C4D5E6F8"',
                'Last-Modified': 'Mon, 15 Jul 2025 08:00:00 GMT'
            },
            status=201
        )

        path = WaterButlerPath('/test-upload.txt')
        metadata, created = await provider.upload(file_stream, path)
        
        assert created is True
        assert metadata.name == 'test-upload.txt'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_overwrite(self, provider, file_stream):
        """Test upload overwriting existing file"""
        blob_url = provider.build_url(provider.container, provider._get_blob_path('existing-file.txt'))

        aiohttpretty.register_uri(
            'HEAD',
            blob_url,
            responses=[
                {
                    'status': 200,
                    'headers': {
                        'ETag': '"0x8D1A2B3C4D5E6F7"',
                        'Content-Length': '1024',
                        'Last-Modified': 'Mon, 15 Jul 2025 07:00:00 GMT'
                    }
                },
                {
                    'status': 200,
                    'headers': {
                        'Content-Type': 'application/octet-stream',
                        'Content-Length': '39',
                        'ETag': '"0x8D1A2B3C4D5E6F9"',
                        'Last-Modified': 'Mon, 15 Jul 2025 08:30:00 GMT'
                    }
                }
            ]
        )

        aiohttpretty.register_uri(
            'PUT',
            blob_url,
            headers={
                'ETag': '"0x8D1A2B3C4D5E6F9"',
                'Last-Modified': 'Mon, 15 Jul 2025 08:30:00 GMT'
            },
            status=201
        )

        path = WaterButlerPath('/existing-file.txt')
        metadata, created = await provider.upload(file_stream, path, conflict='replace')
        
        assert created is False
        assert metadata.name == 'existing-file.txt'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_large_file_simplified(self, provider, large_file_stream):
        """Test large file upload"""
        blob_url = provider.build_url(provider.container, provider._get_blob_path('large-file.bin'))

        aiohttpretty.register_uri(
            'HEAD',
            blob_url,
            responses=[
                {'status': 404},
                {
                    'status': 200,
                    'headers': {
                        'Content-Type': 'application/octet-stream',
                        'Content-Length': '10485760',  # 10MB
                        'ETag': '"0x8D1A2B3C4D5E6FA"',
                        'Last-Modified': 'Mon, 15 Jul 2025 09:00:00 GMT'
                    }
                }
            ]
        )

        aiohttpretty.register_uri(
            'PUT',
            blob_url,
            headers={
                'ETag': '"0x8D1A2B3C4D5E6FA"',
                'Last-Modified': 'Mon, 15 Jul 2025 09:00:00 GMT'
            },
            status=201
        )

        aiohttpretty.register_uri(
            'PUT',
            f'{blob_url}?comp=block&blockid=*',
            status=201,
            match_querystring=False
        )

        aiohttpretty.register_uri(
            'PUT',
            f'{blob_url}?comp=blocklist',
            headers={'ETag': '"0x8D1A2B3C4D5E6FA"'},
            status=201
        )

        path = WaterButlerPath('/large-file.bin')
        metadata, created = await provider.upload(large_file_stream, path)
        
        assert created is True
        assert metadata.name == 'large-file.bin'


class TestMetadata:
    """Test metadata operations"""

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file(self, provider):
        """Test getting file metadata"""
        blob_url = provider.build_url(provider.container, provider._get_blob_path('test-file.txt'))

        aiohttpretty.register_uri(
            'HEAD',
            blob_url,
            headers={
                'Content-Type': 'text/plain',
                'Content-Length': '1024',
                'ETag': '"0x8D1A2B3C4D5E6F7"',
                'Last-Modified': 'Mon, 15 Jul 2025 07:28:00 GMT'
            },
            status=200
        )

        path = WaterButlerPath('/test-file.txt')
        metadata = await provider.metadata(path)
        
        assert metadata.name == 'test-file.txt'
        assert metadata.size == 1024
        assert metadata.content_type == 'text/plain'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder(self, provider, blob_list_xml):
        """Test listing folder contents"""
        list_url = provider.build_url(provider.container)

        aiohttpretty.register_uri(
            'GET',
            list_url,
            params={'restype': 'container', 'comp': 'list', 'prefix': 'test/folder/', 'delimiter': '/'},
            body=blob_list_xml,
            headers={'Content-Type': 'application/xml'},
            status=200
        )

        path = WaterButlerPath('/folder/')
        metadata_list = await provider.metadata(path)
        
        assert isinstance(metadata_list, list)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_root(self, provider, root_list_xml):
        """Test listing root container contents"""
        list_url = provider.build_url(provider.container)

        aiohttpretty.register_uri(
            'GET',
            list_url,
            params={'restype': 'container', 'comp': 'list', 'prefix': 'test/', 'delimiter': '/'},
            body=root_list_xml,
            headers={'Content-Type': 'application/xml'},
            status=200
        )

        path = WaterButlerPath('/')
        metadata_list = await provider.metadata(path)
        assert isinstance(metadata_list, list)


class TestDelete:
    """Test delete operations"""

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_file(self, provider):
        """Test deleting a file"""
        blob_url = provider.build_url(provider.container, provider._get_blob_path('test-file.txt'))

        aiohttpretty.register_uri('DELETE', blob_url, status=202)

        path = WaterButlerPath('/test-file.txt')
        await provider.delete(path)
        
        assert aiohttpretty.has_call(method='DELETE', uri=blob_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_not_found(self, provider, error_not_found_xml):
        """Test deleting non-existent file"""
        blob_url = provider.build_url(provider.container, provider._get_blob_path('missing-file.txt'))

        aiohttpretty.register_uri(
            'DELETE',
            blob_url,
            status=404,
            headers={'Content-Type': 'application/xml'},
            body=error_not_found_xml
        )

        path = WaterButlerPath('/missing-file.txt')
        await provider.delete(path)


class TestCreateFolder:
    """Test folder creation"""

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_create_folder(self, provider, empty_folder_list_xml, folder_placeholder_headers):
        """Test creating a folder (placeholder blob)"""
        list_url = provider.build_url(provider.container)

        aiohttpretty.register_uri(
            'GET',
            list_url,
            params={'restype': 'container', 'comp': 'list', 'prefix': 'test/newfolder/', 'delimiter': '/'},
            body=empty_folder_list_xml,
            headers={'Content-Type': 'application/xml'},
            status=200
        )

        placeholder_url = provider.build_url(provider.container, provider._get_blob_path('newfolder/.osfkeep'))
        
        aiohttpretty.register_uri(
            'HEAD',
            provider.build_url(provider.container, provider._get_blob_path('newfolder')),
            status=404
        )
        
        aiohttpretty.register_uri(
            'PUT',
            placeholder_url,
            headers=folder_placeholder_headers,
            status=201
        )

        path = WaterButlerPath('/newfolder/')
        metadata = await provider.create_folder(path)
        assert metadata.name == 'newfolder'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_create_folder_already_exists(self, provider, folder_exists_xml):
        """Test creating a folder that already exists"""
        list_url = provider.build_url(provider.container)

        aiohttpretty.register_uri(
            'GET',
            list_url,
            params={'restype': 'container', 'comp': 'list', 'prefix': 'test/existingfolder/', 'delimiter': '/'},
            body=folder_exists_xml,
            headers={'Content-Type': 'application/xml'},
            status=200
        )

        path = WaterButlerPath('/existingfolder/')
        with pytest.raises(exceptions.FolderNamingConflict):
            await provider.create_folder(path)


class TestAuthentication:
    """Test authentication and provider initialization"""

    def test_provider_initialization(self, auth, credentials, settings):
        """Test provider initialization with OAuth credentials"""
        provider_instance = AzureBlobStorageProvider(auth, credentials, settings)

        assert provider_instance.account_name == settings['account_name']
        assert provider_instance.container == settings['container']
        assert provider_instance.auth_token == credentials['token']

    def test_provider_validation_missing_credentials(self, auth, settings):
        """Test provider fails with missing credentials"""
        with pytest.raises(ValueError, match="token is required"):
            AzureBlobStorageProvider(auth, {}, settings)

    def test_provider_validation_missing_settings(self, auth, credentials):
        """Test provider fails with missing settings"""
        with pytest.raises(ValueError, match="account_name is required"):
            AzureBlobStorageProvider(auth, credentials, {'container': 'test'})

        with pytest.raises(ValueError, match="container is required"):
            AzureBlobStorageProvider(auth, credentials, {'account_name': 'test'})


class TestErrorHandling:
    """Test core error scenarios"""

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_expired_token(self, provider, error_authentication_failed_xml):
        """Test handling of expired OAuth token"""
        blob_url = provider.build_url(provider.container, provider._get_blob_path('test-file.txt'))

        aiohttpretty.register_uri(
            'GET',
            blob_url,
            status=401,
            headers={'Content-Type': 'application/xml'},
            body=error_authentication_failed_xml
        )

        path = WaterButlerPath('/test-file.txt')
        with pytest.raises(exceptions.DownloadError):
            await provider.download(path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_insufficient_permissions(self, provider, error_authorization_failure_xml):
        """Test handling of insufficient permissions"""
        blob_url = provider.build_url(provider.container, provider._get_blob_path('test-file.txt'))

        aiohttpretty.register_uri(
            'DELETE',
            blob_url,
            status=403,
            headers={'Content-Type': 'application/xml'},
            body=error_authorization_failure_xml
        )

        path = WaterButlerPath('/test-file.txt')
        with pytest.raises(exceptions.DeleteError):
            await provider.delete(path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_server_error(self, provider, error_internal_error_xml):
        """Test handling of server errors"""
        blob_url = provider.build_url(provider.container, provider._get_blob_path('test-file.txt'))

        aiohttpretty.register_uri(
            'GET',
            blob_url,
            status=500,
            headers={'Content-Type': 'application/xml'},
            body=error_internal_error_xml
        )

        path = WaterButlerPath('/test-file.txt')
        with pytest.raises(exceptions.DownloadError):
            await provider.download(path)
