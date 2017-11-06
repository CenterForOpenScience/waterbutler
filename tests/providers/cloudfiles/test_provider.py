import json
import hashlib

import furl
import pytest
import aiohttp
import aiohttpretty

from tests.utils import MockCoroutine
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath
from waterbutler.providers.cloudfiles import settings as cloud_settings

from waterbutler.providers.cloudfiles.metadata import (CloudFilesHeaderMetadata,
                                                       CloudFilesRevisonMetadata)

from tests.providers.cloudfiles.fixtures import (
    auth,
    settings,
    credentials,
    token,
    endpoint,
    temp_url_key,
    mock_time,
    file_content,
    file_content_100_bytes,
    file_stream,
    file_stream_100_bytes,
    file_like,
    mock_temp_key,
    provider,
    connected_provider,
    file_metadata,
    auth_json,
    folder_root,
    folder_root_empty,
    folder_root_level1_empty,
    file_root_similar_name,
    file_root_similar,
    file_header_metadata,
    folder_root_level1,
    folder_root_level1_level2,
    file_root_level1_level2_file2_txt,
    container_header_metadata_with_verision_location,
    container_header_metadata_without_verision_location,
    revision_list
)


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
    async def test_download_revision(self,
                                     connected_provider,
                                     container_header_metadata_with_verision_location):
        body = b'dearly-beloved'
        path = WaterButlerPath('/lets-go-crazy')
        url = connected_provider.sign_url(path)
        aiohttpretty.register_uri('GET', url, body=body, auto_length=True)

        container_url = connected_provider.build_url('')
        aiohttpretty.register_uri('HEAD',
                                  container_url,
                                  headers=container_header_metadata_with_verision_location)

        version_name = '{:03x}'.format(len(path.name)) + path.name + '/'
        revision_url = connected_provider.build_url(version_name, container='versions-container', )
        aiohttpretty.register_uri('GET', revision_url, body=body, auto_length=True)


        result = await connected_provider.download(path, version=version_name)
        content = await result.read()

        assert content == body

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_accept_url(self, connected_provider):
        body = b'dearly-beloved'
        path = WaterButlerPath('/lets-go-crazy')

        url = connected_provider.sign_url(path)
        parsed_url = furl.furl(url)
        parsed_url.args['filename'] = 'lets-go-crazy'

        result = await connected_provider.download(path, accept_url=True)

        assert result == parsed_url.url
        aiohttpretty.register_uri('GET', url, body=body)
        response = await aiohttp.request('GET', url)
        content = await response.read()
        assert content == body

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
    async def test_upload(self,
                          connected_provider,
                          file_content,
                          file_stream,
                          file_header_metadata):
        path = WaterButlerPath('/similar.file')
        content_md5 = hashlib.md5(file_content).hexdigest()
        metadata_url = connected_provider.build_url(path.path)
        url = connected_provider.sign_url(path, 'PUT')
        aiohttpretty.register_uri('HEAD',
                                  metadata_url,
                                  responses=[
                                      {'status': 404},
                                      {'headers': file_header_metadata}
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
    async def test_chunked_upload(self,
                          connected_provider,
                          file_stream_100_bytes,
                          file_header_metadata):
        path = WaterButlerPath('/similar.file')
        metadata_url = connected_provider.build_url(path.path)
        aiohttpretty.register_uri('HEAD',
                                  metadata_url,
                                  responses=[
                                      {'status': 404},
                                      {'headers': file_header_metadata}
                                  ]
                                  )

        for i in range(0, 10):
            url = connected_provider.sign_url(path, 'PUT', segment_num=str(i).zfill(5))
            aiohttpretty.register_uri('PUT', url, status=200)

        url = connected_provider.build_url(path.path)
        aiohttpretty.register_uri('PUT', url, status=200)

        connected_provider.SEGMENT_SIZE = 10  # for testing
        metadata, created = await connected_provider.chunked_upload(file_stream_100_bytes, path)

        assert created is True
        assert metadata.kind == 'file'
        assert aiohttpretty.has_call(method='HEAD', uri=metadata_url)  # check metadata was called
        assert aiohttpretty.has_call(method='PUT', uri=url)  # check manifest was uploaded
        for i in range(0, 10):  # check all 10 segments were uploaded
            url = connected_provider.sign_url(path, 'PUT', segment_num=str(i).zfill(5))
            assert aiohttpretty.has_call(method='PUT', uri=url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_segment(self, connected_provider, file_stream_100_bytes):

        path = WaterButlerPath('/similar.file')

        connected_provider.chunked_upload = MockCoroutine()
        connected_provider.SEGMENT_SIZE = 10  # for test, we dont want to load a 5GB fixture.
        await connected_provider.upload(file_stream_100_bytes, path)

        assert connected_provider.chunked_upload.called_with(file_stream_100_bytes, path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_create_folder(self, connected_provider, file_header_metadata):
        path = WaterButlerPath('/foo/', folder=True)
        metadata_url = connected_provider.build_url(path.path)
        url = connected_provider.sign_url(path, 'PUT')
        aiohttpretty.register_uri('PUT', url, status=200)
        aiohttpretty.register_uri('HEAD', metadata_url, headers=file_header_metadata)

        metadata = await connected_provider.create_folder(path)

        assert metadata.kind == 'folder'
        assert aiohttpretty.has_call(method='PUT', uri=url)
        assert aiohttpretty.has_call(method='HEAD', uri=metadata_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_check_none(self, connected_provider,
                                    file_content, file_stream, file_header_metadata):
        path = WaterButlerPath('/similar.file')
        content_md5 = hashlib.md5(file_content).hexdigest()
        metadata_url = connected_provider.build_url(path.path)
        url = connected_provider.sign_url(path, 'PUT')
        aiohttpretty.register_uri('HEAD', metadata_url, status=404, headers=file_header_metadata)
        aiohttpretty.register_uri('PUT', url, status=200,
                                  headers={'ETag': '"{}"'.format(content_md5)})
        metadata, created = await connected_provider.upload(
            file_stream, path, check_created=False, fetch_metadata=False)

        assert created is None
        assert metadata is None
        assert aiohttpretty.has_call(method='PUT', uri=url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_checksum_mismatch(self,
                                            connected_provider,
                                            file_stream,
                                            file_header_metadata):
        path = WaterButlerPath('/similar.file')
        metadata_url = connected_provider.build_url(path.path)
        url = connected_provider.sign_url(path, 'PUT')
        aiohttpretty.register_uri('HEAD', metadata_url, status=404, headers=file_header_metadata)

        aiohttpretty.register_uri('PUT', url, status=200, headers={'ETag': '"Bogus MD5"'})

        with pytest.raises(exceptions.UploadChecksumMismatchError):
            await connected_provider.upload(file_stream, path)

        assert aiohttpretty.has_call(method='PUT', uri=url)
        assert aiohttpretty.has_call(method='HEAD', uri=metadata_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_root_without_confirm(self, connected_provider, folder_root_empty, file_header_metadata):
        path = WaterButlerPath('/')

        with pytest.raises(exceptions.DeleteError):
            await connected_provider.delete(path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_folder(self, connected_provider, folder_root_empty, file_header_metadata):

        path = WaterButlerPath('/delete/')
        query = {'prefix': path.path}
        url = connected_provider.build_url('',  **query)
        body = json.dumps(folder_root_empty).encode('utf-8')

        delete_query = {'bulk-delete': ''}
        delete_url_folder = connected_provider.build_url(path.name, **delete_query)
        delete_url_content = connected_provider.build_url('', **delete_query)

        file_url = connected_provider.build_url(path.path)
        aiohttpretty.register_uri('GET', url, body=body)
        aiohttpretty.register_uri('HEAD', file_url, headers=file_header_metadata)

        aiohttpretty.register_uri('DELETE', delete_url_content, status=200)
        aiohttpretty.register_uri('DELETE', delete_url_folder, status=204)

        await connected_provider.delete(path)

        assert aiohttpretty.has_call(method='DELETE', uri=delete_url_content)
        assert aiohttpretty.has_call(method='DELETE', uri=delete_url_folder)

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
    async def test_intra_copy(self, connected_provider, file_header_metadata):
        src_path = WaterButlerPath('/delete.file')
        dest_path = WaterButlerPath('/folder1/delete.file')

        dest_url = connected_provider.build_url(dest_path.path)

        aiohttpretty.register_uri('HEAD', dest_url, headers=file_header_metadata)
        aiohttpretty.register_uri('PUT', dest_url, status=201)

        result = await connected_provider.intra_copy(connected_provider, src_path, dest_path)

        assert result[0].path == '/folder1/delete.file'
        assert result[0].name == 'delete.file'
        assert result[0].etag == '8a839ea73aaa78718e27e025bdc2c767'


class TestRevisions:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_revisions(self,
                             connected_provider,
                             container_header_metadata_with_verision_location,
                             revision_list,
                             file_header_metadata):

        path = WaterButlerPath('/file.txt')

        container_url = connected_provider.build_url('')
        aiohttpretty.register_uri('HEAD',
                                  container_url,
                                  headers=container_header_metadata_with_verision_location)

        query = {'prefix': '{:03x}'.format(len(path.name)) + path.name + '/'}
        revision_url = connected_provider.build_url('', container='versions-container', **query)
        aiohttpretty.register_json_uri('GET', revision_url, body=revision_list)

        metadata_url = connected_provider.build_url(path.path)
        aiohttpretty.register_uri('HEAD', metadata_url, status=200, headers=file_header_metadata)


        result = await connected_provider.revisions(path)

        assert type(result) == list
        assert len(result) == 4
        assert type(result[0]) == CloudFilesRevisonMetadata
        assert result[0].name == 'file.txt'
        assert result[1].name == '007123.csv/1507756317.92019'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_revision_metadata(self,
                                     connected_provider,
                                     container_header_metadata_with_verision_location,
                                     file_header_metadata):

        path = WaterButlerPath('/file.txt')
        container_url = connected_provider.build_url('')
        aiohttpretty.register_uri('HEAD', container_url,
                                  headers=container_header_metadata_with_verision_location)

        version_name = '{:03x}'.format(len(path.name)) + path.name + '/'
        revision_url = connected_provider.build_url(version_name + '1507756317.92019',
                                                    container='versions-container')
        aiohttpretty.register_json_uri('HEAD', revision_url, body=file_header_metadata)

        result = await connected_provider.metadata(path, version=version_name + '1507756317.92019')

        assert type(result) == CloudFilesHeaderMetadata
        assert result.name == 'file.txt'
        assert result.path == '/file.txt'
        assert result.kind == 'file'

class TestMetadata:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder_root_empty(self, connected_provider, folder_root_empty):
        path = WaterButlerPath('/')
        body = json.dumps(folder_root_empty).encode('utf-8')
        url = connected_provider.build_url(path.path, prefix=path.path, delimiter='/')
        aiohttpretty.register_uri('GET', url, status=200, body=body)
        result = await connected_provider.metadata(path)

        assert len(result) == 0
        assert result == []

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder_root(self, connected_provider, folder_root):
        path = WaterButlerPath('/')
        body = json.dumps(folder_root).encode('utf-8')
        url = connected_provider.build_url('', prefix=path.path, delimiter='/')
        aiohttpretty.register_uri('GET', url, status=200, body=body)
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
    async def test_metadata_404(self, connected_provider, folder_root_level1):
        path = WaterButlerPath('/level1/')
        body = json.dumps(folder_root_level1).encode('utf-8')
        url = connected_provider.build_url('', prefix=path.path, delimiter='/')
        aiohttpretty.register_uri('GET', url, status=200, body=b'')
        connected_provider._metadata_item = MockCoroutine(return_value=None)

        with pytest.raises(exceptions.MetadataError):
            await connected_provider.metadata(path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder_root_level1(self, connected_provider, folder_root_level1):
        path = WaterButlerPath('/level1/')
        body = json.dumps(folder_root_level1).encode('utf-8')
        url = connected_provider.build_url('', prefix=path.path, delimiter='/')
        aiohttpretty.register_uri('GET', url, status=200, body=body)
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
        body = json.dumps(folder_root_level1_level2).encode('utf-8')
        url = connected_provider.build_url('', prefix=path.path, delimiter='/')
        aiohttpretty.register_uri('GET', url, status=200, body=body)
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
        folder_body = json.dumps([]).encode('utf-8')
        file_url = connected_provider.build_url(path.path.rstrip('/'))
        aiohttpretty.register_uri('GET', folder_url, status=200, body=folder_body)
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
        folder_body = json.dumps([]).encode('utf-8')
        file_url = connected_provider.build_url(path.path.rstrip('/'))
        aiohttpretty.register_uri('GET', folder_url, status=200, body=folder_body)
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
    async def test_no_version_location(self,
                                       connected_provider,
                                       container_header_metadata_without_verision_location):

        path = WaterButlerPath('/file.txt')

        container_url = connected_provider.build_url('')
        aiohttpretty.register_uri('HEAD',
                                  container_url,
                                  headers=container_header_metadata_without_verision_location)

        with pytest.raises(exceptions.MetadataError):
            await connected_provider.revisions(path)

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
