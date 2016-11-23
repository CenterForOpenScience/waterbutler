import json
import os

import pytest

import aiohttpretty

import logging

from waterbutler.core import exceptions
from waterbutler.providers.onedrive.metadata import OneDriveFileMetadata
from waterbutler.providers.onedrive.provider import OneDrivePath

logger = logging.getLogger(__name__)


class TestValidatePath:
    @pytest.mark.asyncio
    @pytest.mark.parametrize('settings', [{'folder': '0'}])
    async def test_root_path(self, provider):
        path = await provider.validate_v1_path('/')
        assert isinstance(path, OneDrivePath)
        assert path.is_root
        assert path.name == ''
        assert path.full_path == '/'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('settings', [{'folder': '75BFE374EBEB1211'}, {'folder': '75BFE374EBEB1211!118'}])
    async def test_root_relative_path(self, provider, folder_sub_response, settings):
        folder_url_by_id = provider._build_content_url(settings['folder'].strip('/'))
        _url_parts = (
            folder_sub_response['parentReference']['path'].replace('/drive/root:', ''),
            folder_sub_response['name']
        )
        folder_url = provider._build_root_url('/drive/root:', *_url_parts)
        aiohttpretty.register_json_uri('GET', folder_url_by_id, body=folder_sub_response)
        aiohttpretty.register_json_uri('GET', folder_url, body=folder_sub_response)

        path = await provider.validate_v1_path('/')

        assert isinstance(path, OneDrivePath)
        assert path.is_root
        assert path.is_folder
        assert path.name == ''
        expected_folder_full_path = '/' + os.path.join(*_url_parts) + '/'
        assert path.full_path == expected_folder_full_path


class TestUpload:
    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_upload(self, provider, folder_sub_response, file_sub_response, file_stream, not_found_error_response):
        path = OneDrivePath.from_response(folder_sub_response).child(file_sub_response['name'])

        file_metadata_url = provider._build_content_url(file_sub_response['id'], expand='children')
        aiohttpretty.register_json_uri('GET', file_metadata_url, body=not_found_error_response, status=404)
        folder_metadata_url = provider._build_content_url(path.parent.identifier, expand='children')
        aiohttpretty.register_json_uri('GET', folder_metadata_url, body=folder_sub_response, status=200)

        file_upload_url = provider._build_content_url(path.parent.identifier, 'children', path.name, 'content')
        aiohttpretty.register_json_uri('PUT', file_upload_url, body=file_sub_response, status=201)

        assert path.identifier is None

        file_metadata, created = await provider.upload(file_stream, path)

        assert aiohttpretty.has_call(method='PUT', uri=file_upload_url)
        assert not aiohttpretty.has_call(method='GET', uri=file_metadata_url)
        assert not aiohttpretty.has_call(method='GET', uri=folder_metadata_url)

        expected = OneDriveFileMetadata(file_sub_response, path)
        assert file_metadata == expected
        assert created is True


    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_upload_update(self, provider, file_stream, file_sub_response):
        path = OneDrivePath.from_response(file_sub_response)

        delete_url = provider._build_content_url(file_sub_response['id'])
        aiohttpretty.register_json_uri('DELETE', delete_url, status=200)
        metadata_url = provider._build_content_url(file_sub_response['id'], expand='children')
        aiohttpretty.register_json_uri('GET', metadata_url, body=file_sub_response, status=200)
        file_upload_url = provider._build_content_url(path.parent.identifier, 'children', path.name, 'content')
        aiohttpretty.register_json_uri('PUT', file_upload_url, body=file_sub_response, status=201)

        file_metadata, created = await provider.upload(file_stream, path, conflict='replace')

        assert aiohttpretty.has_call(method='GET', uri=metadata_url)
        assert aiohttpretty.has_call(method='PUT', uri=file_upload_url)
        assert not aiohttpretty.has_call(method='DELETE', uri=delete_url)
        assert created is False

        expected = OneDriveFileMetadata(file_sub_response, path)
        assert expected == file_metadata


    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_upload_exists(self, provider, file_stream, file_sub_response):
        path = OneDrivePath.from_response(file_sub_response)

        metadata_url = provider._build_content_url(file_sub_response['id'], expand='children')
        aiohttpretty.register_json_uri('GET', metadata_url, body=file_sub_response, status=200)

        with pytest.raises(exceptions.NamingConflict) as e:
            file_metadata, created = await provider.upload(file_stream, path, conflict='warn')

        assert aiohttpretty.has_call(method='GET', uri=metadata_url)
        assert e.value.message == 'Cannot complete action: file or folder "{}" already exists in this location'\
            .format(path.name)

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_upload_rename(self, provider, file_stream, file_sub_response, file_rename_sub_response, not_found_error_response):
        path = OneDrivePath.from_response(file_sub_response)
        new_path = OneDrivePath.from_response(file_sub_response)
        new_path.increment_name()

        metadata_url = provider._build_content_url(file_sub_response['id'], expand='children')
        aiohttpretty.register_json_uri('GET', metadata_url, body=file_sub_response, status=200)

        new_file_metadata_url = provider._build_root_url('/drive/root:', new_path.full_path)
        aiohttpretty.register_json_uri('GET', new_file_metadata_url, body=not_found_error_response, status=404)
        new_file_upload_url = provider._build_content_url(path.parent.identifier, 'children', new_path.name, 'content')
        aiohttpretty.register_json_uri('PUT', new_file_upload_url, body=file_rename_sub_response, status=201)

        file_metadata, created = await provider.upload(file_stream, path, conflict='rename')

        assert aiohttpretty.has_call(method='GET', uri=metadata_url)
        assert aiohttpretty.has_call(method='PUT', uri=new_file_upload_url)
        assert created is True

        assert new_path == file_metadata._path


class TestMetadata:
    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    @pytest.mark.parametrize('settings', [{'folder': '0'}])
    async def test_metadata_root(self, provider, folder_root_response):
        path = OneDrivePath('/')

        metadata_url = provider._build_content_url('root', expand='children')
        aiohttpretty.register_json_uri('GET', metadata_url, body=folder_root_response, status=200)

        res = await provider.metadata(path)

        assert len(res) == len(folder_root_response['children'])

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_metadata_relative_root(self, provider, folder_sub_response):
        path = OneDrivePath('/', _ids=(folder_sub_response['id'], ))

        metadata_url = provider._build_content_url(folder_sub_response['id'], expand='children')
        aiohttpretty.register_json_uri('GET', metadata_url, body=folder_sub_response, status=200)

        res = await provider.metadata(path)

        assert isinstance(res, (list, tuple))
        assert len(res) == len(folder_sub_response['children'])

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_metadata_file(self, provider, file_root_response):
        path = OneDrivePath.from_response(file_root_response)
        metadata_url = provider._build_content_url(file_root_response['id'], expand='children')

        aiohttpretty.register_json_uri('GET', metadata_url, body=file_root_response, status=200)

        res = await provider.metadata(path)

        assert isinstance(res, OneDriveFileMetadata)
        assert res.materialized_path == path.materialized_path

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    @pytest.mark.parametrize('settings', [{'folder': '0'}])
    async def test_metadata_sub_folder(self, provider, folder_sub_response):
        path = OneDrivePath.from_response(folder_sub_response)

        metadata_url = provider._build_content_url(folder_sub_response['id'], expand='children')

        aiohttpretty.register_json_uri('GET', metadata_url, body=folder_sub_response, status=200)

        res = await provider.metadata(path)

        assert isinstance(res, (list, tuple))
        assert len(res) == len(folder_sub_response['children'])

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_metadata_not_found(self, provider, not_found_error_response):
        file_id = '435a3s567fdd433!111'
        path = OneDrivePath('/not-found.jpg', _ids=('0', file_id))

        metadata_url = provider._build_content_url(file_id, expand='children')

        aiohttpretty.register_json_uri('GET', metadata_url, body=not_found_error_response, status=404)

        with pytest.raises(exceptions.MetadataError) as e:
            await provider.metadata(path)

        assert e.value.code == 404
        assert e.value.message == json.dumps(not_found_error_response)


class TestDelete:
    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_not_found(self, provider, folder_sub_response):
        path = OneDrivePath.from_response(folder_sub_response)
        path = path.child('not-exist.jpg')

        with pytest.raises(exceptions.NotFoundError) as e:
            await provider.delete(path)

        assert e.value.code == 404
        assert e.value.message == 'Could not retrieve file or directory {}'.format(path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_root(self, provider, folder_sub_response):
        path = OneDrivePath('/', _ids=(folder_sub_response['id'],), prepend='/{}'.format(folder_sub_response['name']))

        root_delete_url = provider._build_content_url(folder_sub_response['id'])
        root_metadata_url = provider._build_content_url(folder_sub_response['id'], expaned='children')
        aiohttpretty.register_json_uri('DELETE', root_delete_url, status=204)
        aiohttpretty.register_json_uri('GET', root_metadata_url, body=folder_sub_response, status=200)
        child1_delete_url = provider._build_content_url(folder_sub_response['children'][0]['id'])
        aiohttpretty.register_json_uri('DELETE', child1_delete_url, status=204)
        child2_delete_url = provider._build_content_url(folder_sub_response['children'][1]['id'])
        aiohttpretty.register_json_uri('DELETE', child2_delete_url, status=204)
        child3_delete_url = provider._build_content_url(folder_sub_response['children'][2]['id'])
        aiohttpretty.register_json_uri('DELETE', child3_delete_url, status=204)

        await provider.delete(path, confirm_delete=1)

        assert aiohttpretty.has_call(method='GET', uri=root_metadata_url)
        assert not aiohttpretty.has_call(method='DELETE', uri=root_delete_url)

        assert aiohttpretty.has_call(method='DELETE', uri=child1_delete_url)
        assert aiohttpretty.has_call(method='DELETE', uri=child2_delete_url)
        assert aiohttpretty.has_call(method='DELETE', uri=child3_delete_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_root_not_confirmed(self, provider):
        path = OneDrivePath('/', _ids=('0',))

        with pytest.raises(exceptions.DeleteError) as e:
            await provider.delete(path)

        assert e.value.code == 400
        assert e.value.message == 'confirm_delete=1 is required for deleting root provider folder'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_file(self, provider, file_sub_folder_response):
        path = OneDrivePath.from_response(file_sub_folder_response)

        delete_url = provider._build_content_url(file_sub_folder_response['id'])
        aiohttpretty.register_json_uri('DELETE', delete_url, status=204)

        await provider.delete(path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_folder(self, provider, folder_sub_sub_response):
        path = OneDrivePath.from_response(folder_sub_sub_response)

        delete_url = provider._build_content_url(folder_sub_sub_response['id'])
        aiohttpretty.register_json_uri('DELETE', delete_url, status=204)

        await provider.delete(path)



