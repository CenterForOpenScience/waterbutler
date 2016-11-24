import os
import pytest
import aiohttpretty

from tests import utils

from waterbutler.core import exceptions
from waterbutler.providers.onedrive.metadata import OneDriveFileMetadata
from waterbutler.providers.onedrive.provider import OneDrivePath


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


class TestIntraMove:
    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_move_file(self, provider, file_sub_response, file_rename_sub_response):
        src_path = OneDrivePath.from_response(file_sub_response)
        dest_path = src_path.parent.child(file_rename_sub_response['name'])

        update_url = provider._build_content_url(src_path.identifier)
        aiohttpretty.register_json_uri('PATCH', update_url, body=file_rename_sub_response)

        assert dest_path.identifier is None

        metadata, created = await provider.intra_move(provider, src_path, dest_path)

        assert aiohttpretty.has_call(method='PATCH', uri=update_url)
        assert created is True
        assert metadata._path.full_path == dest_path.full_path
        assert metadata._path.parent == src_path.parent

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('settings', [{'folder': '0'}])
    async def test_move_file_exists(self, provider, file_sub_response, file_root_response):
        src_path = OneDrivePath.from_response(file_sub_response)
        dest_path = OneDrivePath.from_response(file_root_response)

        update_url = provider._build_content_url(src_path.identifier)
        aiohttpretty.register_json_uri('PATCH', update_url, body=file_root_response)
        delete_url = provider._build_content_url(dest_path.identifier)
        aiohttpretty.register_json_uri('DELETE', delete_url, status=204)

        metadata, created = await provider.intra_move(provider, src_path, dest_path)

        assert aiohttpretty.has_call(method='PATCH', uri=update_url)
        assert aiohttpretty.has_call(method='DELETE', uri=delete_url)
        assert created is False

        expected_path = OneDrivePath.from_response(file_root_response)
        assert metadata._path == expected_path


class TestIntraCopy:
    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('settings', [{'folder': '0'}])
    async def test_copy_directory(self, provider, folder_sub_response):
        src_path = OneDrivePath.from_response(folder_sub_response)
        dest_response = folder_sub_response.copy()
        dest_response['name'] = 'elect-y.jpg'
        dest_path = src_path.parent.child(dest_response['name'])

        status_url = 'http://any.url'
        copy_url = provider._build_content_url(src_path.identifier, 'action.copy')
        aiohttpretty.register_uri('POST', copy_url, headers={
            'LOCATION': status_url
        }, status=202)
        aiohttpretty.register_json_uri('GET', status_url, body=dest_response, status=200)

        metadata, created = await provider.intra_copy(provider, src_path, dest_path)

        assert aiohttpretty.has_call(method='POST', uri=copy_url)
        assert aiohttpretty.has_call(method='GET', uri=status_url)

        assert created is True
        assert metadata._path.name == dest_path.name

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_copy_timeout(self, monkeypatch, provider, folder_sub_sub_response):
        monkeypatch.setattr('waterbutler.providers.onedrive.settings.ONEDRIVE_COPY_REQUEST_TIMEOUT', 3)

        src_path = OneDrivePath.from_response(folder_sub_sub_response)
        dest_path = src_path.parent.child('new_big_folder/', folder=True)

        status_url = 'http://any.url'
        copy_url = provider._build_content_url(src_path.identifier, 'action.copy')
        aiohttpretty.register_uri('POST', copy_url, headers={
            'LOCATION': status_url
        }, status=202)
        aiohttpretty.register_uri('GET', status_url, status=202)

        with pytest.raises(exceptions.CopyError) as e:
            await provider.intra_copy(provider, src_path, dest_path)

        assert e.value.code == 202
        assert e.value.message.startswith("OneDrive API file copy has not responded in a timely manner")


class TestCreateFolder:
    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('settings', [{'folder': '0'}])
    async def test_create_folder(self, provider, folder_sub_response):
        root_path = OneDrivePath('/', _ids=('0', ))
        path = root_path.child(folder_sub_response['name'], folder=True)

        create_url = provider._build_content_url(path.parent.identifier, 'children')
        create_root_url = provider._build_root_url('drive', 'root', 'children')
        aiohttpretty.register_json_uri('POST', create_url, body=folder_sub_response, status=201)
        aiohttpretty.register_json_uri('POST', create_root_url, body=folder_sub_response, status=201)

        resp = await provider.create_folder(path)

        assert aiohttpretty.has_call(method='POST', uri=create_root_url)
        assert not aiohttpretty.has_call(method='POST', uri=create_url)

        assert resp.kind == 'folder'
        assert resp.name == path.name
        assert resp.path == '/' + path.path

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_create_sub_folder(self, provider, folder_sub_response, folder_sub_sub_response):
        root_path = OneDrivePath.from_response(folder_sub_response)
        path = root_path.child(folder_sub_sub_response['name'], folder=True)

        create_url = provider._build_content_url(path.parent.identifier, 'children')
        create_root_url = provider._build_root_url('drive', 'root', 'children')
        aiohttpretty.register_json_uri('POST', create_url, body=folder_sub_sub_response, status=201)
        aiohttpretty.register_json_uri('POST', create_root_url, body=folder_sub_sub_response, status=201)

        resp = await provider.create_folder(path)

        assert not aiohttpretty.has_call(method='POST', uri=create_root_url)
        assert aiohttpretty.has_call(method='POST', uri=create_url)

        assert resp.kind == 'folder'
        assert resp.name == path.name
        assert resp.path == '/' + path.path

    @pytest.mark.skip
    async def test_already_exist(self, provider):
        pass

    @pytest.mark.asyncio
    async def test_must_be_folder(self, provider):
        with pytest.raises(exceptions.CreateFolderError) as e:
            await provider.create_folder(OneDrivePath('/test.jpg', _ids=(None, None)))


class TestDownload:
    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download(self, provider, file_sub_response):
        body = b'test download content'
        path = OneDrivePath.from_response(file_sub_response)
        download_url = file_sub_response['@content.downloadUrl']
        metadata_url = provider._build_content_url(path.identifier)
        aiohttpretty.register_json_uri('GET', metadata_url, body=file_sub_response, status=200)
        aiohttpretty.register_uri('GET', download_url, body=body, auto_length=True, status=200)

        stream = await provider.download(path)

        assert aiohttpretty.has_call(method='GET', uri=metadata_url)
        assert aiohttpretty.has_call(method='GET', uri=download_url)
        assert (await stream.read()) == body

    @pytest.mark.skip
    async def test_download_revision(self, provider):
        pass


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
        utils.assert_deep_equal(e.value.message, not_found_error_response)


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
