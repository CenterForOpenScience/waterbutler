import io
import json
from http import HTTPStatus

import pytest
import aiohttpretty

from waterbutler.core.path import WaterButlerPath
from waterbutler.core.streams import FileStreamReader, ResponseStreamReader
from waterbutler.core import exceptions as core_exceptions
from waterbutler.providers.googlecloud import utils as pd_utils
from waterbutler.providers.googlecloud import settings as pd_settings
from waterbutler.providers.googlecloud import (GoogleCloudProvider, BaseGoogleCloudMetadata,
                                               GoogleCloudFileMetadata, GoogleCloudFolderMetadata,)


from tests.providers.googlecloud.fixtures import (mock_auth, mock_creds, mock_settings,
                                                  mock_auth_2, mock_creds_2, mock_settings_2,
                                                  file_path, dest_file_path, folder_path,
                                                  test_file_1, test_file_2,
                                                  sub_folder_1_path, meta_sub_folder_1_itself,
                                                  sub_folder_2_path, meta_sub_folder_2_itself,
                                                  sub_file_1_path, sub_file_2_path,
                                                  err_resp_unauthorized, err_resp_not_found,
                                                  meta_file_itself, meta_folder_itself,
                                                  meta_folder_all, meta_folder_immediate,)


@pytest.fixture()
def mock_provider(mock_auth, mock_creds, mock_settings):
    return GoogleCloudProvider(mock_auth, mock_creds, mock_settings)


@pytest.fixture()
def mock_provider_2(mock_auth_2, mock_creds_2, mock_settings_2):
    return GoogleCloudProvider(mock_auth_2, mock_creds_2, mock_settings_2)


@pytest.fixture
def file_stream_test_file(test_file_1):
    return FileStreamReader(io.BytesIO(test_file_1))


@pytest.fixture
def file_stream_test_file_2(test_file_2):
    return FileStreamReader(io.BytesIO(test_file_2))


class TestProviderInit:
    """Test that provider initialization set properties correctly.
    """

    async def test_provider_init(self, mock_provider):

        assert mock_provider is not None
        assert mock_provider.bucket == 'gcloud-test.longzechen.com'
        assert mock_provider.access_token == 'GlxLBdGqh56rEStTEs0KeMdEFmRJlGpg7e95y8jvzQoHbFZrnPDNB'
        assert mock_provider.region == 'US-EAST1'


class TestValidatePath:

    # TODO: implement this test when updating from limited to full version
    # @pytest.mark.asyncio
    # @pytest.mark.aiohttpretty
    # async def test_validate_v1_path_file(self):
    #     pass

    # TODO: implement this test when updating from limited to full version
    # @pytest.mark.asyncio
    # @pytest.mark.aiohttpretty
    # async def test_validate_v1_path_folder(self):
    #     pass

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_path_file(self, mock_provider, sub_file_1_path):

        assert file_path.startswith('/') and not file_path.endswith('/')
        wb_path = await mock_provider.validate_path(sub_file_1_path)
        assert wb_path.path == file_path.lstrip('/')

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_path_file(self, mock_provider, sub_folder_1_path):

        assert sub_folder_1_path.startswith('/') and sub_folder_1_path.endswith('/')
        wb_path = await mock_provider.validate_path(sub_folder_1_path)
        assert wb_path.path == sub_folder_1_path.lstrip('/')


class TestMetadata:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file(self, mock_provider, file_path, meta_file_itself):

        path = WaterButlerPath(file_path)
        metadata_url = mock_provider.build_url(
            base_url=pd_settings.BASE_URL,
            obj_name=pd_utils.get_obj_name(path, is_folder=False),
            **{}
        )
        metadata_json = json.loads(meta_file_itself)

        aiohttpretty.register_uri(
            'GET',
            metadata_url,
            body=json.dumps(metadata_json).encode('UTF-8'),
            headers={'Content-Type': 'application/json; charset=UTF-8'},
            status=HTTPStatus.OK
        )

        metadata = await mock_provider._metadata_object(path, is_folder=False)
        metadata_expected = GoogleCloudFileMetadata(metadata_json)
        assert isinstance(metadata, GoogleCloudFileMetadata)
        assert metadata == metadata_expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder_itself(self, mock_provider, folder_path, meta_folder_itself):

        path = WaterButlerPath(folder_path)
        metadata_url = mock_provider.build_url(
            base_url=pd_settings.BASE_URL,
            obj_name=pd_utils.get_obj_name(path, is_folder=True),
            **{}
        )
        metadata_json = json.loads(meta_folder_itself)

        aiohttpretty.register_uri(
            'GET',
            metadata_url,
            body=json.dumps(metadata_json).encode('UTF-8'),
            headers={'Content-Type': 'application/json; charset=UTF-8'},
            status=HTTPStatus.OK
        )

        metadata = await mock_provider._metadata_object(path, is_folder=True)
        metadata_expected = GoogleCloudFolderMetadata(metadata_json)
        assert isinstance(metadata, GoogleCloudFolderMetadata)
        assert metadata == metadata_expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder_all_children(
            self,
            mock_provider,
            folder_path,
            meta_folder_all
    ):
        path = WaterButlerPath(folder_path)
        prefix = pd_utils.get_obj_name(path, is_folder=True)
        metadata_url = mock_provider.build_url(
            base_url=pd_settings.BASE_URL,
            **{'prefix': prefix}
        )
        metadata_json = json.loads(meta_folder_all)

        aiohttpretty.register_uri(
            'GET',
            metadata_url,
            body=json.dumps(metadata_json).encode('UTF-8'),
            headers={'Content-Type': 'application/json; charset=UTF-8'},
            status=HTTPStatus.OK
        )

        items = await mock_provider._metadata_all_children(path)
        assert isinstance(items, list)
        assert len(items) == 7
        for item in items:
            assert item.get('name', '').startswith(prefix)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder_immediate_children(
            self,
            mock_provider,
            folder_path,
            meta_folder_immediate,
            sub_folder_1_path,
            meta_sub_folder_1_itself,
            sub_folder_2_path,
            meta_sub_folder_2_itself,
            sub_file_1_path,
            sub_file_2_path
    ):
        path = WaterButlerPath(folder_path)
        metadata_url = mock_provider.build_url(
            base_url=pd_settings.BASE_URL,
            **{
                'prefix': pd_utils.get_obj_name(path, is_folder=True),
                'delimiter': '/'
            }
        )
        metadata_json = json.loads(meta_folder_immediate)
        aiohttpretty.register_uri(
            'GET',
            metadata_url,
            body=json.dumps(metadata_json).encode('UTF-8'),
            headers={'Content-Type': 'application/json; charset=UTF-8'},
            status=HTTPStatus.OK
        )

        metadata_url = mock_provider.build_url(
            base_url=pd_settings.BASE_URL,
            obj_name=pd_utils.get_obj_name(WaterButlerPath(sub_folder_1_path), is_folder=True)
        )
        metadata_json = json.loads(meta_sub_folder_1_itself)
        aiohttpretty.register_uri(
            'GET',
            metadata_url,
            body=json.dumps(metadata_json).encode('UTF-8'),
            headers={'Content-Type': 'application/json; charset=UTF-8'},
            status=HTTPStatus.OK
        )

        metadata_url = mock_provider.build_url(
            base_url=pd_settings.BASE_URL,
            obj_name=pd_utils.get_obj_name(WaterButlerPath(sub_folder_2_path), is_folder=True)
        )
        metadata_json = json.loads(meta_sub_folder_2_itself)
        aiohttpretty.register_uri(
            'GET',
            metadata_url,
            body=json.dumps(metadata_json).encode('UTF-8'),
            headers={'Content-Type': 'application/json; charset=UTF-8'},
            status=HTTPStatus.OK
        )

        metadata_list = await mock_provider._metadata_folder(path)
        assert isinstance(metadata_list, list)
        assert len(metadata_list) == 4

        immediate_children = [
            sub_file_1_path,
            sub_file_2_path,
            sub_folder_1_path,
            sub_folder_2_path
        ]
        for metadata in metadata_list:
            assert isinstance(metadata, BaseGoogleCloudMetadata)
            assert metadata.path in immediate_children

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_object_401_unauthorized(
            self,
            mock_provider,
            err_resp_unauthorized
    ):
        # Whether it is a file or a folder does not make a difference, use folder for 401 test.
        path = WaterButlerPath('/temp/')
        metadata_url = mock_provider.build_url(
            base_url=pd_settings.BASE_URL,
            obj_name=pd_utils.get_obj_name(path, is_folder=True),
            **{}
        )
        resp_json = json.loads(err_resp_unauthorized)

        aiohttpretty.register_uri(
            'GET',
            metadata_url,
            body=json.dumps(resp_json).encode('UTF-8'),
            headers={'Content-Type': 'application/json; charset=UTF-8'},
            status=HTTPStatus.UNAUTHORIZED
        )

        with pytest.raises(core_exceptions.MetadataError) as exc:
            await mock_provider._metadata_object(path, is_folder=True)

        assert exc.value.code == HTTPStatus.UNAUTHORIZED

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_object_404_not_found(
            self,
            mock_provider,
            err_resp_not_found
    ):
        # Whether it is a file or a folder does not make a difference, use file for 404 test.
        path = WaterButlerPath('/temp')
        metadata_url = mock_provider.build_url(
            base_url=pd_settings.BASE_URL,
            obj_name=pd_utils.get_obj_name(path, is_folder=False),
            **{}
        )
        metadat_json = json.loads(err_resp_not_found)

        aiohttpretty.register_uri(
            'GET',
            metadata_url,
            body=json.dumps(metadat_json).encode('UTF-8'),
            headers={'Content-Type': 'application/json; charset=UTF-8'},
            status=HTTPStatus.NOT_FOUND
        )

        with pytest.raises(core_exceptions.MetadataError) as exc:
            await mock_provider._metadata_object(path, is_folder=False)

        assert exc.value.code == HTTPStatus.NOT_FOUND


class TestCRUD:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_file(self, mock_provider, file_path, test_file_1):

        path = WaterButlerPath(file_path)
        download_url = mock_provider.build_url(
            base_url=pd_settings.BASE_URL,
            obj_name=pd_utils.get_obj_name(path, is_folder=False),
            **{'alt': 'media'}
        )

        aiohttpretty.register_uri(
            'GET',
            download_url,
            body=test_file_1,
            headers={'Content-Type': 'application/json; charset=UTF-8'},
            status=HTTPStatus.OK
        )

        resp_stream_reader = await mock_provider.download(path)
        file_content = await resp_stream_reader.read()

        assert isinstance(resp_stream_reader, ResponseStreamReader)
        assert file_content == test_file_1

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_file_not_found(self, mock_provider, file_path, err_resp_not_found):
        path = WaterButlerPath(file_path)
        download_url = mock_provider.build_url(
            base_url=pd_settings.BASE_URL,
            obj_name=pd_utils.get_obj_name(path, is_folder=False),
            **{'alt': 'media'}
        )
        metadata_json = json.loads(err_resp_not_found)

        aiohttpretty.register_uri(
            'GET',
            download_url,
            body=json.dumps(metadata_json).encode('UTF-8'),
            headers={'Content-Type': 'application/json; charset=UTF-8'},
            status=HTTPStatus.NOT_FOUND
        )

        with pytest.raises(core_exceptions.DownloadError) as exc:
            await mock_provider.download(path)

        assert exc.value.code == HTTPStatus.NOT_FOUND
        assert aiohttpretty.has_call(method='GET', uri=download_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_folder(
            self,
            mock_provider,
            folder_path,
            test_file_1
    ):
        path = WaterButlerPath(folder_path)
        download_url = mock_provider.build_url(
            base_url=pd_settings.BASE_URL,
            obj_name=pd_utils.get_obj_name(path, is_folder=True),
            **{'alt': 'media'}
        )

        aiohttpretty.register_uri(
            'GET',
            download_url,
            body=test_file_1,
            headers={'Content-Type': 'application/json; charset=UTF-8'},
            status=HTTPStatus.OK
        )

        with pytest.raises(core_exceptions.DownloadError) as exc:
            await mock_provider.download(path)

        assert exc.value.code == HTTPStatus.BAD_REQUEST
        assert not aiohttpretty.has_call(method='GET', uri=download_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_file_new(
            self,
            mock_provider,
            file_path,
            meta_file_itself,
            err_resp_not_found,
            file_stream_test_file
    ):
        path = WaterButlerPath(file_path)
        obj_name = pd_utils.get_obj_name(path, is_folder=False)

        metadata_url = mock_provider.build_url(
            base_url=pd_settings.BASE_URL,
            obj_name=obj_name,
            **{}
        )
        metadata_json = json.loads(err_resp_not_found)
        aiohttpretty.register_uri(
            'GET',
            metadata_url,
            body=json.dumps(metadata_json).encode('UTF-8'),
            headers={'Content-Type': 'application/json; charset=UTF-8'},
            status=HTTPStatus.NOT_FOUND
        )

        upload_url = mock_provider.build_url(
            base_url=pd_settings.UPLOAD_URL,
            **{'uploadType': 'media', 'name': obj_name}
        )
        metadata_json = json.loads(meta_file_itself)
        aiohttpretty.register_uri(
            'POST',
            upload_url,
            body=json.dumps(metadata_json).encode('UTF-8'),
            headers={'Content-Type': 'application/json'},
            status=HTTPStatus.OK
        )

        metadata, created = await mock_provider.upload(file_stream_test_file, path)

        assert created is True
        assert metadata == GoogleCloudFileMetadata(metadata_json)
        assert aiohttpretty.has_call(method='GET', uri=metadata_url)
        assert aiohttpretty.has_call(method='POST', uri=upload_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_file_existing(
            self,
            mock_provider,
            file_path,
            meta_file_itself,
            file_stream_test_file
    ):
        path = WaterButlerPath(file_path)
        obj_name = pd_utils.get_obj_name(path, is_folder=False)

        metadata_url = mock_provider.build_url(
            base_url=pd_settings.BASE_URL,
            obj_name=obj_name,
            **{}
        )
        metadata_json = json.loads(meta_file_itself)
        aiohttpretty.register_uri(
            'GET',
            metadata_url,
            body=json.dumps(metadata_json).encode('UTF-8'),
            headers={'Content-Type': 'application/json; charset=UTF-8'},
            status=HTTPStatus.OK
        )

        upload_url = mock_provider.build_url(
            base_url=pd_settings.UPLOAD_URL,
            **{'uploadType': 'media', 'name': obj_name}
        )
        metadata_json = json.loads(meta_file_itself)
        aiohttpretty.register_uri(
            'POST',
            upload_url,
            body=json.dumps(metadata_json).encode('UTF-8'),
            headers={'Content-Type': 'application/json'},
            status=HTTPStatus.OK
        )

        metadata, created = await mock_provider.upload(file_stream_test_file, path)

        assert created is False
        assert metadata == GoogleCloudFileMetadata(metadata_json)
        assert aiohttpretty.has_call(method='GET', uri=metadata_url)
        assert aiohttpretty.has_call(method='POST', uri=upload_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_file_checksum_mismatch(
            self,
            mock_provider,
            file_path,
            meta_file_itself,
            err_resp_not_found,
            file_stream_test_file_2
    ):
        path = WaterButlerPath(file_path)
        obj_name = pd_utils.get_obj_name(path, is_folder=False)

        metadata_url = mock_provider.build_url(
            base_url=pd_settings.BASE_URL,
            obj_name=obj_name,
            **{}
        )
        metadata_json = json.loads(err_resp_not_found)
        aiohttpretty.register_uri(
            'GET',
            metadata_url,
            body=json.dumps(metadata_json).encode('UTF-8'),
            headers={'Content-Type': 'application/json; charset=UTF-8'},
            status=HTTPStatus.NOT_FOUND
        )

        upload_url = mock_provider.build_url(
            base_url=pd_settings.UPLOAD_URL,
            **{'uploadType': 'media', 'name': obj_name}
        )
        metadata_json = json.loads(meta_file_itself)
        aiohttpretty.register_uri(
            'POST',
            upload_url,
            body=json.dumps(metadata_json).encode('UTF-8'),
            headers={'Content-Type': 'application/json'},
            status=HTTPStatus.OK
        )

        with pytest.raises(core_exceptions.UploadChecksumMismatchError):
            await mock_provider.upload(file_stream_test_file_2, path)

        assert aiohttpretty.has_call(method='GET', uri=metadata_url)
        assert aiohttpretty.has_call(method='POST', uri=upload_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_create_folder_new(
            self,
            mock_provider,
            folder_path,
            meta_folder_itself,
            err_resp_not_found
    ):
        path = WaterButlerPath(folder_path)
        obj_name =pd_utils.get_obj_name(path, is_folder=True)

        metadata_url = mock_provider.build_url(
            base_url=pd_settings.BASE_URL,
            obj_name=obj_name,
            **{}
        )
        metadata_json = json.loads(err_resp_not_found)
        aiohttpretty.register_uri(
            'GET',
            metadata_url,
            body=json.dumps(metadata_json).encode('UTF-8'),
            headers={'Content-Type': 'application/json; charset=UTF-8'},
            status=HTTPStatus.NOT_FOUND
        )

        upload_url = mock_provider.build_url(
            base_url=pd_settings.UPLOAD_URL,
            **{'uploadType': 'media', 'name': obj_name}
        )
        metadata_json = json.loads(meta_folder_itself)
        aiohttpretty.register_uri(
            'POST',
            upload_url,
            body=json.dumps(metadata_json).encode('UTF-8'),
            headers={'Content-Type': 'application/json'},
            status=HTTPStatus.OK
        )

        metadata = await mock_provider.create_folder(path)
        assert metadata == GoogleCloudFolderMetadata(metadata_json)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_create_folder_existing(
            self,
            mock_provider,
            folder_path,
            meta_folder_itself,
    ):
        path = WaterButlerPath(folder_path)
        obj_name = pd_utils.get_obj_name(path, is_folder=True)

        metadata_url = mock_provider.build_url(
            base_url=pd_settings.BASE_URL,
            obj_name=obj_name,
            **{}
        )
        metadata_json = json.loads(meta_folder_itself)
        aiohttpretty.register_uri(
            'GET',
            metadata_url,
            body=json.dumps(metadata_json).encode('UTF-8'),
            headers={'Content-Type': 'application/json; charset=UTF-8'},
            status=HTTPStatus.OK
        )

        with pytest.raises(core_exceptions.CreateFolderError) as exc:
            await mock_provider.create_folder(path)
        assert exc.value.code == HTTPStatus.CONFLICT

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_file(self, mock_provider, file_path):

        path = WaterButlerPath(file_path)
        delete_url = mock_provider.build_url(
            base_url=pd_settings.BASE_URL,
            obj_name=pd_utils.get_obj_name(path, is_folder=False),
            **{}
        )

        aiohttpretty.register_uri(
            'DELETE',
            delete_url,
            headers={'Content-Type': 'application/json'},
            status=HTTPStatus.NO_CONTENT
        )

        await mock_provider.delete(path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_file_not_found(self, mock_provider, file_path, err_resp_not_found):

        path = WaterButlerPath(file_path)
        delete_url = mock_provider.build_url(
            base_url=pd_settings.BASE_URL,
            obj_name=pd_utils.get_obj_name(path, is_folder=False),
            **{}
        )
        resp = json.dumps(json.loads(err_resp_not_found)).encode('UTF-8')

        aiohttpretty.register_uri(
            'DELETE',
            delete_url,
            body=resp,
            headers={'Content-Type': 'application/json; charset=UTF-8'},
            status=HTTPStatus.NOT_FOUND
        )

        with pytest.raises(core_exceptions.DeleteError) as exc:
            await mock_provider.delete(path)

        assert exc.value.code == HTTPStatus.NOT_FOUND

    # TODO: implement this test when updating from limited to full version
    # @pytest.mark.asyncio
    # @pytest.mark.aiohttpretty
    # async def test_delete_folder(self):
    #     pass

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_folder_not_found(self, mock_provider, folder_path):

        path = WaterButlerPath(folder_path)
        metadata_url = mock_provider.build_url(
            base_url=pd_settings.BASE_URL,
            **{'prefix': pd_utils.get_obj_name(path, is_folder=True)}
        )
        resp = json.dumps(json.loads('{"kind": "storage#objects"}')).encode('UTF-8')

        aiohttpretty.register_uri(
            'GET',
            metadata_url,
            body=resp,
            headers={'Content-Type': 'application/json; charset=UTF-8'},
            status=HTTPStatus.OK
        )

        with pytest.raises(core_exceptions.NotFoundError) as exc:
            await mock_provider.delete(path)

        assert exc.value.code == HTTPStatus.NOT_FOUND

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_copy_file_new(
            self,
            mock_provider,
            mock_provider_2,
            file_path,
            dest_file_path,
            meta_file_itself,
            err_resp_not_found
    ):
        src_path = WaterButlerPath(file_path)
        dest_path = WaterButlerPath(dest_file_path)

        metadata_url = mock_provider_2.build_url(
            base_url=pd_settings.BASE_URL,
            obj_name=pd_utils.get_obj_name(dest_path, is_folder=False),
            **{}
        )
        resp_json = json.loads(err_resp_not_found)
        aiohttpretty.register_uri(
            'GET',
            metadata_url,
            body=json.dumps(resp_json).encode('UTF-8'),
            headers={'Content-Type': 'application/json; charset=UTF-8'},
            status=HTTPStatus.NOT_FOUND
        )

        copy_url = mock_provider.build_url(
            base_url=pd_settings.BASE_URL,
            obj_name=pd_utils.get_obj_name(src_path, is_folder=False),
            obj_action=pd_settings.COPY_ACTION,
            dest_bucket=mock_provider_2.bucket,
            dest_obj_name=pd_utils.get_obj_name(dest_path, is_folder=False),
        )
        metadata_json = json.loads(meta_file_itself)
        aiohttpretty.register_uri(
            'POST',
            copy_url,
            body=json.dumps(metadata_json).encode('UTF-8'),
            headers={'Content-Type': 'application/json; charset=UTF-8'},
            status=HTTPStatus.OK
        )

        metadata, created = await mock_provider.intra_copy(mock_provider_2, src_path, dest_path)

        assert created is True
        assert metadata == GoogleCloudFileMetadata(metadata_json)
        assert aiohttpretty.has_call(method='GET', uri=metadata_url)
        assert aiohttpretty.has_call(method='POST', uri=copy_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_copy_file_new_existing(
            self,
            mock_provider,
            mock_provider_2,
            file_path,
            dest_file_path,
            meta_file_itself
    ):
        src_path = WaterButlerPath(file_path)
        dest_path = WaterButlerPath(dest_file_path)

        metadata_url = mock_provider_2.build_url(
            base_url=pd_settings.BASE_URL,
            obj_name=pd_utils.get_obj_name(dest_path, is_folder=False),
            **{}
        )
        metadata_json = json.loads(meta_file_itself)
        aiohttpretty.register_uri(
            'GET',
            metadata_url,
            body=json.dumps(metadata_json).encode('UTF-8'),
            headers={'Content-Type': 'application/json; charset=UTF-8'},
            status=HTTPStatus.OK
        )

        copy_url = mock_provider.build_url(
            base_url=pd_settings.BASE_URL,
            obj_name=pd_utils.get_obj_name(src_path, is_folder=False),
            obj_action=pd_settings.COPY_ACTION,
            dest_bucket=mock_provider_2.bucket,
            dest_obj_name=pd_utils.get_obj_name(dest_path, is_folder=False),
        )
        metadata_json = json.loads(meta_file_itself)
        aiohttpretty.register_uri(
            'POST',
            copy_url,
            body=json.dumps(metadata_json).encode('UTF-8'),
            headers={'Content-Type': 'application/json; charset=UTF-8'},
            status=HTTPStatus.OK
        )

        metadata, created = await mock_provider.intra_copy(mock_provider_2, src_path, dest_path)

        assert created is False
        assert metadata == GoogleCloudFileMetadata(metadata_json)
        assert aiohttpretty.has_call(method='GET', uri=metadata_url)
        assert aiohttpretty.has_call(method='POST', uri=copy_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_copy_file_not_found(
            self,
            mock_provider,
            mock_provider_2,
            file_path,
            dest_file_path,
            err_resp_not_found
    ):
        src_path = WaterButlerPath(file_path)
        dest_path = WaterButlerPath(dest_file_path)
        resp_json = json.loads(err_resp_not_found)

        metadata_url = mock_provider_2.build_url(
            base_url=pd_settings.BASE_URL,
            obj_name=pd_utils.get_obj_name(dest_path, is_folder=False),
            **{}
        )
        aiohttpretty.register_uri(
            'GET',
            metadata_url,
            body=json.dumps(resp_json).encode('UTF-8'),
            headers={'Content-Type': 'application/json; charset=UTF-8'},
            status=HTTPStatus.NOT_FOUND
        )

        copy_url = mock_provider.build_url(
            base_url=pd_settings.BASE_URL,
            obj_name=pd_utils.get_obj_name(src_path, is_folder=False),
            obj_action=pd_settings.COPY_ACTION,
            dest_bucket=mock_provider_2.bucket,
            dest_obj_name=pd_utils.get_obj_name(dest_path, is_folder=False),
        )
        aiohttpretty.register_uri(
            'POST',
            copy_url,
            body=json.dumps(resp_json).encode('UTF-8'),
            headers={'Content-Type': 'application/json; charset=UTF-8'},
            status=HTTPStatus.NOT_FOUND
        )

        with pytest.raises(core_exceptions.CopyError) as exc:
            await mock_provider.intra_copy(mock_provider_2, src_path, dest_path)

        assert exc.value.code == HTTPStatus.NOT_FOUND
        assert aiohttpretty.has_call(method='GET', uri=metadata_url)
        assert aiohttpretty.has_call(method='POST', uri=copy_url)

    # TODO: implement this test when updating from limited to full version
    # @pytest.mark.asyncio
    # @pytest.mark.aiohttpretty
    # async def test_intra_copy_folder(self):
    #     pass


class TestOperations:

    def test_provider_equality(self, mock_provider, mock_provider_2):

        assert mock_provider != mock_provider_2

    def test_can_intra_move(self, mock_provider, mock_provider_2, file_path, folder_path):

        assert mock_provider.can_intra_move(mock_provider, WaterButlerPath(file_path))
        assert not mock_provider.can_intra_move(mock_provider_2, WaterButlerPath(file_path))
        assert mock_provider.can_intra_move(mock_provider, WaterButlerPath(folder_path))
        assert not mock_provider.can_intra_move(mock_provider_2, WaterButlerPath(folder_path))

    def test_can_intra_copy(self, mock_provider, mock_provider_2, file_path, folder_path):

        assert mock_provider.can_intra_copy(mock_provider, WaterButlerPath(file_path))
        assert not mock_provider.can_intra_copy(mock_provider_2, WaterButlerPath(file_path))
        assert mock_provider.can_intra_copy(mock_provider, WaterButlerPath(folder_path))
        assert not mock_provider.can_intra_copy(mock_provider_2, WaterButlerPath(folder_path))

    def test_can_duplicate_names(self, mock_provider):

        assert mock_provider.can_duplicate_names()
