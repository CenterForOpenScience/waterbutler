import json
import logging
from http import HTTPStatus

import pytest
import aiohttpretty

from waterbutler.core.path import WaterButlerPath
from waterbutler.core import exceptions as core_exceptions
from waterbutler.providers.googlecloud import utils as pd_utils
from waterbutler.providers.googlecloud import settings as pd_settings
from waterbutler.providers.googlecloud import (GoogleCloudProvider, BaseGoogleCloudMetadata,
                                               GoogleCloudFileMetadata, GoogleCloudFolderMetadata,)


from tests.providers.googlecloud.fixtures import (mock_auth, mock_creds, mock_settings,
                                                  file_path, folder_path, file_stream,
                                                  sub_folder_1_path, metadata_sub_folder_1_itself,
                                                  sub_folder_2_path, metadata_sub_folder_2_itself,
                                                  sub_file_1_path, sub_file_2_path,
                                                  batch_id_prefix, batch_boundary,
                                                  src_file_wb_path, dest_file_wb_path,
                                                  src_folder_wb_path, dest_folder_wb_path,
                                                  src_file_object_name, dest_file_object_name,
                                                  src_folder_object_name, dest_folder_object_name,
                                                  batch_copy_request, batch_copy_response,
                                                  batch_delete_request, batch_delete_response,
                                                  failed_requests_list,
                                                  batch_copy_request_failed,
                                                  batch_copy_response_failed,
                                                  batch_delete_request_failed,
                                                  batch_delete_response_failed,
                                                  batch_copy_response_part,
                                                  batch_copy_response_failed_part,
                                                  batch_delete_response_failed_part,
                                                  error_response_401_unauthorized,
                                                  error_response_404_not_found,
                                                  metadata_folder_itself, metadata_file_itself,
                                                  metadata_folder_all, metadata_folder_immediate,)

logger = logging.getLogger(__name__)


@pytest.fixture()
def mock_provider(mock_auth, mock_creds, mock_settings):
    return GoogleCloudProvider(mock_auth, mock_creds, mock_settings)


@pytest.fixture()
def mock_dest_provider(mock_provider):
    mock_provider.bucket = 'gcloud-test-2.longzechen.com'
    return mock_provider


class TestProviderInit:
    """Test that provider initialization set properties correctly.
    """

    async def test_provider_init(self, mock_provider):

        assert mock_provider is not None
        assert mock_provider.bucket is not None
        assert mock_provider.access_token is not None
        assert mock_provider.region is not None
        assert mock_provider.can_duplicate_names() is True


class TestValidatePath:

    pass


class TestCRUD:

    pass


class TestMetadata:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file(self, mock_provider, file_path, metadata_file_itself):

        path = WaterButlerPath(file_path)
        metadata_url = mock_provider.build_url(
            base_url=pd_settings.BASE_URL,
            obj_name=pd_utils.get_obj_name(path, is_folder=False),
            **{}
        )
        metadata_json = json.loads(metadata_file_itself)

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
    async def test_metadata_folder_itself(self, mock_provider, folder_path, metadata_folder_itself):

        path = WaterButlerPath(folder_path)
        metadata_url = mock_provider.build_url(
            base_url=pd_settings.BASE_URL,
            obj_name=pd_utils.get_obj_name(path, is_folder=True),
            **{}
        )
        metadata_json = json.loads(metadata_folder_itself)

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
            metadata_folder_all
    ):
        path = WaterButlerPath(folder_path)
        prefix = pd_utils.get_obj_name(path, is_folder=True)
        query = {'prefix': prefix}
        metadata_url = mock_provider.build_url(
            base_url=pd_settings.BASE_URL,
            **query
        )
        metadata_json = json.loads(metadata_folder_all)

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
            logger.info(item.get('name', ''))
            assert item.get('name', '').startswith(prefix)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder_immediate_children(
            self,
            mock_provider,
            folder_path,
            metadata_folder_immediate,
            sub_folder_1_path,
            metadata_sub_folder_1_itself,
            sub_folder_2_path,
            metadata_sub_folder_2_itself,
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
        metadata_json = json.loads(metadata_folder_immediate)
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
        metadata_json = json.loads(metadata_sub_folder_1_itself)
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
        metadata_json = json.loads(metadata_sub_folder_2_itself)
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
            error_response_401_unauthorized
    ):
        # Whether it is a file or a folder does not make a difference, use folder for 401 test.
        path = WaterButlerPath('/temp/')
        metadata_url = mock_provider.build_url(
            base_url=pd_settings.BASE_URL,
            obj_name=pd_utils.get_obj_name(path, is_folder=True),
            **{}
        )
        resp_json = json.loads(error_response_401_unauthorized)

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
            error_response_404_not_found
    ):
        # Whether it is a file or a folder does not make a difference, use file for 404 test.
        path = WaterButlerPath('/temp')
        metadata_url = mock_provider.build_url(
            base_url=pd_settings.BASE_URL,
            obj_name=pd_utils.get_obj_name(path, is_folder=False),
            **{}
        )
        metadat_json = json.loads(error_response_404_not_found)

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


class TestCreateFolder:

    pass


class TestOperations:

    pass
