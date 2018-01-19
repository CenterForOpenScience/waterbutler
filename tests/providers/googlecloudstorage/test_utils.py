import json
import logging
from urllib.parse import quote

import pytest

from tests.providers.googlecloudstorage.fixtures import (mock_auth, mock_credentials,
                                                         mock_settings, file_stream,
                                                         batch_id_prefix, batch_boundary,
                                                         src_file_wb_path, dest_file_wb_path,
                                                         src_folder_wb_path, dest_folder_wb_path,
                                                         src_file_object_name,
                                                         dest_file_object_name,
                                                         src_folder_object_name,
                                                         dest_folder_object_name,
                                                         batch_copy_request,
                                                         batch_copy_response,
                                                         batch_delete_request,
                                                         batch_delete_response,
                                                         failed_requests_list,
                                                         batch_copy_request_failed,
                                                         batch_copy_response_failed,
                                                         batch_delete_request_failed,
                                                         batch_delete_response_failed,
                                                         batch_copy_response_part,
                                                         batch_copy_response_failed_part,
                                                         batch_delete_response_failed_part,
                                                         metadata_folder_all,
                                                         metadata_folder_immediate,
                                                         metadata_folder_itself,
                                                         metadata_file_itself,)

from waterbutler.providers.googlecloudstorage import utils as pd_utils
from waterbutler.providers.googlecloudstorage import settings as pd_settings
from waterbutler.providers.googlecloudstorage import GoogleCloudStorageProvider

logger = logging.getLogger(__name__)


@pytest.fixture()
def mock_provider(mock_auth, mock_credentials, mock_settings):
    return GoogleCloudStorageProvider(mock_auth, mock_credentials, mock_settings)


@pytest.fixture()
def mock_dest_provider(mock_provider):
    mock_provider.bucket = 'gcloud-test-2.longzechen.com'
    return mock_provider


class TestGetObjectName:
    """Test that the "Object Name" can be correctly obtained from a ``WaterbutlerPath`` object.

    Google Cloud Storage API uses "Object Name" as an identifier to refer to objects (files and
    folders) in URL path and query parameters. Make sure that:

    1. For both files and folders, it never starts with a '/'
    2. For files, it does not end with a '/'
    3. For folders, it does end with a '/'
    """

    def test_get_file_object_name(self, src_file_wb_path, src_file_object_name):

        object_name = pd_utils.get_obj_name(src_file_wb_path)
        assert object_name == src_file_object_name

    def test_get_folder_object_name(self, src_folder_wb_path, src_folder_object_name):

        object_name = pd_utils.get_obj_name(src_folder_wb_path, is_folder=True)
        assert object_name == src_folder_object_name


class TestBuildUrl:
    """Test that request URL for storage actions are built correctly.

    Google Cloud Storage API relies heavily on the URL instead of the request body to decide what
    actions to perform and what objects to perform the actions on.

    Please note that "Object Name" are already tested in ``TestGetObjectName``. To reduce overlap
    and avoid wasting resource, fixtures of object name are preferred while fixtures of waterbutler
    path are used only when necessary.

    For folders and files, Google Cloud Storage sees them as objects. For simple actions, each one
    tests both a file and a folder. For complex actions (batch actions and list actions) on a folder
    are handled and tested in their own tests.

    For assertions, don't compare full URL with more than one query parameter since they are built
    in random order. TODO: should we fix it?
    """

    def test_build_url_for_object_actions(
            self,
            mock_provider,
            src_file_object_name,
            src_folder_object_name
    ):
        """DELETE an object and GET an object's raw metadata share the same full URL.
        """

        file_url = mock_provider.build_url(
            pd_settings.BASE_URL,
            obj_name=src_file_object_name,
            **{}
        )
        assert file_url == 'https://www.googleapis.com/storage/v1/b/{}/o/{}'.format(
                                quote(mock_provider.bucket, safe=''),
                                quote(src_file_object_name, safe='')
                            )

        folder_url = mock_provider.build_url(
            pd_settings.BASE_URL,
            obj_name=src_folder_object_name,
            **{}
        )
        assert folder_url == 'https://www.googleapis.com/storage/v1/b/{}/o/{}'.format(
                                 quote(mock_provider.bucket, safe=''),
                                 quote(src_folder_object_name, safe='')
                             )

    def test_build_url_for_objects_upload(
            self,
            mock_provider,
            src_file_object_name,
            src_folder_object_name
    ):
        """File Upload and Folder Creation share the same Object Upload URL.
        """

        query = {
            'uploadType': 'media',
            'name': src_file_object_name
        }
        file_upload_url = mock_provider.build_url(base_url=pd_settings.UPLOAD_URL, **query)

        assert file_upload_url.startswith(
            'https://www.googleapis.com/upload/storage/v1/b/{}/o?'.format(
                quote(mock_provider.bucket, safe='')
            )
        )
        assert 'uploadType=media' in file_upload_url
        assert 'name={}'.format(quote(src_file_object_name, safe='')) in file_upload_url

        query.update({'name': src_folder_object_name})
        folder_create_url = mock_provider.build_url(base_url=pd_settings.UPLOAD_URL, **query)

        assert 'name={}'.format(quote(src_folder_object_name, safe='')) in folder_create_url

    def test_build_url_for_file_download(self, mock_provider, src_file_object_name):

        query = {'alt': 'media'}
        file_url = mock_provider.build_url(
            base_url=pd_settings.BASE_URL,
            obj_name=src_file_object_name,
            **query
        )
        assert file_url == 'https://www.googleapis.com/storage/v1/b/{}/o/{}?alt=media'.format(
                               quote(mock_provider.bucket, safe=''),
                               quote(src_file_object_name, safe='')
                           )

    def test_build_url_for_object_intra_copy(
            self,
            mock_provider,
            mock_dest_provider,
            src_file_object_name,
            dest_file_object_name,
            src_folder_object_name,
            dest_folder_object_name
    ):
        """File Copy and Folder Copy share the same Object CopyTo URL.
        """

        file_intra_copy_url = mock_provider.build_url(
            base_url=pd_settings.BASE_URL,
            obj_name=src_file_object_name,
            obj_action=pd_settings.COPY_ACTION,
            dest_bucket=mock_dest_provider.bucket,
            dest_obj_name=dest_file_object_name,
            **{}
        )

        folder_intra_copy_url = mock_provider.build_url(
            base_url=pd_settings.BASE_URL,
            obj_name=src_folder_object_name,
            obj_action=pd_settings.COPY_ACTION,
            dest_bucket=mock_dest_provider.bucket,
            dest_obj_name=dest_folder_object_name,
            **{}
        )

        assert file_intra_copy_url == 'https://www.googleapis.com/storage/v1' \
                                      '/b/{}/o/{}/copyTo/b/{}/o/{}'.format(
                                          quote(mock_provider.bucket, safe=''),
                                          quote(src_file_object_name, safe=''),
                                          quote(mock_dest_provider.bucket, safe=''),
                                          quote(dest_file_object_name, safe='')
                                      )

        assert folder_intra_copy_url == 'https://www.googleapis.com/storage/v1' \
                                        '/b/{}/o/{}/copyTo/b/{}/o/{}'.format(
                                            quote(mock_provider.bucket, safe=''),
                                            quote(src_folder_object_name, safe=''),
                                            quote(mock_dest_provider.bucket, safe=''),
                                            quote(dest_folder_object_name, safe='')
                                        )

    def test_build_url_for_listing(self, mock_provider, src_folder_object_name):

        query = {'prefix': src_folder_object_name, 'delimiter': '/'}
        metadata_folder_url = mock_provider.build_url(base_url=pd_settings.BASE_URL, **query)

        assert metadata_folder_url.startswith(
            'https://www.googleapis.com/storage/v1/b/{}/o?'.format(
                quote(mock_provider.bucket, safe='')
            )
        )
        assert 'prefix={}'.format(quote(src_folder_object_name, safe='')) in metadata_folder_url
        assert 'delimiter=%2F' in metadata_folder_url


class TestBuildBatchRequestPayload:
    """Test that request body for batch request are built correctly.  This includes both the initial
    request for batch delete/copy and the follow-up request that batch delete/copy failed ones.
    """

    def test_build_payload_for_batch_delete(
            self,
            mock_provider,
            metadata_folder_all,
            batch_id_prefix,
            failed_requests_list,
            batch_delete_request,
            batch_delete_request_failed,
    ):

        items = json.loads(metadata_folder_all).get('items', None)
        payload_full, requests_map = mock_provider.build_payload_for_batch_delete(
            items,
            batch_id_prefix
        )

        assert len(requests_map) == 7
        assert payload_full == batch_delete_request

        payload_failed = pd_utils.build_payload_from_req_map(
            failed_requests_list,
            requests_map
        )

        assert payload_failed == batch_delete_request_failed

    def test_build_payload_for_batch_copy(
            self,
            mock_provider,
            metadata_folder_all,
            batch_id_prefix,
            failed_requests_list,
            src_folder_object_name,
            dest_folder_object_name,
            batch_copy_request,
            batch_copy_request_failed,
    ):

        items = json.loads(metadata_folder_all).get('items', None)
        payload_full, requests_map = mock_provider.build_payload_for_batch_copy(
            items,
            batch_id_prefix,
            src_folder_object_name,
            dest_folder_object_name,
            mock_provider.bucket
        )

        assert len(requests_map) == 7
        assert payload_full == batch_copy_request

        payload_failed = pd_utils.build_payload_from_req_map(
            failed_requests_list,
            requests_map
        )
        assert payload_failed == batch_copy_request_failed


class TestParseBatchResponsePayload:
    """Test that response body are parsed correctly.
    """

    def test_get_request_id(
            self,
            batch_delete_response_failed_part,
            batch_copy_response_failed_part
    ):
        content_id = pd_utils.get_req_id_from_resp_part(batch_delete_response_failed_part)
        assert content_id == 99

        content_id = pd_utils.get_req_id_from_resp_part(batch_copy_response_failed_part)
        assert content_id == 88

    def test_get_metadata(self, batch_copy_response_part, metadata_folder_itself):

        metadata = pd_utils.get_metadata_from_resp_part(batch_copy_response_part)
        assert metadata is not None
        assert metadata == json.loads(metadata_folder_itself)

    def test_parse_payload_for_batch_delete(self, batch_delete_response):

        failed_requests = pd_utils.parse_batch_delete_resp(batch_delete_response)
        assert len(failed_requests) == 0

    def test_parse_payload_for_batch_delete_failed(self, batch_delete_response_failed):

        failed_requests = pd_utils.parse_batch_delete_resp(batch_delete_response_failed)
        assert len(failed_requests) == 3
        assert failed_requests == [1, 3, 5]

    def test_parse_payload_for_batch_copy(
            self,
            batch_copy_response,
            metadata_folder_itself,
            metadata_file_itself
    ):
        metadata_list, failed_requests = pd_utils.parse_batch_copy_resp(batch_copy_response)

        assert len(failed_requests) == 0
        assert len(metadata_list) == 7
        assert json.loads(metadata_folder_itself) in metadata_list
        assert json.loads(metadata_file_itself) in metadata_list

    def test_parse_payload_for_batch_copy_failed(
            self,
            batch_copy_response_failed,
            metadata_folder_itself,
            metadata_file_itself
    ):
        metadata_list, failed_requests = pd_utils.parse_batch_copy_resp(batch_copy_response_failed)

        assert len(failed_requests) == 3
        assert failed_requests == [1, 3, 5]
        assert len(metadata_list) == 3
        assert json.loads(metadata_folder_itself) not in metadata_list
        assert json.loads(metadata_file_itself) in metadata_list
