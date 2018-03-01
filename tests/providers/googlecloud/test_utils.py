from urllib.parse import quote

import pytest

from tests.providers.googlecloud.fixtures import (mock_auth, mock_creds, mock_settings,
                                                  mock_auth_2, mock_creds_2, mock_settings_2,
                                                  src_file_wb_path, src_folder_wb_path,
                                                  src_file_obj_name, dest_file_obj_name,
                                                  src_folder_obj_name, dest_folder_obj_name, )

from waterbutler.providers.googlecloud import utils as pd_utils
from waterbutler.providers.googlecloud import GoogleCloudProvider


@pytest.fixture()
def mock_provider(mock_auth, mock_creds, mock_settings):
    return GoogleCloudProvider(mock_auth, mock_creds, mock_settings)


@pytest.fixture()
def mock_provider_dest(mock_auth_2, mock_creds_2, mock_settings_2):
    return GoogleCloudProvider(mock_auth_2, mock_creds_2, mock_settings_2)


class TestPathAndNameForObjects:
    """Test that the object name can be correctly obtained from a ``WaterbutlerPath`` object, and
    that the path, which is used to initiate ``WaterbutlerPath`` can be correctly obtained from the
    object name.

    Google Cloud Storage API uses "Object Name" as an identifier to refer to objects (files and
    folders) in URL path and query parameters. Make sure that:

    1. For both files and folders, it never starts with a '/'
    2. For files, it does not end with a '/'
    3. For folders, it does end with a '/'

    For ``WaterButlerPath``, ``generic_path_validation()`` expects one and only one leading `/`.
    """

    def test_path_and_obj_name_for_file(self, src_file_wb_path, src_file_obj_name):

        object_name = pd_utils.get_obj_name(src_file_wb_path)
        assert object_name == src_file_obj_name

        path = pd_utils.build_path(src_file_obj_name)
        assert path == '/' + src_file_wb_path.path

    def test_path_and_obj_name_for_folder(self, src_folder_wb_path, src_folder_obj_name):

        object_name = pd_utils.get_obj_name(src_folder_wb_path, is_folder=True)
        assert object_name == src_folder_obj_name

        path = pd_utils.build_path(src_folder_obj_name, is_folder=True)
        assert path == '/' + src_folder_wb_path.path


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

    def test_build_url_for_obj_actions(
            self,
            mock_provider,
            src_file_obj_name,
            src_folder_obj_name
    ):
        """DELETE an object and GET an object's raw metadata share the same full URL.
        """

        file_url = mock_provider.build_url(
            mock_provider.BASE_URL,
            obj_name=src_file_obj_name,
            **{}
        )
        assert file_url == 'https://www.googleapis.com/storage/v1/b/{}/o/{}'.format(
                                quote(mock_provider.bucket, safe=''),
                                quote(src_file_obj_name, safe='')
                            )

        folder_url = mock_provider.build_url(
            mock_provider.BASE_URL,
            obj_name=src_folder_obj_name,
            **{}
        )
        assert folder_url == 'https://www.googleapis.com/storage/v1/b/{}/o/{}'.format(
                                 quote(mock_provider.bucket, safe=''),
                                 quote(src_folder_obj_name, safe='')
                             )

    def test_build_url_for_objects_upload(
            self,
            mock_provider,
            src_file_obj_name,
            src_folder_obj_name
    ):
        """File Upload and Folder Creation share the same Object Upload URL.
        """

        query = {
            'uploadType': 'media',
            'name': src_file_obj_name
        }
        file_upload_url = mock_provider.build_url(
            base_url=mock_provider.BASE_URL + '/upload', **query
        )

        assert file_upload_url.startswith(
            'https://www.googleapis.com/upload/storage/v1/b/{}/o?'.format(
                quote(mock_provider.bucket, safe='')
            )
        )
        assert 'uploadType=media' in file_upload_url
        assert 'name={}'.format(quote(src_file_obj_name, safe='')) in file_upload_url

        query.update({'name': src_folder_obj_name})
        folder_create_url = mock_provider.build_url(
            base_url=mock_provider.BASE_URL + '/upload',
            **query
        )

        assert 'name={}'.format(quote(src_folder_obj_name, safe='')) in folder_create_url

    def test_build_url_for_file_download(self, mock_provider, src_file_obj_name):

        query = {'alt': 'media'}
        file_url = mock_provider.build_url(
            base_url=mock_provider.BASE_URL,
            obj_name=src_file_obj_name,
            **query
        )
        assert file_url == 'https://www.googleapis.com/storage/v1/b/{}/o/{}?alt=media'.format(
                               quote(mock_provider.bucket, safe=''),
                               quote(src_file_obj_name, safe='')
                           )

    def test_build_url_for_obj_intra_copy(
            self,
            mock_provider,
            mock_provider_dest,
            src_file_obj_name,
            dest_file_obj_name,
            src_folder_obj_name,
            dest_folder_obj_name
    ):
        """File Copy and Folder Copy share the same Object CopyTo URL.
        """

        file_intra_copy_url = mock_provider.build_url(
            base_url=mock_provider.BASE_URL,
            obj_name=src_file_obj_name,
            obj_action=mock_provider.COPY_ACTION,
            dest_bucket=mock_provider_dest.bucket,
            dest_obj_name=dest_file_obj_name,
            **{}
        )

        folder_intra_copy_url = mock_provider.build_url(
            base_url=mock_provider.BASE_URL,
            obj_name=src_folder_obj_name,
            obj_action=mock_provider.COPY_ACTION,
            dest_bucket=mock_provider_dest.bucket,
            dest_obj_name=dest_folder_obj_name,
            **{}
        )

        assert file_intra_copy_url == 'https://www.googleapis.com/storage/v1' \
                                      '/b/{}/o/{}/copyTo/b/{}/o/{}'.format(
                                          quote(mock_provider.bucket, safe=''),
                                          quote(src_file_obj_name, safe=''),
                                          quote(mock_provider_dest.bucket, safe=''),
                                          quote(dest_file_obj_name, safe='')
                                      )

        assert folder_intra_copy_url == 'https://www.googleapis.com/storage/v1' \
                                        '/b/{}/o/{}/copyTo/b/{}/o/{}'.format(
                                            quote(mock_provider.bucket, safe=''),
                                            quote(src_folder_obj_name, safe=''),
                                            quote(mock_provider_dest.bucket, safe=''),
                                            quote(dest_folder_obj_name, safe='')
                                        )

    def test_build_url_for_listing(self, mock_provider, src_folder_obj_name):

        query = {'prefix': src_folder_obj_name, 'delimiter': '/'}
        metadata_folder_url = mock_provider.build_url(base_url=mock_provider.BASE_URL, **query)

        assert metadata_folder_url.startswith(
            'https://www.googleapis.com/storage/v1/b/{}/o?'.format(
                quote(mock_provider.bucket, safe='')
            )
        )
        assert 'prefix={}'.format(quote(src_folder_obj_name, safe='')) in metadata_folder_url
        assert 'delimiter=%2F' in metadata_folder_url
