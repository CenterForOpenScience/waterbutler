import json

import pytest

from tests.providers.googlecloud.fixtures import (mock_auth, mock_creds, mock_settings,
                                                  metadata_file_itself, metadata_file_extra,
                                                  metadata_folder_itself, metadata_folder_extra,)

from waterbutler.providers.googlecloud import settings as pd_settings
from waterbutler.providers.googlecloud import (GoogleCloudProvider, BaseGoogleCloudMetadata,
                                               GoogleCloudFileMetadata, GoogleCloudFolderMetadata,)


@pytest.fixture()
def mock_provider(mock_auth, mock_creds, mock_settings):
    return GoogleCloudProvider(mock_auth, mock_creds, mock_settings)


class TestGoogleCloudFileMetadata:

    def test_file_metadata(self, metadata_file_itself, metadata_file_extra):

        # TODO: confirm that there is no need to test ``.serialized()`` and others.

        metadata_json = json.loads(metadata_file_itself)
        metadata_extra = json.loads(metadata_file_extra)
        metadata = GoogleCloudFileMetadata(metadata_json)

        assert isinstance(metadata, BaseGoogleCloudMetadata)
        assert metadata.provider == pd_settings.NAME
        assert metadata.path == '/test-folder-1/DSC_0235.JPG'
        assert metadata.name == 'DSC_0235.JPG'
        assert metadata.kind == 'file'
        assert metadata.content_type == 'image/jpeg'
        assert metadata.modified == '2018-01-11T15:56:12.174Z'
        assert metadata.modified_utc == '2018-01-11T15:56:12+00:00'
        assert metadata.created == '2018-01-11T15:56:12.174Z'
        assert metadata.created_utc == '2018-01-11T15:56:12+00:00'
        assert metadata.etag == 'CM38iP+i0NgCEAE='
        assert metadata.size == 10724401
        assert metadata.extra == dict(metadata_extra)


class TestGoogleCloudFolderMetadata:

    def test_folder_metadata(self, metadata_folder_itself, metadata_folder_extra):

        # TODO: confirm that there is no need to test ``.serialized()`` and others.

        metadata_json = json.loads(metadata_folder_itself)
        metadata_extra = json.loads(metadata_folder_extra)
        metadata = GoogleCloudFolderMetadata(metadata_json)

        assert isinstance(metadata, BaseGoogleCloudMetadata)
        assert metadata.provider == pd_settings.NAME
        assert metadata.path == '/test-folder-1/'
        assert metadata.name == 'test-folder-1'
        assert metadata.kind == 'folder'
        assert metadata.etag == 'CPCK4veFutgCEAE='
        assert metadata.extra == dict(metadata_extra)
