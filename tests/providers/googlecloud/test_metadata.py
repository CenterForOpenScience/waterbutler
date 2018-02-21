import json
import logging

import pytest

from tests.providers.googlecloud.fixtures import (mock_auth, mock_creds, mock_settings,
                                                  meta_file_itself, meta_file_extra,
                                                  meta_folder_itself, )

from waterbutler.providers.googlecloud import (GoogleCloudProvider, BaseGoogleCloudMetadata,
                                               GoogleCloudFileMetadata, GoogleCloudFolderMetadata,)

logger = logging.getLogger(__name__)


@pytest.fixture()
def mock_provider(mock_auth, mock_creds, mock_settings):
    return GoogleCloudProvider(mock_auth, mock_creds, mock_settings)


class TestGoogleCloudFileMetadata:

    def test_file_metadata(self, meta_file_itself, meta_file_extra):

        # TODO: confirm that there is no need to test ``.serialized()`` and others.

        metadata_json = json.loads(meta_file_itself)
        metadata_extra = json.loads(meta_file_extra)
        metadata = GoogleCloudFileMetadata(metadata_json)

        assert isinstance(metadata, BaseGoogleCloudMetadata)
        assert metadata.provider == 'googlecloud'
        assert metadata.path == '/test-folder-1/DSC_0235.JPG'
        assert metadata.name == 'DSC_0235.JPG'
        assert metadata.kind == 'file'
        assert metadata.content_type == 'image/jpeg'
        assert metadata.modified == '2018-01-11T15:56:12.174Z'
        assert metadata.modified_utc == '2018-01-11T15:56:12+00:00'
        assert metadata.created_utc == '2018-01-11T15:56:12+00:00'
        assert metadata.etag == 'CM38iP+i0NgCEAE='
        assert metadata.size == 10724401
        assert metadata.extra == dict(metadata_extra)


class TestGoogleCloudFolderMetadata:

    def test_folder_metadata(self, meta_folder_itself):

        # TODO: confirm that there is no need to test ``.serialized()`` and others.

        metadata_json = json.loads(meta_folder_itself)
        metadata = GoogleCloudFolderMetadata(metadata_json)

        assert isinstance(metadata, BaseGoogleCloudMetadata)
        assert metadata.provider == 'googlecloud'
        assert metadata.path == '/test-folder-1/'
        assert metadata.name == 'test-folder-1'
        assert metadata.kind == 'folder'
