import pytest

from tests import utils

from waterbutler.providers.s3compatinstitutions import S3CompatInstitutionsProvider
from tests.providers.s3compat.test_provider import (
    TestProviderConstruction,
    TestValidatePath,
    TestCRUD,
    TestMetadata,
    TestCreateFolder,
    TestOperations,
    auth,
    credentials,
    mock_time,
    file_content,
    file_like,
    file_stream,
    folder_metadata,
    just_a_folder_metadata,
    contents_and_self,
    folder_empty_metadata,
    file_metadata,
    version_metadata,
    location_response,
    list_objects_response,
    bulk_delete_body,
    build_folder_params,
)

@pytest.fixture
def base_prefix():
    return 'testrootdir/'

@pytest.fixture
def settings(base_prefix):
    return {
        'bucket': 'that kerning',
        'prefix': base_prefix,
        'encrypt_uploads': False
    }

@pytest.fixture
def provider(auth, credentials, settings):
    return S3CompatInstitutionsProvider(auth, credentials, settings)

class TestProviderConstruction2(TestProviderConstruction):
    pass

class TestValidatePath2(TestValidatePath):
    pass

class TestCRUD2(TestCRUD):
    pass

class TestMetadata2(TestMetadata):
    pass

class TestCreateFolder2(TestCreateFolder):
    pass

class TestOperations2(TestOperations):
    pass
