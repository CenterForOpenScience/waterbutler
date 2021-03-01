import pytest

from tests import utils

import boto3
from moto import mock_s3

from waterbutler.providers.ociinstitutions import OCIInstitutionsProvider
from tests.providers.s3compatb3.test_provider import (
    TestProviderConstruction,
    TestValidatePath,
    # TestCRUD,
    # TestMetadata,
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
        'bucket': 'that_kerning',
        'prefix': base_prefix,
        'encrypt_uploads': False
    }

@pytest.fixture
def provider(auth, credentials, settings):
    # return OCIInstitutionsProvider(auth, credentials, settings)
    boto3.DEFAULT_SESSION = None
    with mock_s3():
        provider = OCIInstitutionsProvider(auth, credentials, settings)
        s3client = boto3.client('s3')
        s3client.create_bucket(Bucket=provider.bucket.name)
        s3 = boto3.resource('s3')
        provider.connection.s3 = s3
        provider.bucket = s3.Bucket(provider.bucket.name)
        return provider

class TestProviderConstruction2(TestProviderConstruction):
    pass

class TestValidatePath2(TestValidatePath):
    pass

# class TestCRUD2(TestCRUD):
#     pass

# class TestMetadata2(TestMetadata):
#     pass

class TestCreateFolder2(TestCreateFolder):
    pass

class TestOperations2(TestOperations):
    pass
