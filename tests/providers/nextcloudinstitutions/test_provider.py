import pytest

from tests import utils

from tests.providers.nextcloud.test_provider import (
    TestProviderConstruction,
    TestValidatePath,
    TestCRUD,
    TestIntraMoveCopy,
    TestMetadata,
    TestRevisions,
    TestOperations,
    file_like,
    file_stream
)

from tests.providers.nextcloud.fixtures import (
    auth,
    settings,
    credentials,
    credentials_2,
    credentials_host_with_trailing_slash,
    file_content,
    file_metadata,
    file_metadata_2,
    file_revision_metadata,
    folder_contents_metadata,
    file_metadata_object,
    file_metadata_object_2,
    folder_list,
    folder_metadata,
    file_metadata_unparsable_response,
    file_revision_metadata_error_response,
    moved_folder_metadata,
    moved_parent_folder_metadata
)

from tests.providers.nextcloudinstitutions.fixtures import (
    provider,
    provider_different_credentials
)


class TestProviderConstruction2(TestProviderConstruction):
    pass

class TestValidatePath2(TestValidatePath):
    pass

class TestCRUD2(TestCRUD):
    pass

class TestIntraMoveCopy2(TestIntraMoveCopy):
    pass

class TestMetadata2(TestMetadata):
    pass

class TestRevisions2(TestRevisions):
    pass

class TestOperations2(TestOperations):
    pass
