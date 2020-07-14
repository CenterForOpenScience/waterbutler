import pytest

from waterbutler.providers.nextcloud.metadata import (
    NextcloudFileRevisionMetadata
)

from tests.providers.nextcloud.test_metadata import (
    TestFileMetadata,
    TestFolderMetadata,
    TestRevisionMetadata
)

from tests.providers.nextcloud.fixtures import (
    auth,
    credentials,
    settings,
    file_metadata_object,
    file_metadata_object_less_info,
    folder_metadata_object,
    folder_metadata_object_less_info,
    revision_metadata_object
)

from tests.providers.nextcloudinstitutions.fixtures import (
    provider
)

class TestFileMetadata2(TestFileMetadata):
    pass

class TestFolderMetadata2(TestFolderMetadata):
    pass

class TestRevisionMetadata2(TestRevisionMetadata):
    pass
