import pytest

from waterbutler.providers.dropbox.metadata import (DropboxFileMetadata,
                                                    DropboxFolderMetadata,
                                                    DropboxRevision)

from tests.providers.dropbox.test_metadata import TestDropboxMetadata
from tests.providers.dropbox.fixtures import provider_fixtures, revision_fixtures, auth, credentials
from tests.providers.dropboxbusiness.fixtures import settings, provider

class TestDropboxBusinessMetadata(TestDropboxMetadata):
    pass
