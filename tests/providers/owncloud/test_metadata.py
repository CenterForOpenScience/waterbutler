import pytest

from waterbutler.providers.owncloud.metadata import OwnCloudFileMetadata, \
    OwnCloudFolderMetadata, \
    OwnCloudFileRevisionMetadata

@pytest.fixture
def file_metadata_object():
    return OwnCloudFileMetadata('dissertation.aux','/owncloud/remote.php/webdav/Documents/phile',
            {'{DAV:}getetag':'&quot;a3c411808d58977a9ecd7485b5b7958e&quot;',
            '{DAV:}getlastmodified':'Sun, 10 Jul 2016 23:28:31 GMT',
            '{DAV:}getcontentlength':3011,
            '{DAV:}getcontenttype': 'test-type'})

@pytest.fixture
def file_metadata_object_less_info():
    return OwnCloudFileMetadata('dissertation.aux','/owncloud/remote.php/webdav/Documents/phile',
            {'{DAV:}getetag':'&quot;a3c411808d58977a9ecd7485b5b7958e&quot;',
            '{DAV:}getlastmodified':'Sun, 10 Jul 2016 23:28:31 GMT'})


@pytest.fixture
def folder_metadata_object():
    return OwnCloudFolderMetadata('dissertation.aux','/owncloud/remote.php/webdav/Documents/phile',
            {'{DAV:}getetag':'&quot;a3c411808d58977a9ecd7485b5b7958e&quot;',
            '{DAV:}getlastmodified':'Sun, 10 Jul 2016 23:28:31 GMT',
            '{DAV:}getcontentlength':3011,
            '{DAV:}getcontenttype': 'test-type'})

@pytest.fixture
def folder_metadata_object_less_info():
    return OwnCloudFolderMetadata('dissertation.aux','/owncloud/remote.php/webdav/Documents/phile',
            {'{DAV:}getetag':'&quot;a3c411808d58977a9ecd7485b5b7958e&quot;',
            '{DAV:}getlastmodified':'Sun, 10 Jul 2016 23:28:31 GMT',
            '{DAV:}getcontentlength':3011})


@pytest.fixture
def revision_metadata_object(file_metadata_object):
    return OwnCloudFileRevisionMetadata.from_metadata(file_metadata_object)


class TestFileMetadata:

    def test_file_metadata(self, file_metadata_object, file_metadata_object_less_info):

        assert file_metadata_object.provider == 'owncloud'
        assert file_metadata_object.size == '3011'
        assert not file_metadata_object_less_info.size
        assert file_metadata_object.etag == '&quot;a3c411808d58977a9ecd7485b5b7958e&quot;'
        assert not file_metadata_object.created_utc
        assert file_metadata_object.content_type == 'test-type'
        assert not file_metadata_object_less_info.content_type

class TestFolderMetadata:

    def test_folder_metadata(self, folder_metadata_object, folder_metadata_object_less_info):

        assert folder_metadata_object.content_type == 'test-type'
        assert folder_metadata_object_less_info.content_type == 'httpd/unix-directory'

class TestRevisionMetadata:

    def test_revision_metadata(self, revision_metadata_object):
        revision_metadata_object.version_identifier == 'revision'
        revision_metadata_object.version == 'lasest'