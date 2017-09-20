import pytest
from collections import OrderedDict

from tests.providers.s3.test_provider import file_header_metadata, folder_single_thing_metadata
from waterbutler.providers.s3.metadata import S3FileMetadataHeaders, S3FileMetadata, S3FolderKeyMetadata, S3Revision


@pytest.fixture
def file_metadata_headers_object(file_header_metadata):
    return S3FileMetadataHeaders('/test-path', file_header_metadata)


@pytest.fixture
def file_metadata_object():
    content = OrderedDict(Key='my-image.jpg',
                          LastModified='2009-10-12T17:50:30.000Z',
                          ETag="fba9dede5f27731c9771645a39863328",
                          Size='434234',
                          StorageClass='STANDARD')

    return S3FileMetadata(content)

@pytest.fixture
def folder_key_metadata_object():
    content = OrderedDict(Key='naptime/',
                          LastModified='2009-10-12T17:50:30.000Z',
                          ETag='"fba9dede5f27731c9771645a39863328"',
                          Size='0',
                          StorageClass='STANDARD')


    return S3FolderKeyMetadata(content)

@pytest.fixture
def revision_metadata_object():
    content = OrderedDict(Key='single-version.file',
                VersionId='3/L4kqtJl40Nr8X8gdRQBpUMLUo',
                IsLatest='true',
                LastModified='2009-10-12T17:50:30.000Z',
                ETag='"fba9dede5f27731c9771645a39863328"',
                Size=434234,
                StorageClass='STANDARD',
                Owner=OrderedDict(ID='75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a',
                                  DisplayName='mtd@amazon.com')
                )

    return S3Revision(content)


class TestFileMetadataHeaders:

    def test_file_metadata_headers(self, file_metadata_headers_object, file_header_metadata):
        assert file_metadata_headers_object.provider == 's3'
        assert file_metadata_headers_object.size == file_header_metadata['CONTENT-LENGTH']
        assert file_metadata_headers_object.content_type == file_header_metadata['CONTENT-TYPE']
        assert file_metadata_headers_object.modified == file_header_metadata['LAST-MODIFIED']
        assert not file_metadata_headers_object.created_utc
        assert file_metadata_headers_object.etag == file_header_metadata['ETAG'].replace('"', '')

class TestFileMetadata:

    def test_file_metadata(self, file_metadata_object):
        assert file_metadata_object.provider == 's3'
        assert file_metadata_object.size == 434234
        assert file_metadata_object.modified == '2009-10-12T17:50:30.000Z'
        assert file_metadata_object.etag == "fba9dede5f27731c9771645a39863328".replace('"', '')
        assert not file_metadata_object.created_utc
        assert not file_metadata_object.content_type

class TestFolderKeyMetadata:

    def test_folder_key_metadata(self, folder_key_metadata_object):
        folder_key_metadata_object.name == 'naptime'
        folder_key_metadata_object.path == '/naptime'


class TestRevisionsMetadata:

    def test_revisions_metadata(self, revision_metadata_object):
        assert revision_metadata_object.version_identifier == 'version'
        assert revision_metadata_object.version == 'Latest'
        assert revision_metadata_object.modified == '2009-10-12T17:50:30.000Z'