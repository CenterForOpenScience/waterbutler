import pytest

from tests.providers.s3.fixtures import (
    file_metadata_headers_object,
    file_header_metadata,
    file_metadata_object,
    folder_metadata_object,
    folder_key_metadata_object,
    revision_metadata_object
)


class TestFileMetadataHeaders:

    def test_file_metadata_headers(self, file_metadata_headers_object, file_header_metadata):
        assert not file_metadata_headers_object.is_folder
        assert file_metadata_headers_object.is_file
        assert file_metadata_headers_object.name == 'test-path'
        assert file_metadata_headers_object.path == '/test-path'
        assert file_metadata_headers_object.materialized_path == '/test-path'
        assert file_metadata_headers_object.kind == 'file'
        assert file_metadata_headers_object.provider == 's3'
        assert file_metadata_headers_object.size == '9001'
        assert file_metadata_headers_object.size_as_int == 9001
        assert type(file_metadata_headers_object.size_as_int) == int
        assert file_metadata_headers_object.content_type == 'binary/octet-stream'
        assert file_metadata_headers_object.modified == 'SomeTime'
        assert not file_metadata_headers_object.created_utc
        assert file_metadata_headers_object.etag == 'fba9dede5f27731c9771645a39863328'

        extra = {
            'md5': 'fba9dede5f27731c9771645a39863328',
            'encryption': 'AES256',
            'hashes': {
                'md5': 'fba9dede5f27731c9771645a39863328',
            },
        }

        assert file_metadata_headers_object.extra == extra


class TestFileMetadata:

    def test_file_metadata(self, file_metadata_object):
        assert file_metadata_object.provider == 's3'

        assert not file_metadata_object.is_folder
        assert file_metadata_object.is_file
        assert file_metadata_object.kind == 'file'

        assert file_metadata_object.name == 'my-image.jpg'
        assert file_metadata_object.path == '/my-image.jpg'
        assert file_metadata_object.materialized_path == '/my-image.jpg'
        assert file_metadata_object.size == 434234
        assert file_metadata_object.size_as_int == 434234
        assert type(file_metadata_object.size_as_int) == int
        assert file_metadata_object.modified == '2009-10-12T17:50:30.000Z'
        assert file_metadata_object.etag == 'fba9dede5f27731c9771645a39863328'
        assert not file_metadata_object.created_utc
        assert not file_metadata_object.content_type

        extra = {
            'md5': 'fba9dede5f27731c9771645a39863328',
            'hashes': {
                'md5': 'fba9dede5f27731c9771645a39863328',
            },
        }

        assert file_metadata_object.extra == extra

        links = {'delete': 'http://localhost:7777/v1/resources/guid0/providers/s3/my-image.jpg',
          'download': 'http://localhost:7777/v1/resources/guid0/providers/s3/my-image.jpg',
          'move': 'http://localhost:7777/v1/resources/guid0/providers/s3/my-image.jpg',
          'upload': 'http://localhost:7777/v1/resources/guid0/providers/s3/my-image.jpg?kind=file'}

        assert file_metadata_object._json_api_links('guid0') == links


class TestFolderMetadata:

    def test_folder_metadata(self, folder_metadata_object):
        assert folder_metadata_object.provider == 's3'

        assert folder_metadata_object.is_folder
        assert not folder_metadata_object.is_file
        assert folder_metadata_object.kind == 'folder'

        assert folder_metadata_object.name == 'photos'
        assert folder_metadata_object.path == '/photos/'
        assert folder_metadata_object.materialized_path == '/photos/'

        assert not folder_metadata_object.children

        links = {'delete': 'http://localhost:7777/v1/resources/guid0/providers/s3/photos/',
                 'move': 'http://localhost:7777/v1/resources/guid0/providers/s3/photos/',
                 'new_folder': 'http://localhost:7777/v1/resources/guid0/providers/s3/photos/'
                               '?kind=folder',
                 'upload': 'http://localhost:7777/v1/resources/guid0/providers/s3/photos/'
                           '?kind=file'}

        assert folder_metadata_object._json_api_links('guid0') == links

class TestFolderKeyMetadata:

    def test_folder_key_metadata(self, folder_key_metadata_object):
        assert folder_key_metadata_object.is_folder
        assert not folder_key_metadata_object.is_file
        assert folder_key_metadata_object.kind == 'folder'

        assert folder_key_metadata_object.name == 'naptime'
        assert folder_key_metadata_object.path == '/naptime/'

        assert not folder_key_metadata_object.children

        links = {'delete': 'http://localhost:7777/v1/resources/guid0/providers/s3/naptime/',
                 'move': 'http://localhost:7777/v1/resources/guid0/providers/s3/naptime/',
                 'new_folder': 'http://localhost:7777/v1/resources/guid0/providers/s3/naptime/'
                               '?kind=folder',
                 'upload': 'http://localhost:7777/v1/resources/guid0/providers/s3/naptime/'
                           '?kind=file'}

        assert folder_key_metadata_object._json_api_links('guid0') == links


class TestRevisionsMetadata:

    def test_revisions_metadata(self, revision_metadata_object):

        assert revision_metadata_object.version_identifier == 'version'
        assert revision_metadata_object.version == 'Latest'
        assert revision_metadata_object.modified == '2009-10-12T17:50:30.000Z'
        assert revision_metadata_object.extra == {'md5': 'fba9dede5f27731c9771645a39863328'}

    def test_revisions_metadata_not_lastest(self, revision_metadata_object):
        revision_metadata_object.raw['IsLatest'] = 'false'

        assert revision_metadata_object.version == '3/L4kqtJl40Nr8X8gdRQBpUMLUo'

