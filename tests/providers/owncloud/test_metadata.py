import pytest

from waterbutler.providers.owncloud.metadata import OwnCloudFileRevisionMetadata

from tests.providers.owncloud.fixtures import (
    file_metadata_object,
    file_metadata_object_less_info,
    folder_metadata_object,
    folder_metadata_object_less_info,
    revision_metadata_object
)



class TestFileMetadata:

    def test_file_metadata(self, file_metadata_object):
        assert file_metadata_object.provider == 'owncloud'
        assert file_metadata_object.name == 'dissertation.aux'
        assert file_metadata_object.path == '/Documents/dissertation.aux'
        assert file_metadata_object.materialized_path == '/Documents/dissertation.aux'
        assert file_metadata_object.kind == 'file'
        assert file_metadata_object.size == '3011'
        assert file_metadata_object.size_as_int == 3011
        assert type(file_metadata_object.size_as_int) == int
        assert file_metadata_object.etag == '"a3c411808d58977a9ecd7485b5b7958e"'
        assert file_metadata_object.modified == 'Sun, 10 Jul 2016 23:28:31 GMT'
        assert file_metadata_object.modified_utc == '2016-07-10T23:28:31+00:00'
        assert file_metadata_object.created_utc is None
        assert file_metadata_object.content_type == 'application/octet-stream'
        assert file_metadata_object.extra == {}


        json_api_links = {'delete': 'http://localhost:7777/v1/resources/guid0/providers/owncloud/'
                                    'Documents/dissertation.aux',
                          'download': 'http://localhost:7777/v1/resources/guid0/providers/owncloud/'
                                      'Documents/dissertation.aux',
                          'move': 'http://localhost:7777/v1/resources/guid0/providers/owncloud/'
                                  'Documents/dissertation.aux',
                          'upload': 'http://localhost:7777/v1/resources/guid0/providers/owncloud/'
                                    'Documents/dissertation.aux?kind=file'}

        assert file_metadata_object._json_api_links('guid0') == json_api_links

    def test_file_metadata_less_info(self, file_metadata_object_less_info):
        assert file_metadata_object_less_info.provider == 'owncloud'
        assert file_metadata_object_less_info.name == 'dissertation.aux'
        assert file_metadata_object_less_info.path == '/Documents/dissertation.aux'
        assert file_metadata_object_less_info.materialized_path == '/Documents/dissertation.aux'
        assert file_metadata_object_less_info.kind == 'file'
        assert file_metadata_object_less_info.size is None
        assert file_metadata_object_less_info.size_as_int is None
        assert file_metadata_object_less_info.etag == '"a3c411808d58977a9ecd7485b5b7958e"'
        assert file_metadata_object_less_info.modified == 'Sun, 10 Jul 2016 23:28:31 GMT'
        assert file_metadata_object_less_info.modified_utc == '2016-07-10T23:28:31+00:00'
        assert file_metadata_object_less_info.created_utc is None
        assert file_metadata_object_less_info.content_type is None
        assert file_metadata_object_less_info.extra == {}

        json_api_links = {'delete': 'http://localhost:7777/v1/resources/guid0/providers/owncloud/'
                                    'Documents/dissertation.aux',
                          'download': 'http://localhost:7777/v1/resources/guid0/providers/owncloud/'
                                      'Documents/dissertation.aux',
                          'move': 'http://localhost:7777/v1/resources/guid0/providers/owncloud/'
                                  'Documents/dissertation.aux',
                          'upload': 'http://localhost:7777/v1/resources/guid0/providers/owncloud/'
                                    'Documents/dissertation.aux?kind=file'}

        assert file_metadata_object_less_info._json_api_links('guid0') == json_api_links


class TestFolderMetadata:

    def test_folder_metadata(self, folder_metadata_object):
        assert folder_metadata_object.provider == 'owncloud'
        assert folder_metadata_object.name == 'Documents'
        assert folder_metadata_object.path == '/'
        assert folder_metadata_object.materialized_path == '/'
        assert folder_metadata_object.kind == 'folder'
        assert folder_metadata_object.content_type == 'httpd/unix-directory'
        assert folder_metadata_object.size is None

        assert folder_metadata_object.etag == '"57688dd3584b0"'
        assert folder_metadata_object.extra == {}

        json_api_links = {'delete': 'http://localhost:7777/v1/resources/guid0/providers/owncloud/',
                          'move': 'http://localhost:7777/v1/resources/guid0/providers/owncloud/',
                          'new_folder': 'http://localhost:7777/v1/resources/guid0/providers'
                                        '/owncloud/?kind=folder',
                          'upload': 'http://localhost:7777/v1/resources/guid0/providers/owncloud/'
                                    '?kind=file'}

        assert folder_metadata_object._json_api_links('guid0') == json_api_links

    def test_folder_metadata_less_info(self, folder_metadata_object_less_info):

        assert folder_metadata_object_less_info.provider == 'owncloud'
        assert folder_metadata_object_less_info.name == 'Documents'
        assert folder_metadata_object_less_info.path == '/'
        assert folder_metadata_object_less_info.materialized_path == '/'
        assert folder_metadata_object_less_info.kind == 'folder'
        assert folder_metadata_object_less_info.content_type == 'httpd/unix-directory'
        assert folder_metadata_object_less_info.size is None
        assert folder_metadata_object_less_info.etag == '"a3c411808d58977a9ecd7485b5b7958e"'
        assert folder_metadata_object_less_info.extra == {}

        json_api_links = {'delete': 'http://localhost:7777/v1/resources/guid0/providers/owncloud/',
                          'move': 'http://localhost:7777/v1/resources/guid0/providers/owncloud/',
                          'new_folder': 'http://localhost:7777/v1/resources/guid0/providers/'
                                        'owncloud/?kind=folder',
                          'upload': 'http://localhost:7777/v1/resources/guid0/providers/'
                                    'owncloud/?kind=file'}

        assert folder_metadata_object_less_info._json_api_links('guid0') == json_api_links


class TestRevisionMetadata:

    def test_revision_metadata(self, revision_metadata_object):
        assert revision_metadata_object.version_identifier == 'revision'
        assert revision_metadata_object.version == 'latest'
        assert revision_metadata_object.modified == 'Sun, 10 Jul 2016 23:28:31 GMT'
        assert revision_metadata_object.extra == {}

        serialized = {'extra': {},
                      'modified': 'Sun, 10 Jul 2016 23:28:31 GMT',
                      'modified_utc': '2016-07-10T23:28:31+00:00',
                      'version': 'latest',
                      'versionIdentifier': 'revision'}

        assert revision_metadata_object.serialized() == serialized

        json_api_serialized = {'attributes':
                          {'extra': {},
                           'modified': 'Sun, 10 Jul 2016 23:28:31 GMT',
                           'modified_utc': '2016-07-10T23:28:31+00:00',
                           'version': 'latest',
                           'versionIdentifier': 'revision'},
                      'id': 'latest',
                      'type': 'file_versions'}

        assert revision_metadata_object.json_api_serialized() == json_api_serialized

    def test_revision_from_metadata(self, revision_metadata_object, file_metadata_object):
        revision = OwnCloudFileRevisionMetadata.from_metadata(file_metadata_object)
        assert revision == revision_metadata_object
