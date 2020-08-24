import pytest

from waterbutler.providers.nextcloud.metadata import NextcloudFileRevisionMetadata

from tests.providers.nextcloud.fixtures import (
    auth,
    credentials,
    settings,
    provider,
    file_metadata_object,
    file_metadata_object_less_info,
    folder_metadata_object,
    folder_metadata_object_less_info,
    revision_metadata_object
)



class TestFileMetadata:

    def test_file_metadata(self, file_metadata_object, provider):
        assert file_metadata_object.provider == provider.NAME
        assert file_metadata_object.name == 'dissertation.aux'
        assert file_metadata_object.path == '/Documents/dissertation.aux'
        assert file_metadata_object.materialized_path == '/Documents/dissertation.aux'
        assert file_metadata_object.kind == 'file'
        assert file_metadata_object.size == '3011'
        assert file_metadata_object.etag == '"a3c411808d58977a9ecd7485b5b7958e"'
        assert file_metadata_object.modified == 'Sun, 10 Jul 2016 23:28:31 GMT'
        assert file_metadata_object.modified_utc == '2016-07-10T23:28:31+00:00'
        assert file_metadata_object.created_utc is None
        assert file_metadata_object.fileid == '7923'
        assert file_metadata_object.content_type == 'application/octet-stream'
        assert file_metadata_object.extra == {}


        url = 'http://localhost:7777/v1/resources/guid0/providers/{}/Documents/dissertation.aux'.format(provider.NAME)
        json_api_links = {'delete': url,
                          'download': url,
                          'move': url,
                          'upload': url + '?kind=file'}

        assert file_metadata_object._json_api_links('guid0') == json_api_links

    def test_file_metadata_less_info(self, file_metadata_object_less_info, provider):
        assert file_metadata_object_less_info.provider == provider.NAME
        assert file_metadata_object_less_info.name == 'dissertation.aux'
        assert file_metadata_object_less_info.path == '/Documents/dissertation.aux'
        assert file_metadata_object_less_info.materialized_path == '/Documents/dissertation.aux'
        assert file_metadata_object_less_info.kind == 'file'
        assert file_metadata_object_less_info.size is None
        assert file_metadata_object_less_info.etag == '"a3c411808d58977a9ecd7485b5b7958e"'
        assert file_metadata_object_less_info.modified == 'Sun, 10 Jul 2016 23:28:31 GMT'
        assert file_metadata_object_less_info.modified_utc == '2016-07-10T23:28:31+00:00'
        assert file_metadata_object_less_info.created_utc is None
        assert file_metadata_object_less_info.fileid is None
        assert file_metadata_object_less_info.content_type is None
        assert file_metadata_object_less_info.extra == {}

        url = 'http://localhost:7777/v1/resources/guid0/providers/{}/Documents/dissertation.aux'.format(provider.NAME)
        json_api_links = {'delete': url,
                          'download': url,
                          'move': url,
                          'upload': url + '?kind=file'}

        assert file_metadata_object_less_info._json_api_links('guid0') == json_api_links


class TestFolderMetadata:

    def test_folder_metadata(self, folder_metadata_object, provider):
        assert folder_metadata_object.provider == provider.NAME
        assert folder_metadata_object.name == 'Documents'
        assert folder_metadata_object.path == '/'
        assert folder_metadata_object.materialized_path == '/'
        assert folder_metadata_object.kind == 'folder'
        assert folder_metadata_object.content_type == 'httpd/unix-directory'
        assert folder_metadata_object.size is None

        assert folder_metadata_object.etag == '"57688dd3584b0"'
        assert folder_metadata_object.extra == {}

        url = 'http://localhost:7777/v1/resources/guid0/providers/{}/'.format(provider.NAME)
        json_api_links = {'delete': url,
                          'move': url,
                          'new_folder': url + '?kind=folder',
                          'upload': url + '?kind=file'}

        assert folder_metadata_object._json_api_links('guid0') == json_api_links

    def test_folder_metadata_less_info(self, folder_metadata_object_less_info, provider):

        assert folder_metadata_object_less_info.provider == provider.NAME
        assert folder_metadata_object_less_info.name == 'Documents'
        assert folder_metadata_object_less_info.path == '/'
        assert folder_metadata_object_less_info.materialized_path == '/'
        assert folder_metadata_object_less_info.kind == 'folder'
        assert folder_metadata_object_less_info.content_type == 'httpd/unix-directory'
        assert folder_metadata_object_less_info.size is None
        assert folder_metadata_object_less_info.etag == '"a3c411808d58977a9ecd7485b5b7958e"'
        assert folder_metadata_object_less_info.extra == {}

        url = 'http://localhost:7777/v1/resources/guid0/providers/{}/'.format(provider.NAME)
        json_api_links = {'delete': url,
                          'move': url,
                          'new_folder': url + '?kind=folder',
                          'upload': url + '?kind=file'}

        assert folder_metadata_object_less_info._json_api_links('guid0') == json_api_links


class TestRevisionMetadata:

    etag = '"a3c411808d58977a9ecd7485b5b7958e"'
    version = etag.strip('"')

    def test_revision_metadata(self, revision_metadata_object):
        assert revision_metadata_object.version_identifier == 'revision'
        assert revision_metadata_object.version == self.version
        assert revision_metadata_object.modified == 'Sun, 10 Jul 2016 23:28:31 GMT'

        # hashes = {'hashes': {'md5': '', 'sha256': ''}}
        hashes = {}
        assert revision_metadata_object.extra == hashes

        serialized = {'extra': hashes,
                      'modified': 'Sun, 10 Jul 2016 23:28:31 GMT',
                      'modified_utc': '2016-07-10T23:28:31+00:00',
                      'version': self.version,
                      'versionIdentifier': 'revision'}

        assert revision_metadata_object.serialized() == serialized

        json_api_serialized = {'attributes':
                          {'extra': hashes,
                           'modified': 'Sun, 10 Jul 2016 23:28:31 GMT',
                           'modified_utc': '2016-07-10T23:28:31+00:00',
                           'version': self.version,
                           'versionIdentifier': 'revision'},
                      'id': self.version,
                      'type': 'file_versions'}

        assert revision_metadata_object.json_api_serialized() == json_api_serialized

    def test_revision_from_metadata(self, revision_metadata_object, file_metadata_object):
        revision = NextcloudFileRevisionMetadata.from_metadata(
            self.version, file_metadata_object)
        assert revision == revision_metadata_object
