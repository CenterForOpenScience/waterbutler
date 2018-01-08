import os

import pytest

from waterbutler.providers.googledrive.provider import GoogleDrivePath
from waterbutler.providers.googledrive.metadata import (GoogleDriveRevision,
                                                        GoogleDriveFileMetadata,
                                                        GoogleDriveFolderMetadata)

from tests.providers.googledrive.fixtures import (error_fixtures, root_provider_fixtures,
                                                  revision_fixtures, sharing_fixtures)


@pytest.fixture
def basepath():
    return GoogleDrivePath('/conrad')


class TestMetadata:

    def test_file_metadata_drive(self, basepath, root_provider_fixtures):
        file = root_provider_fixtures['list_file']['files'][0]
        path = basepath.child(file['name'])
        parsed = GoogleDriveFileMetadata(file, path)

        assert parsed.provider == 'googledrive'
        assert parsed.id == file['id']
        assert path.name == file['name']
        assert parsed.name == file['name']
        assert parsed.size == file['size']
        assert parsed.modified == file['modifiedTime']
        assert parsed.content_type == file['mimeType']
        assert parsed.extra == {
            'revisionId': file['version'],
            'webView': file['webViewLink'],
            'hashes': {'md5': file['md5Checksum']},
        }
        assert parsed.path == '/' + os.path.join(*[x.raw for x in path.parts])
        assert parsed.materialized_path == str(path)
        assert parsed.is_google_doc is False
        assert parsed.export_name == file['name']

    def test_file_metadata_drive_slashes(self, basepath, root_provider_fixtures):
        file = root_provider_fixtures['file_forward_slash']
        path = basepath.child(file['name'])
        parsed = GoogleDriveFileMetadata(file, path)

        assert parsed.provider == 'googledrive'
        assert parsed.id == file['id']
        assert parsed.name == file['name']
        assert parsed.name == path.name
        assert parsed.size == file['size']
        assert parsed.modified == file['modifiedTime']
        assert parsed.content_type == file['mimeType']
        assert parsed.extra == {
            'revisionId': file['version'],
            'webView': file['webViewLink'],
            'hashes': {'md5': file['md5Checksum']},
        }
        assert parsed.path == '/' + os.path.join(*[x.raw for x in path.parts])
        assert parsed.materialized_path == str(path)
        assert parsed.is_google_doc is False
        assert parsed.export_name == file['name']

    def test_file_metadata_docs(self, basepath, root_provider_fixtures):
        file = root_provider_fixtures['docs_file_metadata']
        path = basepath.child(file['name'])
        parsed = GoogleDriveFileMetadata(file, path)

        assert parsed.name == file['name'] + '.gdoc'
        assert parsed.extra == {
            'revisionId': file['version'],
            'downloadExt': '.docx',
            'webView': file['webViewLink'],
        }
        assert parsed.is_google_doc is True
        assert parsed.export_name == file['name'] + '.docx'

    def test_folder_metadata(self, root_provider_fixtures):
        file = root_provider_fixtures['folder_metadata']
        path = GoogleDrivePath('/we/love/you/conrad').child(file['name'], folder=True)
        parsed = GoogleDriveFolderMetadata(file, path)

        assert parsed.provider == 'googledrive'
        assert parsed.id == file['id']
        assert parsed.name == file['name']
        assert parsed.extra == {'revisionId': file['version']}
        assert parsed.path == '/' + os.path.join(*[x.raw for x in path.parts]) + '/'
        assert parsed.materialized_path == str(path)
        assert parsed.export_name == file['name']

    def test_folder_metadata_slash(self, root_provider_fixtures):
        file = root_provider_fixtures['folder_metadata_forward_slash']
        path = GoogleDrivePath('/we/love/you/conrad').child(file['name'], folder=True)
        parsed = GoogleDriveFolderMetadata(file, path)

        assert parsed.provider == 'googledrive'
        assert parsed.id == file['id']
        assert parsed.name == file['name']
        assert parsed.extra == {'revisionId': file['version']}
        assert parsed.path == '/' + os.path.join(*[x.raw for x in path.parts]) + '/'
        assert parsed.materialized_path == str(path)
        assert parsed.export_name == file['name']

    def test_revision_metadata(self, revision_fixtures):
        revision = revision_fixtures['revision_metadata']
        parsed = GoogleDriveRevision(revision)

        assert parsed.version_identifier == 'revision'
        assert parsed.version == revision['id']
        assert parsed.modified == revision['modifiedTime']
