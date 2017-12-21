import pytest

import os

from waterbutler.providers.googledrive.provider import GoogleDrivePath
from waterbutler.providers.googledrive.provider import GoogleDrivePathPart
from waterbutler.providers.googledrive.metadata import GoogleDriveRevision
from waterbutler.providers.googledrive.metadata import GoogleDriveFileMetadata
from waterbutler.providers.googledrive.metadata import GoogleDriveFolderMetadata

from tests.providers.googledrive.fixtures import(
    error_fixtures,
    root_provider_fixtures,
    revision_fixtures,
    sharing_fixtures,
)


@pytest.fixture
def basepath():
    return GoogleDrivePath('/conrad')


class TestMetadata:

    def test_file_metadata_drive(self, basepath, root_provider_fixtures):
        item = root_provider_fixtures['list_file']['items'][0]
        path = basepath.child(item['title'])
        parsed = GoogleDriveFileMetadata(item, path)

        assert parsed.provider == 'googledrive'
        assert parsed.id == item['id']
        assert path.name == item['title']
        assert parsed.name == item['title']
        assert parsed.size_as_int == 918668
        assert type(parsed.size_as_int) == int
        assert parsed.size == item['fileSize']
        assert parsed.modified == item['modifiedDate']
        assert parsed.content_type == item['mimeType']
        assert parsed.extra == {
            'revisionId': item['version'],
            'webView': item['alternateLink'],
            'hashes': {'md5': item['md5Checksum']},
        }
        assert parsed.path == '/' + os.path.join(*[x.raw for x in path.parts])
        assert parsed.materialized_path == str(path)
        assert parsed.is_google_doc is False
        assert parsed.export_name == item['title']

    def test_file_metadata_drive_slashes(self, basepath, root_provider_fixtures):
        item = root_provider_fixtures['file_forward_slash']
        path = basepath.child(item['title'])
        parsed = GoogleDriveFileMetadata(item, path)

        assert parsed.provider == 'googledrive'
        assert parsed.id == item['id']
        assert parsed.name == item['title']
        assert parsed.name == path.name
        assert parsed.size == item['fileSize']
        assert parsed.size_as_int == 918668
        assert type(parsed.size_as_int) == int
        assert parsed.modified == item['modifiedDate']
        assert parsed.content_type == item['mimeType']
        assert parsed.extra == {
            'revisionId': item['version'],
            'webView': item['alternateLink'],
            'hashes': {'md5': item['md5Checksum']},
        }
        assert parsed.path == '/' + os.path.join(*[x.raw for x in path.parts])
        assert parsed.materialized_path == str(path)
        assert parsed.is_google_doc is False
        assert parsed.export_name == item['title']

    def test_file_metadata_docs(self, basepath, root_provider_fixtures):
        item = root_provider_fixtures['docs_file_metadata']
        path = basepath.child(item['title'])
        parsed = GoogleDriveFileMetadata(item, path)

        assert parsed.name == item['title'] + '.gdoc'
        assert parsed.extra == {
            'revisionId': item['version'],
            'downloadExt': '.docx',
            'webView': item['alternateLink'],
        }
        assert parsed.is_google_doc is True
        assert parsed.export_name == item['title'] + '.docx'

    def test_folder_metadata(self, root_provider_fixtures):
        item = root_provider_fixtures['folder_metadata']
        path = GoogleDrivePath('/we/love/you/conrad').child(item['title'], folder=True)
        parsed = GoogleDriveFolderMetadata(item, path)

        assert parsed.provider == 'googledrive'
        assert parsed.id == item['id']
        assert parsed.name == item['title']
        assert parsed.extra == {'revisionId': item['version']}
        assert parsed.path == '/' + os.path.join(*[x.raw for x in path.parts]) + '/'
        assert parsed.materialized_path == str(path)
        assert parsed.export_name == item['title']

    def test_folder_metadata_slash(self, root_provider_fixtures):
        item = root_provider_fixtures['folder_metadata_forward_slash']
        path = GoogleDrivePath('/we/love/you/conrad').child(item['title'], folder=True)
        parsed = GoogleDriveFolderMetadata(item, path)

        assert parsed.provider == 'googledrive'
        assert parsed.id == item['id']
        assert parsed.name == item['title']
        assert parsed.extra == {'revisionId': item['version']}
        assert parsed.path == '/' + os.path.join(*[x.raw for x in path.parts]) + '/'
        assert parsed.materialized_path == str(path)
        assert parsed.export_name == item['title']

    def test_revision_metadata(self, revision_fixtures):
        item = revision_fixtures['revision_metadata']
        parsed = GoogleDriveRevision(item)
        assert parsed.version_identifier == 'revision'
        assert parsed.version == item['id']
        assert parsed.modified == item['modifiedDate']
