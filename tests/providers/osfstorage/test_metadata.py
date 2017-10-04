import pytest

from tests.providers.osfstorage.fixtures import (file_path,
                                                 file_metadata,
                                                 file_metadata_object,
                                                 folder_metadata,
                                                 folder_metadata_object,
                                                 revisions_metadata,
                                                 revision_metadata_object)

class TestFileMetadata:

    def test_file_metadata(self, file_metadata, file_metadata_object):

        assert file_metadata_object.provider == 'osfstorage'
        assert file_metadata_object.name == 'doc.rst'
        assert file_metadata_object.path == '/59a9b628b7d1c903ab5a8f52'
        assert str(file_metadata_object.materialized_path) == '/doc.rst'
        assert file_metadata_object.size == 5596
        assert file_metadata_object.size_as_int == 5596
        assert file_metadata_object.modified == '2017-09-01T19:34:00.175741+00:00'
        assert file_metadata_object.kind == 'file'
        assert file_metadata_object.content_type == None
        assert file_metadata_object.etag == '1::/59a9b628b7d1c903ab5a8f52'
        assert file_metadata_object.raw == file_metadata

        extra_expected = {
            'checkout': None,
            'downloads': 0,
            'guid': None,
            'hashes': {
                'md5': 'eb3f7cc15ba7b6effb2186284185c5cf',
                'sha256': '043be9ff919762f0dc36fff0222cd90c753ce28b39feb52112be9360c476ef88'
            },
            'version': 1,
            'latestVersionSeen': None
        }

        assert file_metadata_object.extra == extra_expected

    def test_file_metadata_modified_utc(self, file_metadata_object):

        # Case 1: all relevant metadata provided
        assert file_metadata_object.modified_utc == '2017-09-01T19:34:00.175741+00:00'

        # Case 2: no relevant metadata provided
        file_metadata_object.raw['modified'] = None
        assert not file_metadata_object.modified_utc

        # Case 3: no timezone metadata provided
        file_metadata_object.raw['modified'] = '2017-09-01T19:34:00.175741'
        assert file_metadata_object.modified_utc == '2017-09-01T19:34:00.175741+00:00'

    def test_file_metadata_created_utc(self, file_metadata_object):

        # Case 1: all relevant metadata provided
        assert file_metadata_object.created_utc == '2017-09-01T19:34:00.175741+00:00'

        # Case 2: no relevant metadata provided
        file_metadata_object.raw['created'] = None
        assert not file_metadata_object.created_utc

        # Case 3: no timezone metadata provided
        file_metadata_object.raw['created'] = '2017-09-01T19:34:00.175741'
        assert file_metadata_object.created_utc == '2017-09-01T19:34:00.175741+00:00'


class TestFolderMetadata:

    def test_folder_metadata(self, folder_metadata_object):

        assert folder_metadata_object.provider == 'osfstorage'
        assert folder_metadata_object.name == 'New Folder'
        assert folder_metadata_object.path == '/59c0054cb7d1c90114c456af/'
        assert str(folder_metadata_object.materialized_path) == '/New Folder/'
        assert folder_metadata_object.kind == 'folder'
        assert not folder_metadata_object.etag

        expect_raw = {'id': '59c0054cb7d1c90114c456af',
                      'kind': 'folder',
                      'name': 'New Folder',
                      'path': '/59c0054cb7d1c90114c456af/'}

        assert folder_metadata_object.raw == expect_raw


class TestRevisionMetadata:

    def test_revision_metadata(self, revisions_metadata, revision_metadata_object):
        revision_metadata = revisions_metadata['revisions'][0]

        assert revision_metadata_object.modified == '2017-09-01T19:34:34.981415+00:00'
        assert revision_metadata_object.version_identifier == 'version'
        assert revision_metadata_object.version == '2'
        assert revision_metadata_object.raw == revision_metadata

        expected_extra = {
            'downloads': 0,
            'hashes': {
                'md5': 'c4ca4238a0b923820dcc509a6f75849b',
                'sha256': '6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b'},
            'user': {
                'name': 'Dr. Fake, DDS ΩΩΩ',
                'url': '/64yvj/'
            }
        }
        assert revision_metadata_object.extra == expected_extra
