import pytest

from waterbutler.providers.osfstorage.metadata import OsfStorageRevisionMetadata, OsfStorageFileMetadata
from waterbutler.core.path import WaterButlerPath

from tests.providers.osfstorage.test_provider import file_metadata, file_path, revisions_metadata


@pytest.fixture
def file_metadata_object(file_metadata):
    path = WaterButlerPath('/' + file_metadata['name'])
    return OsfStorageFileMetadata(file_metadata, path)


@pytest.fixture
def revision_metadata_object(revisions_metadata):
    return OsfStorageRevisionMetadata(revisions_metadata['revisions'][0])


class TestFileMetadata:

    def test_file_metadata(self, file_metadata, file_metadata_object):

        assert file_metadata_object.provider == 'osfstorage'
        assert file_metadata_object.name == file_metadata['name']
        assert file_metadata_object.size == file_metadata['size']
        assert file_metadata_object.modified == file_metadata['modified']
        assert file_metadata_object.kind == file_metadata['kind']
        assert file_metadata_object.content_type == file_metadata['contentType']
        assert file_metadata_object.etag == '{}::{}'.format(file_metadata['version'], file_metadata['path'])
        assert file_metadata_object.raw == file_metadata
        assert file_metadata_object.materialized_path == WaterButlerPath(file_metadata['fullPath'], prepend=None)

    def test_file_metadata_modified_utc(self, file_metadata, file_metadata_object):

        # Case 1: all relevant metadata provided
        assert file_metadata_object.modified_utc == file_metadata['modified']

        # Case 2: no relevant metadata provided
        file_metadata_object.raw['modified'] = None

        assert file_metadata_object.modified_utc == file_metadata['modified']

        # Case 3: no timezone metadata provided
        file_metadata_object.raw['modified'] = '2017-09-01T19:34:00.175741'
        assert file_metadata_object.modified_utc == '2017-09-01T19:34:00.175741+00:00'

    def test_file_metadata_created_utc(self, file_metadata, file_metadata_object):

        # Case 1: all relevant metadata provided
        assert file_metadata_object.created_utc == file_metadata['created']

        # Case 2: no relevant metadata provided
        file_metadata_object.raw['created'] = None

        assert file_metadata_object.created_utc == file_metadata['created']

        # Case 3: no timezone metadata provided
        file_metadata_object.raw['created'] = '2017-09-01T19:34:00.175741'
        assert file_metadata_object.created_utc == '2017-09-01T19:34:00.175741+00:00'


class TestRevisionMetadata:

    def test_revision_metadata(self, revisions_metadata, revision_metadata_object):
        revision_metadata = revisions_metadata['revisions'][0]

        assert revision_metadata_object.modified == revision_metadata['date']
        assert revision_metadata_object.version_identifier == 'version'
        assert revision_metadata_object.version == str(revision_metadata['index'])
        assert revision_metadata_object.raw == revision_metadata

        expected_extra = {
            'user': revision_metadata['user'],
            'downloads': revision_metadata['downloads'],
            'hashes': {
                'md5': revision_metadata['md5'],
                'sha256': revision_metadata['sha256']
            },
        }

        assert revision_metadata_object.extra == expected_extra
