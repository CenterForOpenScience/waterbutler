import pytest
from tests.providers.dataverse.fixtures import (
    dataset_metadata_object,
    revision_metadata_object,
    file_metadata_object
)

class TestDatasetMetadata:

    def test_dataset_metadata(self, dataset_metadata_object):
        assert dataset_metadata_object.provider == 'dataverse'
        assert not dataset_metadata_object.is_file
        assert dataset_metadata_object.is_folder
        assert dataset_metadata_object.kind == 'folder'
        assert dataset_metadata_object.name == 'Dataset Test Name'
        assert dataset_metadata_object.path == '/Dataset Test DOI/'
        assert dataset_metadata_object.materialized_path == '/Dataset Test DOI/'
        assert not dataset_metadata_object.etag
        assert not dataset_metadata_object.extra


class TestRevisionMetadata:

    def test_revision_metadata(self, revision_metadata_object):
        assert revision_metadata_object.version_identifier == 'version'
        assert revision_metadata_object.version == 'Test Dataset Verision'
        assert not revision_metadata_object.modified
        assert not revision_metadata_object.modified_utc
        assert not revision_metadata_object.extra


class TestFileMetadata:

    def test_file_metadata(self, file_metadata_object):
        assert file_metadata_object.is_file
        assert not file_metadata_object.is_folder
        assert file_metadata_object.provider == 'dataverse'
        assert file_metadata_object.kind == 'file'
        assert file_metadata_object.file_id == '20'
        assert file_metadata_object.name == 'thefile.txt'
        assert file_metadata_object.size is None
        assert file_metadata_object.size_as_int is None
        assert file_metadata_object.path == '/20'
        assert file_metadata_object.materialized_path == '/thefile.txt'
        assert not file_metadata_object.size
        assert not file_metadata_object.modified
        assert not file_metadata_object.created_utc
        assert file_metadata_object.content_type == 'text/plain; charset=US-ASCII'
        assert file_metadata_object.etag == 'latest::20'
        assert file_metadata_object.extra == {
            'fileId': '20',
            'datasetVersion': 'latest',
            'hasPublishedVersion': False,
            'hashes': {
                'md5': '6b50249f91258397fc5cb7d5a4127e15',
            },
        }
