import pytest

from tests.providers.dataverse.fixtures import native_dataset_metadata
from waterbutler.providers.dataverse.metadata import DataverseDatasetMetadata, DataverseRevision

@pytest.fixture()
def dataset_metadata_object(native_dataset_metadata):
    return DataverseDatasetMetadata(
        native_dataset_metadata['data'],
        'Dataset Test Name',
        'Dataset Test DOI',
        'Dataset Test Version'
    )

@pytest.fixture()
def revision_metadata_object(native_dataset_metadata):
    return DataverseRevision('Test Dataset Verision')


class TestDatasetMetadata:

    def test_dataset_metadata(self, dataset_metadata_object):
        assert dataset_metadata_object.name == 'Dataset Test Name'
        assert dataset_metadata_object.path == '/Dataset Test DOI/'


class TestRevisionMetadata:

    def test_dataset_metadata(self, revision_metadata_object):
        assert revision_metadata_object.version_identifier == 'version'
        assert revision_metadata_object.version == 'Test Dataset Verision'
        assert not revision_metadata_object.modified
