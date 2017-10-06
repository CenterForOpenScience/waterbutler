import pytest
import os
import json

from waterbutler.providers.dataverse.metadata import (
    DataverseDatasetMetadata,
    DataverseRevision,
    DataverseFileMetadata
)


@pytest.fixture
def auth():
    return {
        'name': 'cat',
        'email': 'cat@cat.com',
    }


@pytest.fixture
def credentials():
    return {'token': 'wrote harry potter'}


@pytest.fixture
def settings():
    return {
        'host': 'myfakehost.dataverse.org',
        'doi': 'doi:10.5072/FK2/ABCDEF',
        'id': '18',
        'name': 'A look at wizards',
    }

@pytest.fixture
def native_file_metadata():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/root_provider.json'), 'r') as fp:
        return json.load(fp)['native_file_metadata']


@pytest.fixture
def native_dataset_metadata():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/root_provider.json'), 'r') as fp:
        return json.load(fp)['native_dataset_metadata']


@pytest.fixture
def empty_native_dataset_metadata():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/root_provider.json'), 'r') as fp:
        return json.load(fp)['empty_native_dataset_metadata']


@pytest.fixture
def checksum_mismatch_dataset_metadata():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/root_provider.json'), 'r') as fp:
        return json.load(fp)['checksum_mismatch_dataset_metadata']


@pytest.fixture
def dataset_metadata_object():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/root_provider.json'), 'r') as fp:
        return DataverseDatasetMetadata(
            json.load(fp)['native_dataset_metadata']['data'],
            'Dataset Test Name',
            'Dataset Test DOI',
            'Dataset Test Version'
        )

@pytest.fixture
def file_metadata_object():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/root_provider.json'), 'r') as fp:
        return DataverseFileMetadata(json.load(fp)['native_file_metadata']['datafile'], 'latest')


@pytest.fixture
def revision_metadata_object():
    return DataverseRevision('Test Dataset Verision')
