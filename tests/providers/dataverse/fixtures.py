import pytest
import os
import json


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
