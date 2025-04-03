import os
import json
import pytest


@pytest.fixture
def root_provider_fixtures():
    # fixtures for testing validate_v1_path for root provider
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/root_provider.json')) as fp:
        return json.load(fp)


@pytest.fixture
def subfolder_provider_fixtures():
    # fixtures for testing validate_v1_path for subfolder provider
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/subfolder_provider.json')) as fp:
        return json.load(fp)


@pytest.fixture
def revision_fixtures():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/revisions.json')) as fp:
        return json.load(fp)


@pytest.fixture
def download_fixtures():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/download.json')) as fp:
        return json.load(fp)


@pytest.fixture
def path_fixtures():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/paths.json')) as fp:
        return json.load(fp)


@pytest.fixture
def readwrite_fixtures():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/readwrite.json')) as fp:
        return json.load(fp)
