import pytest
import os
import json


@pytest.fixture
def root_provider_fixtures():
    # fixtures for testing validate_v1_path for root provider
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/root_provider.json'), 'r') as fp:
        return json.load(fp)


@pytest.fixture
def revision_fixtures():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/revisions.json'), 'r') as fp:
        return json.load(fp)


@pytest.fixture
def intra_fixtures():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/intra_fixtures.json'), 'r') as fp:
        return json.load(fp)
