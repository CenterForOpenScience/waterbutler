import os
import json

import pytest


@pytest.fixture
def provider_fixtures():
    # fixtures for testing validate_v1_path for root provider
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/root_provider.json'), 'r') as fp:
        return json.load(fp)


@pytest.fixture
def revision_fixtures():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/revisions.json'), 'r') as fp:
        return json.load(fp)


@pytest.fixture
def crud_fixtures():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/crud.json'), 'r') as fp:
        return json.load(fp)


@pytest.fixture
def error_fixtures():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/errors.json'), 'r') as fp:
        return json.load(fp)


@pytest.fixture
def metadata_fixtures():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/metadata.json'), 'r') as fp:
        return json.load(fp)
