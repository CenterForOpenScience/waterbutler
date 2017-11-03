import os
import json

import pytest

from tests.utils import MockStream


@pytest.fixture
def stream_200_MB():
    data = os.urandom(200000000)  # 200 MB
    return MockStream(data)


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
def intra_copy_fixtures():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/intra_copy.json'), 'r') as fp:
        return json.load(fp)


@pytest.fixture
def error_fixtures():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/errors.json'), 'r') as fp:
        return json.load(fp)
