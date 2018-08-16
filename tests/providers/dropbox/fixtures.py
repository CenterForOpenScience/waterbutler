import io
import os
import json

import pytest

from waterbutler.core import streams
from waterbutler.providers.dropbox import DropboxProvider


@pytest.fixture
def auth():
    return {'name': 'cat', 'email': 'cat@cat.com'}


@pytest.fixture
def credentials():
    return {'token': 'wrote harry potter'}


@pytest.fixture
def other_credentials():
    return {'token': 'did not write harry potter'}


@pytest.fixture
def settings():
    return {'folder': '/Photos'}


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
def error_fixtures():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/errors.json'), 'r') as fp:
        return json.load(fp)


@pytest.fixture
def file_content():
    return b'SLEEP IS FOR THE WEAK GO SERVE STREAMS'


@pytest.fixture
def file_like(file_content):
    return io.BytesIO(file_content)


@pytest.fixture
def file_stream(file_like):
    return streams.FileStreamReader(file_like)


@pytest.fixture
def provider(auth, credentials, settings):
    return DropboxProvider(auth, credentials, settings)


@pytest.fixture
def other_provider(auth, other_credentials, settings):
    return DropboxProvider(auth, other_credentials, settings)
