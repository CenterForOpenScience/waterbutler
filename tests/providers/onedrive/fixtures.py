import io
import json
import os

import pytest

from waterbutler.core import streams
from waterbutler.providers.onedrive import OneDriveProvider


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
    return {'folder': '/Photos', 'drive_id': '01234567'}


@pytest.fixture
def root_settings():
    return {'folder': 'root', 'drive_id': 'deadbeef'}


@pytest.fixture
def subfolder_settings(subfolder_provider_fixtures):
    return {'folder': subfolder_provider_fixtures['root_id'], 'drive_id': '43218765'}


@pytest.fixture
def root_provider_fixtures():
    # fixtures for testing validate_v1_path for root provider
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/root_provider.json'), 'r') as fp:
        return json.load(fp)


@pytest.fixture
def subfolder_provider_fixtures():
    # fixtures for testing validate_v1_path for subfolder provider
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/subfolder_provider.json'), 'r') as fp:
        return json.load(fp)


@pytest.fixture
def revision_fixtures():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/revisions.json'), 'r') as fp:
        return json.load(fp)


@pytest.fixture
def download_fixtures():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/download.json'), 'r') as fp:
        return json.load(fp)


@pytest.fixture
def path_fixtures():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/paths.json'), 'r') as fp:
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
def empty_file_content():
    return b''


@pytest.fixture
def empty_file_like(empty_file_content):
    return io.BytesIO(empty_file_content)


@pytest.fixture
def empty_file_stream(empty_file_like):
    return streams.FileStreamReader(empty_file_like)


@pytest.fixture
def provider(auth, credentials, settings):
    return OneDriveProvider(auth, credentials, settings)


@pytest.fixture
def other_provider(auth, other_credentials, settings):
    return OneDriveProvider(auth, other_credentials, settings)


@pytest.fixture
def root_provider(auth, credentials, root_settings):
    return OneDriveProvider(auth, credentials, root_settings)


@pytest.fixture
def subfolder_provider(auth, credentials, subfolder_settings):
    """Provider root is subfolder of OneDrive account root"""
    return OneDriveProvider(auth, credentials, subfolder_settings)
