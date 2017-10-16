import io
import os
import json
import time
from unittest import mock

import pytest

from waterbutler.core import streams
from waterbutler.core.path import WaterButlerPath
from waterbutler.providers.osfstorage.provider import OSFStorageProvider
from waterbutler.providers.osfstorage.metadata import (OsfStorageFileMetadata,
                                                       OsfStorageFolderMetadata,
                                                       OsfStorageRevisionMetadata)

from tests import utils


@pytest.fixture
def auth():
    return {
        'id': 'cat',
        'name': 'cat',
        'email': 'cat@cat.com',
    }


@pytest.fixture
def credentials():
    return {
        'storage': {
            'access_key': 'Dont dead',
            'secret_key': 'open inside',
        },
        'archive': {},
        'parity': {},
    }


@pytest.fixture
def settings():
    return {
        'justa': 'setting',
        'nid': 'foo',
        'rootId': 'rootId',
        'baseUrl': 'https://waterbutler.io',
        'storage': {
            'provider': 'mock',
        },
        'archive': {},
        'parity': {},
    }

@pytest.fixture
def folder_children_metadata():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/root_provider.json'), 'r') as fp:
        return json.load(fp)['folder_children_metadata']


@pytest.fixture
def download_response():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/root_provider.json'), 'r') as fp:
        return json.load(fp)['download_response']


@pytest.fixture
def download_path(download_response):
    return WaterButlerPath('/' + download_response['data']['name'],
                           _ids=('rootId',  download_response['data']['path']))


@pytest.fixture
def upload_response():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/root_provider.json'), 'r') as fp:
        return json.load(fp)['upload_response']


@pytest.fixture
def upload_path(upload_response):
    return WaterButlerPath('/' + upload_response['data']['name'],
                           _ids=('rootId',  upload_response['data']['id']))


@pytest.fixture
def folder_metadata():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/root_provider.json'), 'r') as fp:
        return json.load(fp)['folder_metadata']


@pytest.fixture
def folder_lineage():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/root_provider.json'), 'r') as fp:
        return json.load(fp)['folder_lineage']


@pytest.fixture
def folder_path(folder_lineage):
    return WaterButlerPath(folder_lineage['data'][0]['path'],
                           _ids=(folder_lineage['data'][-1]['id'], folder_lineage['data'][0]['id']),
                           folder=True)


@pytest.fixture
def file_metadata():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/root_provider.json'), 'r') as fp:
        return json.load(fp)['file_metadata']


@pytest.fixture
def file_lineage():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/root_provider.json'), 'r') as fp:
        return json.load(fp)['file_lineage']


@pytest.fixture
def file_path(file_lineage):
    return WaterButlerPath(file_lineage['data'][0]['path'],
                           _ids=(file_lineage['data'][-1]['id'], file_lineage['data'][0]['id']))


@pytest.fixture
def revisions_metadata():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/root_provider.json'), 'r') as fp:
        return json.load(fp)['revisions_metadata']


@pytest.fixture
def root_path(provider):
    return WaterButlerPath('/', _ids=[provider.root_id], folder=True)


@pytest.fixture
def file_metadata_object(file_metadata):
    path = WaterButlerPath('/' + file_metadata['name'])
    return OsfStorageFileMetadata(file_metadata, path)


@pytest.fixture
def folder_metadata_object(folder_metadata):
    path = WaterButlerPath('/' + folder_metadata['data']['name'], folder=True)
    return OsfStorageFolderMetadata(folder_metadata['data'], path)


@pytest.fixture
def revision_metadata_object(revisions_metadata):
    return OsfStorageRevisionMetadata(revisions_metadata['revisions'][0])


@pytest.fixture
def file_content():
    return b'sleepy'


@pytest.fixture
def file_like(file_content):
    return io.BytesIO(file_content)


@pytest.fixture
def file_stream(file_like):
    return streams.FileStreamReader(file_like)


@pytest.fixture
def provider_and_mock(monkeypatch, auth, credentials, settings):
    mock_provider = utils.MockProvider1({}, {}, {})

    mock_provider.copy = utils.MockCoroutine()
    mock_provider.move = utils.MockCoroutine()
    mock_provider.delete = utils.MockCoroutine()
    mock_provider.upload = utils.MockCoroutine()
    mock_provider.download = utils.MockCoroutine()
    mock_provider.metadata = utils.MockCoroutine()
    mock_provider.validate_v1_path = utils.MockCoroutine()
    mock_provider._children_metadata = utils.MockCoroutine()

    mock_make_provider = mock.Mock(return_value=mock_provider)
    monkeypatch.setattr(OSFStorageProvider, 'make_provider', mock_make_provider)
    return OSFStorageProvider(auth, credentials, settings), mock_provider


@pytest.fixture
def provider_and_mock2(monkeypatch, auth, credentials, settings):
    mock_provider = utils.MockProvider1({}, {}, {})

    mock_provider.copy = utils.MockCoroutine()
    mock_provider.move = utils.MockCoroutine()
    mock_provider.delete = utils.MockCoroutine()
    mock_provider.upload = utils.MockCoroutine()
    mock_provider.download = utils.MockCoroutine()
    mock_provider.metadata = utils.MockCoroutine()
    mock_provider.validate_v1_path = utils.MockCoroutine()
    mock_provider._children_metadata = utils.MockCoroutine()

    mock_make_provider = mock.Mock(return_value=mock_provider)
    monkeypatch.setattr(OSFStorageProvider, 'make_provider', mock_make_provider)
    return OSFStorageProvider(auth, credentials, settings), mock_provider


@pytest.fixture
def provider(auth, credentials, settings):
    return OSFStorageProvider(auth, credentials, settings)


@pytest.fixture
def mock_time(monkeypatch):
    mock_time = mock.Mock(return_value=1454684930.0)
    monkeypatch.setattr(time, 'time', mock_time)
