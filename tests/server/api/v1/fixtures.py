import os
import sys
import json
from unittest import mock

import pytest
from tornado.web import HTTPError
from tornado.httputil import HTTPServerRequest
from tornado.http1connection import HTTP1ConnectionParameters

import waterbutler
from waterbutler.core.path import WaterButlerPath
from waterbutler.tasks.exceptions import WaitTimeOutError
from waterbutler.server.api.v1.provider import ProviderHandler
from tests.utils import (MockProvider, MockFileMetadata, MockFolderMetadata,
                         MockFileRevisionMetadata, MockCoroutine, MockRequestBody, MockStream)


@pytest.fixture
def http_request():
    mocked_http_request = HTTPServerRequest(
        uri='/v1/resources/test/providers/test/path/mock',
        method='GET'
    )
    mocked_http_request.headers['User-Agent'] = 'test'
    mocked_http_request.connection = HTTP1ConnectionParameters()
    mocked_http_request.connection.set_close_callback = mock.Mock()
    mocked_http_request.request_time = mock.Mock(return_value=10)
    mocked_http_request.body = MockRequestBody()
    return mocked_http_request


@pytest.fixture
def mock_stream():
    return MockStream()


@pytest.fixture
def mock_partial_stream():
    stream = MockStream()
    stream.partial = True
    stream.content_range = 'bytes=10-100'
    return stream


@pytest.fixture()
def mock_file_metadata():
    return MockFileMetadata()


@pytest.fixture()
def mock_folder_metadata():
    return MockFolderMetadata()


@pytest.fixture()
def mock_revision_metadata():
    return [MockFileRevisionMetadata()]


@pytest.fixture()
def mock_folder_children():
    return [MockFolderMetadata(), MockFileMetadata(), MockFileMetadata()]


@pytest.fixture
def patch_auth_handler(monkeypatch, handler_auth):
    mock_auth_handler = MockCoroutine(return_value=handler_auth)
    monkeypatch.setattr(waterbutler.server.auth.AuthHandler, 'get', mock_auth_handler)
    return mock_auth_handler


@pytest.fixture
def patch_make_provider_move_copy(monkeypatch):
    make_provider = mock.Mock(return_value=MockProvider())
    monkeypatch.setattr(waterbutler.server.api.v1.provider.movecopy, 'make_provider', make_provider)
    return make_provider


@pytest.fixture
def patch_make_provider_core(monkeypatch):
    make_provider = mock.Mock(return_value=MockProvider())
    monkeypatch.setattr(waterbutler.server.api.v1.provider.utils, 'make_provider', make_provider)
    return make_provider


@pytest.fixture(params=[True, False])
def mock_intra(monkeypatch, request):
    src_provider = MockProvider()
    dest_provider = MockProvider()
    mock_make_provider = mock.Mock(side_effect=[src_provider, dest_provider])
    monkeypatch.setattr(waterbutler.server.api.v1.provider.movecopy,
                        'make_provider',
                        mock_make_provider)

    src_provider.can_intra_copy = mock.Mock(return_value=True)
    src_provider.can_intra_move = mock.Mock(return_value=True)

    mock_backgrounded = MockCoroutine(return_value=(MockFileMetadata(), request.param))
    monkeypatch.setattr(waterbutler.server.api.v1.provider.movecopy.tasks,
                        'backgrounded',
                        mock_backgrounded)

    return mock_make_provider, mock_backgrounded


@pytest.fixture(params=[True, False])
def mock_inter(monkeypatch, request):
    src_provider = MockProvider()
    dest_provider = MockProvider()
    mock_make_provider = mock.Mock(side_effect=[src_provider, dest_provider])
    monkeypatch.setattr(waterbutler.server.api.v1.provider.movecopy,
                        'make_provider',
                        mock_make_provider)

    mock_celery = MockCoroutine(return_value=(MockFileMetadata(), request.param))
    mock_adelay = MockCoroutine(return_value='4ef2d1dd-c5da-41a7-ae4a-9d0ba7a68927')
    monkeypatch.setattr(waterbutler.server.api.v1.provider.movecopy.tasks.copy,
                        'adelay',
                        mock_adelay)
    monkeypatch.setattr(waterbutler.server.api.v1.provider.movecopy.tasks.move,
                        'adelay',
                        mock_adelay)
    monkeypatch.setattr(waterbutler.server.api.v1.provider.movecopy.tasks,
                        'wait_on_celery',
                        mock_celery)

    return mock_make_provider, mock_adelay


@pytest.fixture
def mock_exc_info():
    try:
        raise Exception('test exception')
    except:
        return sys.exc_info()


@pytest.fixture
def mock_exc_info_http():
    try:
        raise HTTPError(status_code=500, log_message='test http exception')
    except HTTPError:
        return sys.exc_info()


@pytest.fixture
def mock_exc_info_202():
    try:
        raise WaitTimeOutError('test exception')
    except WaitTimeOutError:
        return sys.exc_info()


@pytest.fixture
def move_copy_args():
    return (
        {
            'nid': 'test_source_resource',
            'provider': {
                'credentials': {},
                'name': 'MockProvider',
                'auth': {},
                'settings': {}
            },
            'path': '/test_path'
        },
        {
            'nid': 'test_dest_resource',
            'provider': {
                'credentials': {},
                'name': 'MockProvider',
                'auth': {},
                'settings': {}
            }, 'path': '/test_dest_path'
        }
    )


@pytest.fixture
def celery_src_copy_params():
    return {
            'nid': 'test_source_resource',
            'path': WaterButlerPath('/test_path', prepend=None),
            'provider': {
                'credentials': {},
                'name': 'MockProvider',
                'settings': {},
                'auth': {}
            }
    }


@pytest.fixture
def celery_dest_copy_params():
    return {
            'nid': 'test_source_resource',
            'path': WaterButlerPath('/test_path/', prepend=None),
            'provider': {
                'credentials': {},
                'name': 'MockProvider',
                'settings': {},
                'auth': {}
            }
    }


@pytest.fixture
def celery_dest_copy_params_root():
    return {
            'nid': 'test_source_resource',
            'path': WaterButlerPath('/', prepend=None),
            'provider': {
                'credentials': {},
                'name': 'MockProvider',
                'settings': {},
                'auth': {}
            }
    }


@pytest.fixture
def handler_auth():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/fixtures.json'), 'r') as fp:
        return json.load(fp)['hander_auth']


@pytest.fixture
def serialized_metadata():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/fixtures.json'), 'r') as fp:
        return json.load(fp)['serialized_metadata']


@pytest.fixture
def serialized_request():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/fixtures.json'), 'r') as fp:
        return json.load(fp)['serialized_request']
