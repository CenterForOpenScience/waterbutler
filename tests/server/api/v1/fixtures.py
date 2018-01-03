import time
import asyncio
from unittest import mock

import pytest
from tornado.httputil import HTTPServerRequest
from tornado.http1connection import HTTP1ConnectionParameters

from waterbutler.server.app import make_app
from waterbutler.core.path import WaterButlerPath
from waterbutler.core.log_payload import LogPayload
from waterbutler.server.api.v1.provider import ProviderHandler

from tests.utils import MockProvider, MockFileMetadata
from tests.providers.osfstorage.fixtures import (auth, provider, file_metadata_object,
                                                 file_metadata, file_path, file_lineage)


@pytest.yield_fixture
def event_loop():
    """Create an instance of the default event loop for each test case."""
    policy = asyncio.get_event_loop_policy()
    res = policy.new_event_loop()
    asyncio.set_event_loop(res)
    res._close = res.close
    res.close = lambda: None

    yield res

    res._close()


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

    return mocked_http_request


@pytest.fixture
def log_payload():
    return LogPayload('test', MockProvider(), path=WaterButlerPath('/test_path'))


@pytest.fixture
def mock_time(monkeypatch):
    mocked_time = mock.Mock()
    mocked_time.return_value = 10
    monkeypatch.setattr(time, 'time', mocked_time)


@pytest.fixture
def handler(http_request):
    mocked_handler = ProviderHandler(make_app(True), http_request)
    mocked_handler.path = WaterButlerPath('/test_path')

    mocked_handler.provider = MockProvider()
    mocked_handler.resource = 'test_source_resource'
    mocked_handler.metadata = MockFileMetadata()

    mocked_handler.dest_provider = MockProvider()
    mocked_handler.dest_resource = 'test_dest_resource'
    mocked_handler.dest_meta = MockFileMetadata()

    return mocked_handler


@pytest.fixture
def source_payload(handler):
    return LogPayload(handler.resource, handler.provider, path=handler.path)


@pytest.fixture
def destination_payload(handler):
    return LogPayload(handler.dest_resource, handler.provider, metadata=handler.dest_meta)


@pytest.fixture
def payload_path(handler):
    return LogPayload(handler.resource, handler.provider, path=handler.path)


@pytest.fixture
def payload_metadata(handler):
    return LogPayload(handler.resource, handler.provider, metadata=handler.metadata)


@pytest.fixture
def serialized_request():
    return {
        'request': {
            'url': 'http://127.0.0.1/v1/resources/test/providers/test/path/mock',
            'method': 'GET',
            'headers': {},
            'time': 10
        },
        'tech': {
            'ua': 'test',
            'ip': None
        },
        'referrer': {
            'url': None
        },
    }
