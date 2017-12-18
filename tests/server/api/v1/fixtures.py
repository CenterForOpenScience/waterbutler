import time
import asyncio
from unittest import mock

import pytest
import tornado

from waterbutler.server.app import make_app
from waterbutler.server.api.v1.provider import ProviderHandler
from waterbutler.core.path import WaterButlerPath
from waterbutler.core.log_payload import LogPayload

from tests.providers.osfstorage.fixtures import (
    file_metadata_object,
    file_metadata,
    file_path,
    file_lineage,
    provider,
    auth
)

from tests.utils import (
    MockProvider,
    MockFileMetadata
)

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
    http_request = tornado.httputil.HTTPServerRequest(
        uri='/v1/resources/test/providers/test/path/mock',
        method='GET')
    http_request.headers['User-Agent'] = 'test'
    http_request.connection = tornado.http1connection.HTTP1ConnectionParameters()
    http_request.connection.set_close_callback = mock.Mock()
    http_request.request_time = mock.Mock(return_value=10)

    return http_request

@pytest.fixture
def log_payload():
    return LogPayload('test', MockProvider(), path=WaterButlerPath('/test_path'))

@pytest.fixture
def mock_time(monkeypatch):
    mock_time = mock.Mock()
    mock_time.return_value = 10
    monkeypatch.setattr(time, 'time', mock_time)

@pytest.fixture
def handler(http_request):
    handler = ProviderHandler(make_app(True), http_request)
    handler.path = WaterButlerPath('/test_path')

    handler.provider = MockProvider()
    handler.resource = 'test_source_resource'
    handler.metadata = MockFileMetadata()

    handler.dest_provider = MockProvider()
    handler.dest_resource = 'test_dest_resource'
    handler.dest_meta = MockFileMetadata()

    return handler


@pytest.fixture
def source_payload(handler):
    return LogPayload(handler.resource,
                                handler.provider,
                                path=handler.path)


@pytest.fixture
def destination_payload(handler):
    return LogPayload(handler.dest_resource,
                      handler.provider,
                      metadata=handler.dest_meta)


@pytest.fixture
def payload_path(handler):
    return LogPayload(handler.resource,
                                handler.provider,
                                path=handler.path)


@pytest.fixture
def payload_metadata(handler):
    return LogPayload(handler.resource,
                      handler.provider,
                      metadata=handler.metadata)


@pytest.fixture
def serialzied_request(handler):
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
        }
    }