import sys
import copy
import time
import asyncio
import hashlib
from unittest import mock

import celery
import pytest

from waterbutler import tasks  # noqa
from waterbutler.core import remote_logging
from waterbutler.core import utils as core_utils
from waterbutler.core.path import WaterButlerPath

import tests.utils as test_utils

# Hack to get the module, not the function
move = sys.modules['waterbutler.tasks.move']

FAKE_TIME = 1454684930.0

@pytest.fixture(autouse=True)
def patch_backend(monkeypatch):
    monkeypatch.setattr(move.core.app, 'backend', None)


@pytest.fixture(autouse=True)
def callback(monkeypatch):
    mock_request = test_utils.MockCoroutine(
        return_value=mock.Mock(
            status=200,
            read=test_utils.MockCoroutine(
                return_value=b'meowmeowmeow'
            )
        )
    )
    monkeypatch.setattr(core_utils, 'send_signed_request', mock_request)
    return mock_request


@pytest.fixture
def mock_time(monkeypatch):
    mock_time = mock.Mock(return_value=FAKE_TIME)
    monkeypatch.setattr(time, 'time', mock_time)


@pytest.fixture
def src_path():
    return WaterButlerPath('/user/bin/python')


@pytest.fixture
def dest_path():
    return WaterButlerPath('/usr/bin/golang')


@pytest.fixture(scope='function')
def src_provider():
    p = test_utils.MockProvider()
    p.move.return_value = (test_utils.MockFileMetadata(), True)
    p.auth['callback_url'] = 'src_callback'
    return p


@pytest.fixture(scope='function')
def dest_provider():
    p = test_utils.MockProvider()
    p.move.return_value = (test_utils.MockFileMetadata(), True)
    p.auth['callback_url'] = 'dest_callback'
    return p


@pytest.fixture(scope='function')
def providers(monkeypatch, src_provider, dest_provider):
    def make_provider(name=None, **kwargs):
        if name == 'src':
            return src_provider
        if name == 'dest':
            return dest_provider
        raise ValueError('Unexpected provider')
    monkeypatch.setattr(move.utils, 'make_provider', make_provider)
    return src_provider, dest_provider


@pytest.fixture(autouse=True)
def log_to_keen(monkeypatch):
    mock_log_to_keen = test_utils.MockCoroutine()
    monkeypatch.setattr(remote_logging, 'log_to_keen', mock_log_to_keen)
    return mock_log_to_keen


@pytest.fixture
def src_bundle(src_path):
    return {
        'nid': 'mst3k',
        'path': src_path,
        'provider': {
            'name': 'src',
            'auth': {
                'callback_url': '',
            },
            'settings': {},
            'credentials': {},
        }
    }


@pytest.fixture
def dest_bundle(dest_path):
    return {
        'nid': 'fbi4u',
        'path': dest_path,
        'provider': {
            'name': 'dest',
            'auth': {
                'callback_url': '',
            },
            'settings': {},
            'credentials': {},
        }
    }


@pytest.fixture
def bundles(src_bundle, dest_bundle):
    return src_bundle, dest_bundle


class TestMoveTask:

    def test_move_calls_move(self, event_loop, providers, bundles):
        src, dest = providers
        src_bundle, dest_bundle = bundles

        move.move(copy.deepcopy(src_bundle), copy.deepcopy(dest_bundle))

        assert src.move.called
        src.move.assert_called_once_with(dest, src_bundle['path'], dest_bundle['path'])

    def test_is_task(self):
        assert callable(move.move)
        assert isinstance(move.move, celery.Task)
        assert not asyncio.iscoroutine(move.move)
        assert asyncio.iscoroutinefunction(move.move.adelay)

    def test_imputes_exceptions(self, event_loop, providers, bundles, callback):
        src, dest = providers
        src_bundle, dest_bundle = bundles

        src.move.side_effect = Exception('This is a string')

        with pytest.raises(Exception):
            move.move(copy.deepcopy(src_bundle), copy.deepcopy(dest_bundle))

        (method, url, data), _ = callback.call_args_list[0]

        assert src.move.called
        src.move.assert_called_once_with(dest, src_bundle['path'], dest_bundle['path'])

        assert method == 'PUT'
        assert data['errors'] == ["Exception('This is a string',)"]
        assert url == 'dest_callback'

    def test_return_values(self, event_loop, providers, bundles, callback, src_path, dest_path, mock_time):
        src, dest = providers
        src_bundle, dest_bundle = bundles

        metadata = test_utils.MockFileMetadata()
        src.move.return_value = (metadata, False)

        ret1, ret2 = move.move(copy.deepcopy(src_bundle), copy.deepcopy(dest_bundle))

        assert (ret1, ret2) == (metadata, False)

        (method, url, data), _ = callback.call_args_list[0]
        assert method == 'PUT'
        assert url == 'dest_callback'
        assert data['action'] == 'move'
        assert data['auth'] == {'callback_url': 'dest_callback'}
        assert data['email'] == False
        assert data['errors'] == []
        assert data['time'] == FAKE_TIME + 60

        assert data['source'] == {
            'nid': 'mst3k',
            'resource': 'mst3k',
            'path': '/' + src_path.raw_path,
            'name': src_path.name,
            'materialized': str(src_path),
            'provider': src.NAME,
            'kind': 'file',
            'extra': {},
        }

        assert data['destination'] == {
            'nid': 'fbi4u',
            'resource': 'fbi4u',
            'path': metadata.path,
            'name': metadata.name,
            'materialized': metadata.path,
            'provider': dest.NAME,
            'kind': 'file',
            'contentType': metadata.content_type,
            'etag': hashlib.sha256(
                '{}::{}'.format(metadata.provider, metadata.etag)
                .encode('utf-8')
            ).hexdigest(),
            'extra': metadata.extra,
            'modified': metadata.modified,
            'modified_utc': metadata.modified_utc,
            'created_utc': metadata.created_utc,
            'size': metadata.size,
            'sizeInt': metadata.size_as_int,
        }

    def test_starttime_override(self, event_loop, providers, bundles, callback, mock_time):
        src, dest = providers
        src_bundle, dest_bundle = bundles

        stamp = FAKE_TIME
        move.move(copy.deepcopy(src_bundle), copy.deepcopy(dest_bundle), start_time=stamp-100)
        move.move(copy.deepcopy(src_bundle), copy.deepcopy(dest_bundle), start_time=stamp+100)

        (_, _, data), _ = callback.call_args_list[0]

        assert data['email'] is True
        assert data['time'] == 60 + stamp

        (_, _, data), _ = callback.call_args_list[1]

        assert data['email'] is False
        assert data['time'] == 60 + stamp
