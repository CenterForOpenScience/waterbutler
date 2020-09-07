import sys
import time
import asyncio
import hashlib
import copy as cp

import celery
import pytest

from waterbutler import tasks  # noqa

import tests.utils as test_utils


# Hack to get the module, not the function
move = sys.modules['waterbutler.tasks.move']


@pytest.fixture(autouse=True)
def patch_backend(monkeypatch):
    monkeypatch.setattr(move.core.app, 'backend', None)


@pytest.fixture
def src_provider():
    p = test_utils.MockProvider()
    p.move.return_value = (test_utils.MockFileMetadata(), True)
    p.auth['callback_url'] = 'src_callback'
    return p


@pytest.fixture
def dest_provider():
    p = test_utils.MockProvider()
    p.move.return_value = (test_utils.MockFileMetadata(), True)
    p.auth['callback_url'] = 'dest_callback'
    return p


@pytest.fixture
def providers(monkeypatch, src_provider, dest_provider):
    def make_provider(name=None, **kwargs):
        if name == 'src':
            return src_provider
        if name == 'dest':
            return dest_provider
        raise ValueError('Unexpected provider')
    monkeypatch.setattr(move.utils, 'make_provider', make_provider)
    return src_provider, dest_provider


def test_move_calls_move(providers, bundles, callback):
    src, dest = providers
    src_bundle, dest_bundle = bundles

    move.move(cp.deepcopy(src_bundle), cp.deepcopy(dest_bundle))

    assert src.move.called
    src.move.assert_called_once_with(dest, src_bundle['path'], dest_bundle['path'])


def test_is_task():
    assert callable(move.move)
    assert isinstance(move.move, celery.Task)
    assert not asyncio.iscoroutine(move.move)
    assert asyncio.iscoroutinefunction(move.move.adelay)


def test_imputes_exceptions(providers, bundles, callback):
    src, dest = providers
    src_bundle, dest_bundle = bundles

    src.move.side_effect = Exception('This is a string')

    with pytest.raises(Exception):
        move.move(cp.deepcopy(src_bundle), cp.deepcopy(dest_bundle))

    (method, url, data), _ = callback.call_args_list[0]

    assert src.move.called
    src.move.assert_called_once_with(dest, src_bundle['path'], dest_bundle['path'])

    assert method == 'PUT'
    assert data['errors'] == ["Exception('This is a string',)"]
    assert url == 'dest_callback'


def test_return_values(providers, bundles, callback, src_path, dest_path, mock_time, FAKE_TIME):
    src, dest = providers
    src_bundle, dest_bundle = bundles

    metadata = test_utils.MockFileMetadata()
    src.move.return_value = (metadata, False)

    ret1, ret2 = move.move(cp.deepcopy(src_bundle), cp.deepcopy(dest_bundle))

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


def test_starttime_override(providers, bundles, callback, mock_time, FAKE_TIME):
    src, dest = providers
    src_bundle, dest_bundle = bundles

    stamp = FAKE_TIME
    move.move(cp.deepcopy(src_bundle), cp.deepcopy(dest_bundle), start_time=stamp-100)
    move.move(cp.deepcopy(src_bundle), cp.deepcopy(dest_bundle), start_time=stamp+100)

    (_, _, data), _ = callback.call_args_list[0]

    assert data['email'] is True
    assert data['time'] == 60 + stamp

    (_, _, data), _ = callback.call_args_list[1]

    assert data['email'] is False
    assert data['time'] == 60 + stamp

