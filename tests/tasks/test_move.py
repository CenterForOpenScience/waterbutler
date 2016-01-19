import sys
import copy
import asyncio
import datetime

import celery
import pytest
import freezegun

from waterbutler import tasks  # noqa
from waterbutler.core.path import WaterButlerPath

import tests.utils as test_utils

# Hack to get the module, not the function
move = sys.modules['waterbutler.tasks.move']


@pytest.fixture(autouse=True)
def patch_backend(monkeypatch):
    monkeypatch.setattr(move.core.app, 'backend', None)


@pytest.fixture(autouse=True)
def callback(monkeypatch):
    mock_request = test_utils.MockCoroutine()
    monkeypatch.setattr(move.utils, 'send_signed_request', mock_request)
    return mock_request


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
    return p


@pytest.fixture(scope='function')
def dest_provider():
    p = test_utils.MockProvider()
    p.move.return_value = (test_utils.MockFileMetadata(), True)
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


@pytest.fixture
def src_bundle(src_path):
    return {
        'path': src_path,
        'provider': {
            'name': 'src',
            'auth': {},
            'settings': {},
            'credentials': {},
        }
    }


@pytest.fixture
def dest_bundle(dest_path):
    return {
        'path': dest_path,
        'provider': {
            'name': 'dest',
            'auth': {},
            'settings': {},
            'credentials': {},
        }
    }


@pytest.fixture
def bundles(src_bundle, dest_bundle):
    return src_bundle, dest_bundle


class TestMoveTask:

    def test_move_calls_move(self, providers, bundles):
        src, dest = providers
        src_bundle, dest_bundle = bundles

        move.move(copy.deepcopy(src_bundle), copy.deepcopy(dest_bundle), '', {'auth': {}})

        assert src.move.called
        src.move.assert_called_once_with(dest, src_bundle['path'], dest_bundle['path'])

    def test_is_task(self):
        assert callable(move.move)
        assert isinstance(move.move, celery.Task)
        assert not asyncio.iscoroutine(move.move)
        assert asyncio.iscoroutinefunction(move.move.adelay)

    def test_imputes_exceptions(self, providers, bundles, callback):
        src, dest = providers
        src_bundle, dest_bundle = bundles

        src.move.side_effect = Exception('This is a string')

        with pytest.raises(Exception):
            move.move(copy.deepcopy(src_bundle), copy.deepcopy(dest_bundle), '', {'auth': {}})

        (method, url, data), _ = callback.call_args_list[0]

        assert src.move.called
        src.move.assert_called_once_with(dest, src_bundle['path'], dest_bundle['path'])

        assert url == ''
        assert method == 'PUT'
        assert data['errors'] == ["Exception('This is a string',)"]

    def test__return_values(self, providers, bundles, callback, src_path, dest_path):
        src, dest = providers
        src_bundle, dest_bundle = bundles

        metadata = test_utils.MockFileMetadata()
        src.move.return_value = (metadata, False)

        dt = datetime.datetime.utcfromtimestamp(60)
        with freezegun.freeze_time(dt):
            ret1, ret2 = move.move(copy.deepcopy(src_bundle), copy.deepcopy(dest_bundle), 'Test.com', {'auth': {'user': 'name'}})

        assert (ret1, ret2) == (metadata, False)

        callback.assert_called_once_with(
            'PUT',
            'Test.com',
            {
                'errors': [],
                'action': 'move',
                'source': {
                    'path': '/' + src_path.path,
                    'name': src_path.name,
                    'materialized': str(src_path),
                    'provider': src.NAME,
                },
                'destination': metadata.serialized(),
                'auth': {'user': 'name'},
                'time': 120,
                'email': False
            }
        )

    def test_starttime_override(self, providers, bundles, callback):
        src, dest = providers
        src_bundle, dest_bundle = bundles

        dt = datetime.datetime.utcnow()
        stamp = dt.replace(tzinfo=datetime.timezone.utc).timestamp()
        with freezegun.freeze_time(dt, tz_offset=0):
            move.move(copy.deepcopy(src_bundle), copy.deepcopy(dest_bundle), '', {'auth': {}}, start_time=stamp-100)
            move.move(copy.deepcopy(src_bundle), copy.deepcopy(dest_bundle), '', {'auth': {}}, start_time=stamp+100)

        (_, _, data), _ = callback.call_args_list[0]

        assert data['email'] is True
        assert data['time'] == 60 + stamp

        (_, _, data), _ = callback.call_args_list[1]

        assert data['email'] is False
        assert data['time'] == 60 + stamp
