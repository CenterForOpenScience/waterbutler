import time
from http import HTTPStatus
from unittest import mock

import pytest

from waterbutler.core.path import WaterButlerPath
from waterbutler.core.log_payload import LogPayload

from tests.utils import MockCoroutine
from tests.providers.osfstorage.fixtures import (auth, file_path, file_lineage, provider,
                                                 file_metadata_object, file_metadata)


@pytest.fixture
def log_payload(file_metadata_object, file_path, provider):
    return LogPayload('guid0', provider, file_metadata_object, file_path)


@pytest.fixture
def callback_log_payload_move():
    return {
        'auth': {
            'callback_url': 'fakecallback.com',
            'id': 'cat',
            'name': 'cat',
            'email': 'cat@cat.com'
        },
        'time': 70,
        'action': 'move',
        'source': {
            'materialized': WaterButlerPath('/doc.rst', prepend=None),
            'path': '/59a9b628b7d1c903ab5a8f52',
            'kind': 'file',
            'extra': {
                'checkout': None,
                'downloads': 0,
                'guid': None,
                'hashes': {
                    'sha256': '043be9ff919762f0dc36fff0222cd90c753ce28b39feb52112be9360c476ef88',
                    'md5': 'eb3f7cc15ba7b6effb2186284185c5cf'
                },
                'version': 1
            },
            'nid': 'guid0',
            'etag': 'eccd2270585257f4b48d8493bed863c01cf0b6dc0bb590101407c9b5e10b8e08',
            'contentType': None,
            'created_utc': '2017-09-01T19:34:00.175741+00:00',
            'provider': 'osfstorage',
            'modified': '2017-09-01T19:34:00.175741+00:00',
            'modified_utc': '2017-09-01T19:34:00.175741+00:00',
            'name': 'doc.rst',
            'size': 5596,
            'resource': 'guid0'
        },
        'errors': [],
        'destination': {
            'materialized': WaterButlerPath('/doc.rst', prepend=None),
            'path': '/59a9b628b7d1c903ab5a8f52',
            'kind': 'file',
            'extra': {
                'checkout': None,
                'downloads': 0,
                'guid': None,
                'hashes': {
                    'sha256': '043be9ff919762f0dc36fff0222cd90c753ce28b39feb52112be9360c476ef88',
                    'md5': 'eb3f7cc15ba7b6effb2186284185c5cf'
                },
                'version': 1
            },
            'nid': 'guid0',
            'etag': 'eccd2270585257f4b48d8493bed863c01cf0b6dc0bb590101407c9b5e10b8e08',
            'contentType': None, 'created_utc': '2017-09-01T19:34:00.175741+00:00',
            'provider': 'osfstorage',
            'modified': '2017-09-01T19:34:00.175741+00:00',
            'modified_utc': '2017-09-01T19:34:00.175741+00:00',
            'name': 'doc.rst',
            'size': 5596,
            'resource': 'guid0'
        }
    }


@pytest.fixture
def callback_log_payload_copy():
    return {
        'auth': {
            'callback_url': 'fakecallback.com',
            'id': 'cat',
            'name': 'cat',
            'email': 'cat@cat.com'
        },
        'time': 70,
        'action': 'copy',
        'source': {
            'materialized': WaterButlerPath('/doc.rst', prepend=None),
            'path': '/59a9b628b7d1c903ab5a8f52',
            'kind': 'file',
            'extra': {
                'checkout': None,
                'downloads': 0,
                'guid': None,
                'hashes': {
                    'sha256': '043be9ff919762f0dc36fff0222cd90c753ce28b39feb52112be9360c476ef88',
                    'md5': 'eb3f7cc15ba7b6effb2186284185c5cf'
                },
                'version': 1
            },
            'nid': 'guid0',
            'etag': 'eccd2270585257f4b48d8493bed863c01cf0b6dc0bb590101407c9b5e10b8e08',
            'contentType': None,
            'created_utc': '2017-09-01T19:34:00.175741+00:00',
            'provider': 'osfstorage',
            'modified': '2017-09-01T19:34:00.175741+00:00',
            'modified_utc': '2017-09-01T19:34:00.175741+00:00',
            'name': 'doc.rst',
            'size': 5596,
            'resource': 'guid0'
        },
        'errors': [],
        'destination': {
            'materialized': WaterButlerPath('/doc.rst', prepend=None),
            'path': '/59a9b628b7d1c903ab5a8f52',
            'kind': 'file',
            'extra': {
                'checkout': None,
                'downloads': 0,
                'guid': None,
                'hashes': {
                    'sha256': '043be9ff919762f0dc36fff0222cd90c753ce28b39feb52112be9360c476ef88',
                    'md5': 'eb3f7cc15ba7b6effb2186284185c5cf'
                },
                'version': 1
            },
            'nid': 'guid0',
            'etag': 'eccd2270585257f4b48d8493bed863c01cf0b6dc0bb590101407c9b5e10b8e08',
            'contentType': None, 'created_utc': '2017-09-01T19:34:00.175741+00:00',
            'provider': 'osfstorage',
            'modified': '2017-09-01T19:34:00.175741+00:00',
            'modified_utc': '2017-09-01T19:34:00.175741+00:00',
            'name': 'doc.rst',
            'size': 5596,
            'resource': 'guid0'
        }
    }


@pytest.fixture
def callback_log_payload_upload():
    return {
        'auth': {
            'id': 'cat',
            'email': 'cat@cat.com',
            'name': 'cat',
            'callback_url': 'fakecallback.com'
        },
        'errors': [],
        'time': 70,
        'action': 'upload',
        'provider': 'osfstorage',
        'metadata': {
            'kind': 'file',
            'name': 'doc.rst',
            'resource': 'guid0',
            'modified_utc': '2017-09-01T19:34:00.175741+00:00',
            'created_utc': '2017-09-01T19:34:00.175741+00:00',
            'provider': 'osfstorage',
            'modified': '2017-09-01T19:34:00.175741+00:00',
            'size': 5596,
            'path': '/59a9b628b7d1c903ab5a8f52',
            'etag': 'eccd2270585257f4b48d8493bed863c01cf0b6dc0bb590101407c9b5e10b8e08',
            'materialized': WaterButlerPath('/doc.rst', prepend=None),
            'extra': {
                'downloads': 0,
                'guid': None,
                'hashes': {
                    'sha256': '043be9ff919762f0dc36fff0222cd90c753ce28b39feb52112be9360c476ef88',
                    'md5': 'eb3f7cc15ba7b6effb2186284185c5cf'},
                'checkout': None,
                'version': 1
            },
            'contentType': None,
            'nid': 'guid0'}
    }


@pytest.fixture
def mock_time(monkeypatch):
    mock_time = mock.Mock()
    mock_time.return_value = 10
    monkeypatch.setattr(time, 'time', mock_time)


@pytest.fixture
def mock_signed_request():
    return MockCoroutine(return_value=MockResponse())


class MockResponse:
    status = HTTPStatus.OK
    read = MockCoroutine(return_value=b'{"status": "success"}')


class MockBadResponse:
    status = HTTPStatus.INTERNAL_SERVER_ERROR
    read = MockCoroutine(return_value=b'{"status": "failure"}')
