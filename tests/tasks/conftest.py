import time
import asyncio
from unittest import mock

import pytest

from waterbutler.core import remote_logging
from waterbutler.core import utils as core_utils
from waterbutler.core.path import WaterButlerPath

import tests.utils as test_utils

import logging
logger = logging.getLogger(__name__)


# Fixtures for task tests

@pytest.fixture(autouse=True)
def log_to_keen(monkeypatch):
    mock_log_to_keen = test_utils.MockCoroutine()
    monkeypatch.setattr(remote_logging, 'log_to_keen', mock_log_to_keen)
    return mock_log_to_keen


@pytest.fixture
def callback(monkeypatch):
    mock_request = test_utils.MockCoroutine(return_value=(200, b'meowmeowmeow'))
    monkeypatch.setattr(core_utils, 'send_signed_request', mock_request)
    return mock_request


@pytest.fixture
def FAKE_TIME():
    return 1454684930.0


@pytest.fixture
def mock_time(monkeypatch, FAKE_TIME):
    mock_time = mock.Mock(return_value=FAKE_TIME)
    monkeypatch.setattr(time, 'time', mock_time)


@pytest.fixture
def src_path():
    return WaterButlerPath('/user/bin/python')


@pytest.fixture
def dest_path():
    return WaterButlerPath('/usr/bin/golang')


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
