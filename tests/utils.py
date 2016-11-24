import asyncio
import os
import copy
import shutil
import tempfile
import json

from unittest import mock
from copy import deepcopy
from difflib import unified_diff

import pytest
from tornado import testing
from tornado.platform.asyncio import AsyncIOMainLoop

from waterbutler.core import metadata
from waterbutler.core import provider
from waterbutler.server.app import make_app
from waterbutler.core.path import WaterButlerPath


class MockCoroutine(mock.Mock):
    async def __call__(self, *args, **kwargs):
        return super().__call__(*args, **kwargs)


class MockFileMetadata(metadata.BaseFileMetadata):
    provider = 'MockProvider'
    name = 'Foo.name'
    size = 1337
    etag = 'etag'
    path = '/Foo.name'
    modified = 'never'
    modified_utc = 'never'
    content_type = 'application/octet-stream'

    def __init__(self):
        super().__init__({})


class MockFolderMetadata(metadata.BaseFolderMetadata):
    provider = 'MockProvider'
    name = 'Bar'
    size = 1337
    etag = 'etag'
    path = '/Bar/'
    modified = 'never'
    content_type = 'application/octet-stream'

    def __init__(self):
        super().__init__({})


class MockFileRevisionMetadata(metadata.BaseFileRevisionMetadata):
    version = 1
    version_identifier = 'versions'
    modified = 'never'
    modified_utc = 'never'

    def __init__(self):
        super().__init__({})


class MockProvider(provider.BaseProvider):
    NAME = 'MockProvider'
    copy = None
    move = None
    delete = None
    upload = None
    download = None
    metadata = None
    validate_v1_path = None
    validate_path = None
    revalidate_path = None
    can_duplicate_names = True

    def __init__(self, auth=None, creds=None, settings=None):
        super().__init__(auth or {}, creds or {}, settings or {})
        self.copy = MockCoroutine()
        self.move = MockCoroutine()
        self.delete = MockCoroutine()
        self.upload = MockCoroutine()
        self.download = MockCoroutine()
        self.metadata = MockCoroutine()
        self.validate_v1_path = MockCoroutine()
        self.revalidate_path = MockCoroutine()


class MockProvider1(provider.BaseProvider):

    NAME = 'MockProvider1'

    async def validate_v1_path(self, path, **kwargs):
        return await self.validate_path(path, **kwargs)

    async def validate_path(self, path, **kwargs):
        return WaterButlerPath(path)

    async def upload(self, stream, path, **kwargs):
        return MockFileMetadata(), True

    async def delete(self, path, **kwargs):
        pass

    async def metadata(self, path, throw=None, **kwargs):
        if throw:
            raise throw
        return MockFolderMetadata()

    async def download(self, path, **kwargs):
        return b''

    def can_duplicate_names(self):
        return True


class MockProvider2(MockProvider1):

    NAME = 'MockProvider2'

    def can_intra_move(self, other, path=None):
        return self.__class__ == other.__class__

    def can_intra_copy(self, other, path=None):
        return self.__class__ == other.__class__


class HandlerTestCase(testing.AsyncHTTPTestCase):

    def setUp(self):
        policy = asyncio.get_event_loop_policy()
        policy.get_event_loop().close()
        self.event_loop = policy.new_event_loop()
        policy.set_event_loop(self.event_loop)

        super().setUp()

        def get_identity(*args, **kwargs):
            return copy.deepcopy({
                'auth': {},
                'credentials': {},
                'settings': {},
                'callback_url': 'example.com'
            })

        self.mock_identity = MockCoroutine(side_effect=get_identity)

        # self.mock_identity.return_value = identity_future
        self.identity_patcher = mock.patch('waterbutler.server.api.v0.core.auth_handler.fetch', self.mock_identity)

        self.mock_provider = MockProvider1({}, {}, {})
        self.mock_make_provider = mock.Mock(return_value=self.mock_provider)
        self.make_provider_patcher = mock.patch('waterbutler.core.utils.make_provider', self.mock_make_provider)

        if hasattr(self, 'HOOK_PATH'):
            self.mock_send_hook = mock.Mock()
            self.send_hook_patcher = mock.patch(self.HOOK_PATH, self.mock_send_hook)
            self.send_hook_patcher.start()

        self.identity_patcher.start()
        self.make_provider_patcher.start()

    def tearDown(self):
        super().tearDown()
        self.identity_patcher.stop()
        if hasattr(self, 'HOOK_PATH'):
            self.send_hook_patcher.stop()
        self.make_provider_patcher.stop()
        self.event_loop.close()

    def get_app(self):
        return make_app(debug=False)

    def get_new_ioloop(self):
        return AsyncIOMainLoop()


class MultiProviderHandlerTestCase(HandlerTestCase):

    def setUp(self):
        super().setUp()
        self.source_provider = MockProvider2({}, {}, {})
        self.destination_provider = MockProvider2({}, {}, {})

        self.mock_make_provider.return_value = None
        self.mock_make_provider.side_effect = [
            self.source_provider,
            self.destination_provider
        ]

    def tearDown(self):
        super().tearDown()

    def payload(self):
        return copy.deepcopy({
            'source': {
                'nid': 'foo',
                'provider': 'source',
                'path': '/source/path',
                'callback_url': 'example.com'
            },
            'destination': {
                'nid': 'bar',
                'provider': 'destination',
                'path': '/destination/path',
                'callback_url': 'example.com'
            }
        })


class TempFilesContext:
    def __init__(self):
        self._dir = tempfile.mkdtemp()
        self.files = []

    def add_file(self, filename=None):
        _, path = tempfile.mkstemp(dir=self._dir)

        if filename:
            os.rename(path, os.path.join(self._dir, filename))

        return path

    def tear_down(self):
        shutil.rmtree(self._dir)


@pytest.yield_fixture
def temp_files():
    context = TempFilesContext()
    yield context
    context.tear_down()


def _check_list(items):
    return len(items) > 1 and all(isinstance(v, dict) and v.get('id') for v in items)


def _reorder(obj):
    if isinstance(obj, dict):
        for k, v in obj.items():
            obj[k] = _reorder(v)
    if isinstance(obj, tuple):
        obj = tuple(_reorder(list(obj)))
    if isinstance(obj, list):
        # A list of dicts can't be sorted by default. _check_list is used
        # to check if all items of list are dicts and has an 'id' attr
        if _check_list(obj):
            obj = sorted(obj, key=lambda item: item['id'])
        else:
            obj = sorted(obj)
        for i, v in enumerate(obj):
            obj[i] = _reorder(v)
    return obj


def assert_deep_equal(payload1, payload2):
    payload1 = json.loads(payload1) if isinstance(payload1, str) else payload1
    payload2 = json.loads(payload2) if isinstance(payload2, str) else payload2

    f = _reorder(deepcopy(payload1))
    s = _reorder(deepcopy(payload2))
    payload1_str = json.dumps(f, indent=4, sort_keys=True)
    payload2_str = json.dumps(s, indent=4, sort_keys=True)
    diff = '\n'.join(unified_diff(
        payload1_str.splitlines(), payload2_str.splitlines(), fromfile='payload1 argument', tofile='payload2 argument'))

    if diff:
        raise AssertionError('Payloads are not equals.\n' + diff)
