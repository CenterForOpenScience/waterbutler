import os
import sys
import copy
import shutil
import asyncio
import tempfile
from unittest import mock

import pytest
from tornado import testing
from tornado.platform.asyncio import AsyncIOMainLoop

from waterbutler.server.app import make_app
from waterbutler.core import metadata, provider
from waterbutler.core.path import WaterButlerPath
from waterbutler.core.streams.file import FileStreamReader


class MockCoroutine(mock.Mock):

    if sys.version_info >= (3, 5, 3):
        _is_coroutine = asyncio.coroutines._is_coroutine

    async def __call__(self, *args, **kwargs):
        return super().__call__(*args, **kwargs)

    def assert_awaited_once(self):
        assert self.call_count == 1


class MockFileMetadata(metadata.BaseFileMetadata):
    provider = 'MockProvider'
    name = 'Foo.name'
    size = 1337
    etag = 'etag'
    path = '/Foo.name'
    modified = '9/25/2017'
    modified_utc = '1991-09-25T19:20:30.45+01:00'
    created_utc = 'always'
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
    created_utc = 'always'

    def __init__(self):
        super().__init__({})


class MockStream(FileStreamReader):
    content_type = 'application/octet-stream'
    size = 1334

    def __init__(self):
        super().__init__(tempfile.TemporaryFile())


class MockRequestBody(asyncio.Future):

    def __await__(self):
        yield None


class MockWriter(object):
    write = mock.Mock()
    drain = MockCoroutine()


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
        self.revalidate_path = MockCoroutine(
            side_effect=lambda base, path, *args, **kwargs: base.child(path, *args, **kwargs))
        self.validate_v1_path = MockCoroutine(
            side_effect=lambda path,  **kwargs: WaterButlerPath(path, **kwargs))
        self.validate_path = MockCoroutine(
            side_effect=lambda path, **kwargs: WaterButlerPath(path, **kwargs))


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
