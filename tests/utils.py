import asyncio
import os
import shutil
import tempfile
from unittest import mock

from decorator import decorator

import pytest
from tornado import testing
from tornado.platform.asyncio import AsyncIOMainLoop

from waterbutler.core import metadata
from waterbutler.core import provider
from waterbutler.server.app import make_app
from waterbutler.core.path import WaterButlerPath


class MockCoroutine(mock.Mock):
    @asyncio.coroutine
    def __call__(self, *args, **kwargs):
        return super().__call__(*args, **kwargs)


@decorator
def async(func, *args, **kwargs):
    future = func(*args, **kwargs)
    asyncio.get_event_loop().run_until_complete(future)


class MockFileMetadata(metadata.BaseFileMetadata):
    provider = 'mock'
    name = 'Foo.name'
    size = 1337
    etag = 'etag'
    path = '/Foo.name'
    modified = 'never'
    content_type = 'application/octet-stream'

    def __init__(self):
        super().__init__({})


class MockFolderMetadata(metadata.BaseFolderMetadata):
    provider = 'mock'
    name = 'Bar'
    size = 1337
    etag = 'etag'
    path = '/Bar/'
    modified = 'never'
    content_type = 'application/octet-stream'

    def __init__(self):
        super().__init__({})


class MockProvider1(provider.BaseProvider):

    NAME = 'MockProvider1'

    @asyncio.coroutine
    def validate_path(self, path, **kwargs):
        return WaterButlerPath(path)

    @asyncio.coroutine
    def upload(self, stream, path, **kwargs):
        return MockFileMetadata(), True

    @asyncio.coroutine
    def delete(self, path, **kwargs):
        pass

    @asyncio.coroutine
    def metadata(self, path, throw=None, **kwargs):
        if throw:
            raise throw
        return MockFolderMetadata()

    @asyncio.coroutine
    def download(self, path, **kwargs):
        return b''


class MockProvider2(MockProvider1):

    NAME = 'MockProvider2'

    def can_intra_move(self, other, path=None):
        return self.__class__ == other.__class__

    def can_intra_copy(self, other, path=None):
        return self.__class__ == other.__class__

    @asyncio.coroutine
    def web_view(self, **kwargs):
        return 'www.url.com'


class HandlerTestCase(testing.AsyncHTTPTestCase):

    def setUp(self):
        super().setUp()
        identity_future = asyncio.Future()
        identity_future.set_result({
            'auth': {},
            'credentials': {},
            'settings': {},
        })
        self.mock_identity = mock.Mock()
        self.mock_identity.return_value = identity_future
        self.identity_patcher = mock.patch('waterbutler.server.handlers.core.auth_handler.fetch', self.mock_identity)

        self.mock_provider = MockProvider1({}, {}, {})
        self.mock_make_provider = mock.Mock(return_value=self.mock_provider)
        self.make_provider_patcher = mock.patch('waterbutler.core.utils.make_provider', self.mock_make_provider)

        self.identity_patcher.start()
        self.make_provider_patcher.start()

    def tearDown(self):
        super().tearDown()
        self.identity_patcher.stop()
        self.make_provider_patcher.stop()

    def get_app(self):
        return make_app(debug=False)

    def get_new_ioloop(self):
        return AsyncIOMainLoop()


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
