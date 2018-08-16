import os
import sys
import copy
import shutil
import asyncio
import tempfile
from unittest import mock

import pytest
from tornado import concurrent, testing
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


class MockRequestBody(concurrent.Future):

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
