import pytest

from tests.utils import MockCoroutine

import io
import time
import base64
import hashlib
from http import client
from unittest import mock

import aiohttpretty

from waterbutler.core import streams
from waterbutler.core import metadata
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.weko import WEKOProvider
from waterbutler.providers.weko.metadata import WEKOItemMetadata
from waterbutler.providers.weko.metadata import WEKOIndexMetadata


@pytest.fixture
def auth():
    return {
        'name': 'cat',
        'email': 'cat@cat.com'
    }


@pytest.fixture(scope='module', params=['token', 'password'])
def credentials(request):
    return {
        request.param: 'open inside',
        'user_id': 'requester'
    }


@pytest.fixture
def settings():
    return {
        'url': 'http://localhost/test',
        'index_id': 'that kerning',
        'index_title': 'sample archive',
        'nid': 'project_id'
    }

@pytest.fixture
def mock_time(monkeypatch):
    mock_time = mock.Mock(return_value=1454684930.0)
    monkeypatch.setattr(time, 'time', mock_time)


@pytest.fixture
def provider(auth, credentials, settings):
    provider = WEKOProvider(auth, credentials, settings)
    return provider


@pytest.fixture
def file_content():
    return b'sleepy'


@pytest.fixture
def file_like(file_content):
    return io.BytesIO(file_content)


@pytest.fixture
def file_stream(file_like):
    return streams.FileStreamReader(file_like)


class TestValidatePath:

    @pytest.mark.asyncio
    async def test_normal_name(self, provider, mock_time):
        path = await provider.validate_path('/this/is/a/path.txt')
        assert path.name == 'path.txt'
        assert path.parent.name == 'a'
        assert path.is_file
        assert not path.is_dir
        assert not path.is_root

    @pytest.mark.asyncio
    async def test_folder(self, provider, mock_time):
        path = await provider.validate_path('/this/is/a/folder/')
        assert path.name == 'folder'
        assert path.parent.name == 'a'
        assert not path.is_file
        assert path.is_dir
        assert not path.is_root

    @pytest.mark.asyncio
    async def test_root(self, provider, mock_time):
        path = await provider.validate_path('/this/is/a/folder/')
        assert path.name == 'folder'
        assert path.parent.name == 'a'
        assert not path.is_file
        assert path.is_dir
        assert not path.is_root


class TestOperations:

    async def test_equality(self, provider, mock_time):
        assert provider.can_intra_copy(provider)
        assert provider.can_intra_move(provider)
