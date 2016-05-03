import pytest

from tests.utils import async

import io
from http import client

import json
import asyncio
import aiohttpretty

from waterbutler.core import streams
from waterbutler.core import exceptions
from waterbutler.core import metadata
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.fedora import FedoraProvider
from waterbutler.providers.fedora.metadata import FedoraFileMetadata
from waterbutler.providers.fedora.metadata import FedoraFolderMetadata

# IT tests FedoraProvider against a live Fedora 4 at http://localhost:8080/rest/

repo = 'http://localhost:8080/rest/'

auth = {
    'name': 'Calvin',
    'email': 'bill.watterson@example.com',
}

credentials = {
    'repo': repo,
    'user': 'calvin',
    'password': 'hobbes'
}

settings = {}

# Location in Fedora repo used for testing
test_path = '/test'

@pytest.fixture
def provider():
    return FedoraProvider(auth, credentials, settings)

class TestProviderIntegration:

    # Delete sandbox used for tests and then create sandbox
    @asyncio.coroutine
    def setup_sandbox(self, provider):
        url = repo + test_path

        yield from provider.make_request(
            'DELETE', url, expects=(204,404)
        )

        yield from provider.make_request(
            'DELETE', url + '/fcr:tombstone', expects=(204,404)
        )

        yield from provider.make_request(
            'PUT', url, expects=(201,)
        )

    # Test that root folder return list.
    @async
    def test_get_root_metadata(self, provider):
        root_path = WaterButlerPath('/')

        result = yield from provider.metadata(root_path)

        assert type(result) == list

    # Test uploading and downloading a file
    @async
    def test_upload_file(self, provider):
        yield from self.setup_sandbox(provider)

        file_path = WaterButlerPath(test_path + '/data.txt')
        file_content =  b'important data'
        file_stream = streams.FileStreamReader(io.BytesIO(file_content))

        md, created = yield from provider.upload(file_stream, file_path)

        assert created == True
        assert md.kind == 'file'
        assert md.name == 'data.txt'
        assert md.size == str(len(file_content))
        assert md.content_type == 'text/plain'

        result = yield from provider.download(file_path)
        content = yield from result.response.read()

        assert file_content == content

    # Test creating a folder
    @async
    def test_create_folder(self, provider):
        yield from self.setup_sandbox(provider)

        folder_path = WaterButlerPath(test_path + '/moo/')

        md = yield from provider.create_folder(folder_path)

        assert md.kind == 'folder'
        assert md.name == 'moo'


    # Test moving a file into a subfolder.
    @async
    def test_move_file(self, provider):
        yield from self.setup_sandbox(provider)

        file_path = WaterButlerPath(test_path + '/data.txt')
        folder_path = WaterButlerPath(test_path + '/moo/')

        new_file_path = WaterButlerPath(test_path + '/moo/data.txt')
        file_content =  b'important data'
        file_stream = streams.FileStreamReader(io.BytesIO(file_content))

        # Create file

        yield from provider.upload(file_stream, file_path)

        exists = yield from provider.exists(file_path)
        assert exists != False

        # Create subfolder

        yield from provider.create_folder(folder_path)

        exists = yield from provider.exists(folder_path)
        assert exists != False

        # Move file to subfolder

        md, created = yield from provider.move(provider, file_path, new_file_path)

        assert created == True
        assert md.kind == 'file'
        assert md.name == 'data.txt'
        assert md.size == str(len(file_content))
        assert md.content_type == 'text/plain'

        exists = yield from provider.exists(file_path)
        assert exists == False

        exists = yield from provider.exists(new_file_path)
        assert exists != False


    # Test copying a file into a subfolder.
    @async
    def test_copying_file(self, provider):
        yield from self.setup_sandbox(provider)

        file_path = WaterButlerPath(test_path + '/data.txt')
        folder_path = WaterButlerPath(test_path + '/moo/')

        new_file_path = WaterButlerPath(test_path + '/moo/data.txt')
        file_content =  b'important data'
        file_stream = streams.FileStreamReader(io.BytesIO(file_content))

        # Create file

        yield from provider.upload(file_stream, file_path)

        exists = yield from provider.exists(file_path)
        assert exists != False

        # Create subfolder

        yield from provider.create_folder(folder_path)

        exists = yield from provider.exists(folder_path)
        assert exists != False

        # Copy file to subfolder

        md, created = yield from provider.copy(provider, file_path, folder_path)

        assert created == True
        assert md.kind == 'file'
        assert md.name == 'data.txt'
        assert md.size == str(len(file_content))
        assert md.content_type == 'text/plain'

        exists = yield from provider.exists(file_path)
        assert exists != False

        exists = yield from provider.exists(new_file_path)
        assert exists != False

    # Test copying a folder into a subfolder.
    @async
    def test_copying_folder(self, provider):
        yield from self.setup_sandbox(provider)

        folder1_path = WaterButlerPath(test_path + '/cow/')
        folder2_path = WaterButlerPath(test_path + '/moo/')
        new_folder_path = WaterButlerPath(test_path + '/cow/moo/')

        # Create folders

        yield from provider.create_folder(folder1_path)

        exists = yield from provider.exists(folder1_path)
        assert exists != False

        yield from provider.create_folder(folder2_path)

        exists = yield from provider.exists(folder2_path)
        assert exists != False

        # Copy /moo to /cow.

        md, created = yield from provider.copy(provider, folder2_path, folder1_path)

        assert created == True
        assert md.kind == 'folder'
        assert md.name == 'moo'

        exists = yield from provider.exists(folder2_path)
        assert exists != False

        exists = yield from provider.exists(new_folder_path)
        assert exists != False
