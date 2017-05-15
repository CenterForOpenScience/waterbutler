import pytest

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

# Integration tests for FedoraProvider against a live Fedora 4 at http://localhost:8080/rest/
# Running the one-click distribution of Fedora will setup such a instance.

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
    async def setup_sandbox(self, provider):
        url = repo + test_path

        await provider.make_request(
            'DELETE', url, expects=(204,404,410)
        )

        await provider.make_request(
            'DELETE', url + '/fcr:tombstone', expects=(204,404)
        )

        await provider.make_request(
            'PUT', url, headers={'Content-Type': 'text/turtle'}, expects=(201,)
        )

    # Test that root folder return list.
    @pytest.mark.asyncio
    async def test_get_root_metadata(self, provider):
        root_path = WaterButlerPath('/')

        result = await provider.metadata(root_path)

        assert type(result) == list

    # Test uploading and downloading a file
    @pytest.mark.asyncio
    async def test_upload_file(self, provider):
        await self.setup_sandbox(provider)

        file_path = WaterButlerPath(test_path + '/data.txt')
        file_content =  b'important data'
        file_stream = streams.FileStreamReader(io.BytesIO(file_content))

        md, new = await provider.upload(file_stream, file_path)

        assert new == True
        assert md.kind == 'file'
        assert md.name == 'data.txt'
        assert md.size == len(file_content)
        assert md.content_type == 'text/plain'

        result = await provider.download(file_path)
        content = await result.response.read()

        assert file_content == content

        # Reupload the file with different data to check that new is false

        file_content =  b'important, but different, data'
        file_stream = streams.FileStreamReader(io.BytesIO(file_content))
        md, new = await provider.upload(file_stream, file_path)
        assert new == False
        assert md.kind == 'file'
        assert md.name == 'data.txt'
        assert md.size == len(file_content)
        assert md.content_type == 'text/plain'

        result = await provider.download(file_path)
        content = await result.response.read()

        assert file_content == content

    # Test uploading and downloading a file with an escaped name
    @pytest.mark.asyncio
    async def test_escaped_upload_file(self, provider):
        await self.setup_sandbox(provider)

        file_path = WaterButlerPath(test_path + '/mu%CC%88-%E0%B8%97%E0%B8%99%E0%B8%99.zip')
        file_content =  b'very important data'
        file_stream = streams.FileStreamReader(io.BytesIO(file_content))

        md, new = await provider.upload(file_stream, file_path)

        assert new == True
        assert md.kind == 'file'
        assert md.name == 'mü-ทนน.zip'
        assert md.size == len(file_content)
        assert md.content_type == 'application/zip'

        result = await provider.download(file_path)
        content = await result.response.read()

        assert file_content == content

    # Test deleting a file
    @pytest.mark.asyncio
    async def test_delete_file(self, provider):
        await self.setup_sandbox(provider)

        file_path = WaterButlerPath(test_path + '/moo.txt')
        file_content =  b'moo goes the cow'
        file_stream = streams.FileStreamReader(io.BytesIO(file_content))

        md, new = await provider.upload(file_stream, file_path)

        assert new == True
        assert md.kind == 'file'
        assert md.name == 'moo.txt'
        assert md.size == len(file_content)
        assert md.content_type == 'text/plain'

        await provider.delete(file_path)

        exists = await provider.path_exists(file_path)
        assert exists == False

    # Test that deleting the root folder deletes all the folder contents
    # Note that this test operates outside of the sandbox
    @pytest.mark.asyncio
    async def test_delete_root_folder(self, provider):
        file_path1 = WaterButlerPath('/moo1.txt')
        file_path2 = WaterButlerPath('/moo2.txt')
        file_content =  b'moo goes the cow'
        file_stream1 = streams.FileStreamReader(io.BytesIO(file_content))
        file_stream2 = streams.FileStreamReader(io.BytesIO(file_content))

        md, new = await provider.upload(file_stream1, file_path1)

        # Add two files to root

        assert md.kind == 'file'
        assert md.name == 'moo1.txt'
        assert md.size == len(file_content)
        assert md.content_type == 'text/plain'

        md, new = await provider.upload(file_stream2, file_path2)

        assert md.kind == 'file'
        assert md.name == 'moo2.txt'
        assert md.size == len(file_content)
        assert md.content_type == 'text/plain'

        root_path = WaterButlerPath('/')

        exists = await provider.path_exists(root_path)
        assert exists == True

        await provider.delete(root_path, confirm_delete=1)

        # Files should be gone, but root folder should remain

        exists = await provider.path_exists(file_path1)
        assert exists == False

        exists = await provider.path_exists(file_path2)
        assert exists == False

        exists = await provider.path_exists(root_path)
        assert exists == True

    # Test creating a folder
    @pytest.mark.asyncio
    async def test_create_folder(self, provider):
        await self.setup_sandbox(provider)

        folder_path = WaterButlerPath(test_path + '/moo/')

        md = await provider.create_folder(folder_path)

        assert md.kind == 'folder'
        assert md.name == 'moo'

        exists = await provider.path_exists(folder_path)
        assert exists == True


    # Test moving a file into a subfolder.
    @pytest.mark.asyncio
    async def test_move_file(self, provider):
        await self.setup_sandbox(provider)

        file_path = WaterButlerPath(test_path + '/data.txt')
        folder_path = WaterButlerPath(test_path + '/moo/')

        new_file_path = WaterButlerPath(test_path + '/moo/data.txt')
        file_content =  b'important data'
        file_stream = streams.FileStreamReader(io.BytesIO(file_content))

        # Create file

        await provider.upload(file_stream, file_path)

        exists = await provider.path_exists(file_path)
        assert exists != False

        # Create subfolder

        await provider.create_folder(folder_path)

        exists = await provider.path_exists(folder_path)
        assert exists != False

        # Move file to subfolder

        md, created = await provider.move(provider, file_path, new_file_path)

        assert created == True
        assert md.kind == 'file'
        assert md.name == 'data.txt'
        assert md.size == len(file_content)
        assert md.content_type == 'text/plain'

        exists = await provider.path_exists(file_path)
        assert exists == False

        exists = await provider.path_exists(new_file_path)
        assert exists != False

        # Create another file

        file_path2 = WaterButlerPath(test_path + '/hmm.txt')
        file_content2 =  b'abcdef'
        file_stream2 = streams.FileStreamReader(io.BytesIO(file_content2))

        await provider.upload(file_stream2, file_path2)

        exists = await provider.path_exists(file_path2)
        assert exists != False

        # Move the other file into the subfolder, overwriting the existing file

        md, created = await provider.move(provider, file_path2, new_file_path)

        assert created == False
        assert md.kind == 'file'
        assert md.name == 'data.txt'
        assert md.size == len(file_content2)
        assert md.content_type == 'text/plain'

        exists = await provider.path_exists(file_path)
        assert exists == False

        exists = await provider.path_exists(new_file_path)
        assert exists != False



    # Test copying a file into a subfolder
    @pytest.mark.asyncio
    async def test_copying_file(self, provider):
        await self.setup_sandbox(provider)

        file_path = WaterButlerPath(test_path + '/data.txt')
        folder_path = WaterButlerPath(test_path + '/moo/')

        new_file_path = WaterButlerPath(test_path + '/moo/data.txt')
        file_content =  b'important data'
        file_stream = streams.FileStreamReader(io.BytesIO(file_content))

        # Create file

        await provider.upload(file_stream, file_path)

        exists = await provider.path_exists(file_path)
        assert exists != False

        # Create subfolder

        await provider.create_folder(folder_path)

        exists = await provider.path_exists(folder_path)
        assert exists != False

        # Copy file to subfolder

        md, created = await provider.copy(provider, file_path, folder_path)

        assert created == True
        assert md.kind == 'file'
        assert md.name == 'data.txt'
        assert md.size == len(file_content)
        assert md.content_type == 'text/plain'

        exists = await provider.path_exists(file_path)
        assert exists != False

        exists = await provider.path_exists(new_file_path)
        assert exists != False

        # Create another file

        file_path2 = WaterButlerPath(test_path + '/hmm.txt')
        file_content2 =  b'abcdef'
        file_stream2 = streams.FileStreamReader(io.BytesIO(file_content2))

        await provider.upload(file_stream2, file_path2)

        exists = await provider.path_exists(file_path2)
        assert exists != False

        # Attempt to copy over file in subfolder

        md, created = await provider.copy(provider, file_path2, new_file_path)

        assert created == False
        assert md.kind == 'file'
        assert md.name == 'data.txt'
        assert md.size == len(file_content2)
        assert md.content_type == 'text/plain'

        exists = await provider.path_exists(file_path2)
        assert exists != False

        exists = await provider.path_exists(new_file_path)
        assert exists != False


    # Test copying a folder into a subfolder.
    @pytest.mark.asyncio
    async def test_copying_folder(self, provider):
        await self.setup_sandbox(provider)

        folder1_path = WaterButlerPath(test_path + '/cow/')
        folder2_path = WaterButlerPath(test_path + '/moo/')
        new_folder_path = WaterButlerPath(test_path + '/cow/moo/')

        # Create folders

        await provider.create_folder(folder1_path)

        exists = await provider.path_exists(folder1_path)
        assert exists != False

        await provider.create_folder(folder2_path)

        exists = await provider.path_exists(folder2_path)
        assert exists != False

        # Copy /moo to /cow.

        md, created = await provider.copy(provider, folder2_path, folder1_path)

        assert created == True
        assert md.kind == 'folder'
        assert md.name == 'moo'

        exists = await provider.path_exists(folder2_path)
        assert exists != False

        exists = await provider.path_exists(new_folder_path)
        assert exists != False

    # Test creating a pair tree object and getting its metadata
    # TODO Pair tree behavior will need some more thought and testing
    @pytest.mark.asyncio
    async def test_get_pairtree_metadata(self, provider):
        await self.setup_sandbox(provider)

        # Create pair tree container
        url = repo + test_path.strip('/')
        await provider.make_request('POST', url, expects=(201,))

        test_folder = WaterButlerPath(test_path)

        md = await provider.metadata(test_folder)

        assert len(md) == 1
