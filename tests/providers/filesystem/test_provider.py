import pytest

import io
import os
import shutil

from waterbutler.core import streams
from waterbutler.core import metadata
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.filesystem import FileSystemProvider
from waterbutler.providers.filesystem.metadata import FileSystemFileMetadata


@pytest.fixture
def auth():
    return {}


@pytest.fixture
def credentials():
    return {}


@pytest.fixture
def settings(tmpdir):
    return {'folder': str(tmpdir)}


@pytest.fixture
def provider(auth, credentials, settings):
    return FileSystemProvider(auth, credentials, settings)


@pytest.fixture(scope="function", autouse=True)
def setup_filesystem(provider):
    shutil.rmtree(provider.folder)
    os.makedirs(provider.folder, exist_ok=True)

    with open(os.path.join(provider.folder, 'flower.jpg'), 'wb') as fp:
        fp.write(b'I am a file')

    os.mkdir(os.path.join(provider.folder, 'subfolder'))

    with open(os.path.join(provider.folder, 'subfolder', 'nested.txt'), 'wb') as fp:
        fp.write(b'Here is my content')


class TestCRUD:

    @pytest.mark.asyncio
    async def test_download(self, provider):
        path = await provider.validate_path('/flower.jpg')

        result = await provider.download(path)
        content = await result.read()

        assert content == b'I am a file'

    @pytest.mark.asyncio
    async def test_download_not_found(self, provider):
        path = await provider.validate_path('/missing.txt')

        with pytest.raises(exceptions.DownloadError):
            await provider.download(path)

    @pytest.mark.asyncio
    async def test_upload_create(self, provider):
        file_name = 'upload.txt'
        file_folder = '/'
        file_path = os.path.join(file_folder, file_name)
        file_content = b'Test Upload Content'
        file_stream = streams.StringStream(file_content)

        path = await provider.validate_path(file_path)
        metadata, created = await provider.upload(file_stream, path)

        assert metadata.name == file_name
        assert metadata.path == file_path
        assert metadata.size == len(file_content)
        assert created is True

    @pytest.mark.asyncio
    async def test_upload_update(self, provider):
        file_name = 'flower.jpg'
        file_folder = '/'
        file_path = os.path.join(file_folder, file_name)
        file_content = b'Short and stout'
        file_stream = streams.FileStreamReader(io.BytesIO(file_content))

        path = await provider.validate_path(file_path)
        metadata, created = await provider.upload(file_stream, path)

        assert metadata.name == file_name
        assert metadata.path == file_path
        assert metadata.size == len(file_content)
        assert created is False

    @pytest.mark.asyncio
    async def test_upload_nested_create(self, provider):
        file_name = 'new.txt'
        file_folder = '/newsubfolder'
        file_path = os.path.join(file_folder, file_name)
        file_content = b'Test New Nested Content'
        file_stream = streams.FileStreamReader(io.BytesIO(file_content))

        path = await provider.validate_path(file_path)
        metadata, created = await provider.upload(file_stream, path)

        assert metadata.name == file_name
        assert metadata.path == file_path
        assert metadata.size == len(file_content)
        assert created is True

    @pytest.mark.asyncio
    async def test_upload_nested_update(self, provider):
        file_name = 'nested.txt'
        file_folder = '/subfolder'
        file_path = os.path.join(file_folder, file_name)
        file_content = b'Test Update Nested Content'
        file_stream = streams.FileStreamReader(io.BytesIO(file_content))

        path = await provider.validate_path(file_path)
        metadata, created = await provider.upload(file_stream, path)

        assert metadata.name == file_name
        assert metadata.path == file_path
        assert metadata.size == len(file_content)
        assert created is False

    @pytest.mark.asyncio
    async def test_delete_file(self, provider):
        path = await provider.validate_path('/flower.jpg')

        await provider.delete(path)

        with pytest.raises(exceptions.MetadataError):
            await provider.metadata(path)


class TestMetadata:

    @pytest.mark.asyncio
    async def test_metadata(self, provider):
        path = await provider.validate_path('/')
        result = await provider.metadata(path)

        assert isinstance(result, list)
        assert len(result) == 2

        file = next(x for x in result if x.kind == 'file')
        assert file.name == 'flower.jpg'
        assert file.path == '/flower.jpg'
        folder = next(x for x in result if x.kind == 'folder')
        assert folder.name == 'subfolder'
        assert folder.path == '/subfolder/'

    @pytest.mark.asyncio
    async def test_metadata_root_file(self, provider):
        path = await provider.validate_path('/flower.jpg')
        result = await provider.metadata(path)

        assert isinstance(result, metadata.BaseFileMetadata)
        assert result.kind == 'file'
        assert result.name == 'flower.jpg'
        assert result.path == '/flower.jpg'

    @pytest.mark.asyncio
    async def test_metadata_missing(self, provider):
        path = await provider.validate_path('/missing.txt')

        with pytest.raises(exceptions.MetadataError):
            await provider.metadata(path)


class TestOperations:

    async def test_can_intra_copy(self, provider):
        assert provider.can_intra_copy(provider)

    async def test_can_intra_move(self, provider):
        assert provider.can_intra_move(provider)
