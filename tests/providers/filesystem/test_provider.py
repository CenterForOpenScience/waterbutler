import pytest

import io
import os
import shutil
from http import client

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
    os.mkdir(os.path.join(provider.folder, 'other_subfolder'))

    with open(os.path.join(provider.folder, 'subfolder', 'nested.txt'), 'wb') as fp:
        fp.write(b'Here is my content')


class TestValidatePath:

    @pytest.mark.asyncio
    async def test_validate_v1_path_file(self, provider):
        try:
            wb_path_v1 = await provider.validate_v1_path('/flower.jpg')
        except Exception as exc:
            pytest.fail(str(exc))

        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider.validate_v1_path('/flower.jpg/')

        assert exc.value.code == client.NOT_FOUND

        wb_path_v0 = await provider.validate_path('/flower.jpg')

        assert wb_path_v1 == wb_path_v0

    @pytest.mark.asyncio
    async def test_validate_v1_path_folder(self, provider):
        try:
            wb_path_v1 = await provider.validate_v1_path('/subfolder/')
        except Exception as exc:
            pytest.fail(str(exc))

        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider.validate_v1_path('/subfolder')

        assert exc.value.code == client.NOT_FOUND

        wb_path_v0 = await provider.validate_path('/subfolder/')

        assert wb_path_v1 == wb_path_v0


class TestCRUD:

    @pytest.mark.asyncio
    async def test_download(self, provider):
        path = await provider.validate_path('/flower.jpg')

        result = await provider.download(path)
        content = await result.read()

        assert content == b'I am a file'

    @pytest.mark.asyncio
    async def test_download_range(self, provider):
        path = await provider.validate_path('/flower.jpg')

        result = await provider.download(path, range=(0, 1))
        assert result.partial
        content = await result.read()
        assert content == b'I '

        result = await provider.download(path, range=(2, 5))
        assert result.partial
        content = await result.read()
        assert content == b'am a'

    @pytest.mark.asyncio
    async def test_download_range_open_ended(self, provider):
        path = await provider.validate_path('/flower.jpg')

        result = await provider.download(path, range=(0, None))
        assert hasattr('result', 'partial') == False
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
        assert metadata.size_as_int == len(file_content)
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
        assert metadata.size_as_int == len(file_content)
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
        assert metadata.size_as_int == len(file_content)
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
        assert metadata.size_as_int == len(file_content)
        assert created is False

    @pytest.mark.asyncio
    async def test_delete_file(self, provider):
        path = await provider.validate_path('/flower.jpg')

        await provider.delete(path)

        with pytest.raises(exceptions.MetadataError):
            await provider.metadata(path)

    @pytest.mark.asyncio
    async def test_delete_folder(self, provider):
        path = await provider.validate_path('/subfolder/')

        await provider.delete(path)

        with pytest.raises(exceptions.MetadataError):
            await provider.metadata(path)

    @pytest.mark.asyncio
    async def test_delete_root(self, provider):
        path = await provider.validate_path('/')

        await provider.delete(path)

        assert os.path.exists(provider.folder)


class TestMetadata:

    @pytest.mark.asyncio
    async def test_metadata(self, provider):
        path = await provider.validate_path('/')
        result = await provider.metadata(path)

        assert isinstance(result, list)
        assert len(result) == 3

        file = next(x for x in result if x.kind == 'file')
        assert file.name == 'flower.jpg'
        assert file.path == '/flower.jpg'
        folder = next(x for x in result if x.kind == 'folder')
        assert folder.name == 'subfolder' or 'other_subfolder'
        assert folder.path == '/subfolder/' or '/other_subfolder/'

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


class TestIntra:

    @pytest.mark.asyncio
    async def test_intra_copy_file(self, provider):
        src_path = await provider.validate_path('/flower.jpg')
        dest_path = await provider.validate_path('/subfolder/flower.jpg')

        result = await provider.intra_copy(provider, src_path, dest_path)

        assert result[1] is True
        assert isinstance(result[0], metadata.BaseFileMetadata)
        assert result[0].path == '/subfolder/flower.jpg'
        assert result[0].kind == 'file'
        assert result[0].name == 'flower.jpg'

    @pytest.mark.asyncio
    async def test_intra_move_folder(self, provider):
        src_path = await provider.validate_path('/subfolder/')
        dest_path = await provider.validate_path('/other_subfolder/subfolder/')

        result = await provider.intra_move(provider, src_path, dest_path)

        assert result[1] is True
        assert result[0][0].path == '/other_subfolder/subfolder/nested.txt'
        assert result[0][0].kind == 'file'
        assert result[0][0].name == 'nested.txt'

    @pytest.mark.asyncio
    async def test_intra_move_file(self, provider):
        src_path = await provider.validate_path('/flower.jpg')
        dest_path = await provider.validate_path('/subfolder/flower.jpg')

        result = await provider.intra_move(provider, src_path, dest_path)

        assert result[1] is True
        assert isinstance(result[0], metadata.BaseFileMetadata)
        assert result[0].path == '/subfolder/flower.jpg'
        assert result[0].kind == 'file'
        assert result[0].name == 'flower.jpg'


class TestOperations:

    def test_can_duplicate_names(self, provider):
        assert provider.can_duplicate_names() is False

    def test_can_intra_copy(self, provider):
        assert provider.can_intra_copy(provider)

    def test_can_intra_move(self, provider):
        assert provider.can_intra_move(provider)
