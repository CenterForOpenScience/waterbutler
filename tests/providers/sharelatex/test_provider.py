import pytest

import io
import json
import random
import aiohttpretty

from waterbutler.core import streams
from waterbutler.core import exceptions

from waterbutler.providers.sharelatex import metadata
from waterbutler.providers.sharelatex import provider
from waterbutler.providers.sharelatex.metadata import ShareLatexFileMetadata
from waterbutler.providers.sharelatex.metadata import ShareLatexFolderMetadata

from tests.providers.sharelatex import fixtures


@pytest.fixture
def auth():
    return {
        'name': 'cat',
        'email': 'cat@cat.com',
    }

@pytest.fixture
def credentials():
    return {
        'auth_token': 'brian',
        'sharelatex_url': 'www.sharelatex.com',
    }

@pytest.fixture
def settings():
    return {
        'project': 'to define',
    }

@pytest.fixture
def empty_project_settings():
    return {
        'project': fixtures.empty_project_id
    }

@pytest.fixture
def empty_project_provider(auth, credentials, empty_project_settings):
    return provider.ShareLatexProvider(auth, credentials, empty_project_settings)

@pytest.fixture
def only_files_metadata():
    return fixtures.only_files_metadata

@pytest.fixture
def only_folders_metadata():
    return fixtures.only_folders_metadata

@pytest.fixture
def empty_metadata():
    return {}

@pytest.fixture
def default_project_provider(auth, credentials, settings):
    return provider.ShareLatexProvider(auth, credentials, settings)

@pytest.fixture
def default_project_metadata():
    return fixtures.default_project_metadata

@pytest.fixture
def only_docs_metadata():
    return fixtures.only_docs_metadata


class TestMetadata:


    def only_files(self, items):
        result = []
        for f in items:
            if f.kind == 'file':
                result.append(f)
        return result

    def contains_file_with_type(self, items, t):
        result = []
        files = self.only_files(items)
        for f in files:
            path = str(f.path)
            if path.find(t) != -1:
                result.append(f)
        return result

    def check_metadata_kind(self, metadata, kind):
        assert metadata.provider == 'sharelatex'
        assert metadata.kind == kind

    def check_kind_is_folder(self, metadata):
        self.check_metadata_kind(metadata, 'folder')

    def check_kind_is_file(self, metadata):
        self.check_metadata_kind(metadata, 'file')

    def check_metadata_file(self, metadata, extension):
        self.check_kind_is_file(metadata)
        path_has_extension = metadata.path.find(extension) != -1
        assert path_has_extension

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_no_root_folder(self, empty_project_provider, empty_metadata):
        root_folder_path = await empty_project_provider.validate_path('/')
        root_folder_url = empty_project_provider.build_url('project', empty_project_provider.project_id, 'docs')
        aiohttpretty.register_json_uri('GET', root_folder_url, body=empty_metadata)

        with pytest.raises(exceptions.NotFoundError) as e:
            await empty_project_provider.metadata(root_folder_path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_file_not_found(self, empty_project_provider, default_project_metadata):
        path = await empty_project_provider.validate_path('/a.txt')
        url = empty_project_provider.build_url('project', empty_project_provider.project_id, 'docs')
        aiohttpretty.register_json_uri('GET', url, body=default_project_metadata)

        with pytest.raises(exceptions.NotFoundError) as e:
            await empty_project_provider.metadata(path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_not_found(self, default_project_provider, empty_metadata):
        path = await default_project_provider.validate_path('/')
        url = default_project_provider.build_url('project', default_project_provider.project_id, 'docs')
        aiohttpretty.register_json_uri('GET', url, status=404)

        with pytest.raises(exceptions.MetadataError) as e:
            await default_project_provider.metadata(path)

        assert e.value.code == 404

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_root_folder_without_folders(self, default_project_provider, only_files_metadata):
        root_folder_path = await default_project_provider.validate_path('/')
        root_folder_url = default_project_provider.build_url('project', default_project_provider.project_id, 'docs')

        aiohttpretty.register_json_uri('GET', root_folder_url, body=only_files_metadata)

        result = await default_project_provider.metadata(root_folder_path)

        assert result
        for f in result:
            self.check_kind_is_file(f)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_root_folder_without_files(self, default_project_provider, only_folders_metadata):
        root_folder_path = await default_project_provider.validate_path('/')
        root_folder_url = default_project_provider.build_url('project', default_project_provider.project_id, 'docs')

        aiohttpretty.register_json_uri('GET', root_folder_url, body=only_folders_metadata)

        result = await default_project_provider.metadata(root_folder_path)

        assert result
        for f in result:
            self.check_kind_is_folder(f)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_tex_on_root_folder(self, default_project_provider, only_docs_metadata):
        path = await default_project_provider.validate_path('/')
        url = default_project_provider.build_url('project', default_project_provider.project_id, 'docs')

        aiohttpretty.register_json_uri('GET', url, body=only_docs_metadata)

        result = await default_project_provider.metadata(path)
        assert result

        for f in result:
            self.check_metadata_file(f, '.tex')

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_other_files_on_root_folder(self, default_project_provider, default_project_metadata):
        path = await default_project_provider.validate_path('/')
        url = default_project_provider.build_url('project', default_project_provider.project_id, 'docs')

        aiohttpretty.register_json_uri('GET', url, body=default_project_metadata)

        result = await default_project_provider.metadata(path)

        fonts = self.contains_file_with_type(result, 'otf')
        images = self.contains_file_with_type(result, 'jpg')
        files = self.only_files(result)

        assert fonts
        assert images
        assert files

        for font in fonts:
            assert font.content_type == 'application/x-font-opentype'
            self.check_metadata_file(font, '.otf')

        for image in images:
            assert image.content_type == 'image/jpeg'
            self.check_metadata_file(image, '.jpg')

        for f in files:
            self.check_kind_is_file(f)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_file_on_root_folder(self, default_project_provider, default_project_metadata):
        path = await default_project_provider.validate_path('/raw.txt')
        url = default_project_provider.build_url('project', default_project_provider.project_id, 'docs')

        aiohttpretty.register_json_uri('GET', url, body=default_project_metadata)

        result = await default_project_provider.metadata(path)

        assert result.name == 'raw.txt'
        assert result.kind == 'file'
        assert result.content_type == 'text/plain'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_file_in_one_level_dir(self, default_project_provider, default_project_metadata):
        raw_path = '/UmDiretorioNaRaiz/pngImage.png'
        path = await default_project_provider.validate_path(raw_path)
        url = default_project_provider.build_url('project', default_project_provider.project_id, 'docs')

        aiohttpretty.register_json_uri('GET', url, body=default_project_metadata)

        result = await default_project_provider.metadata(path)

        assert result.name == 'pngImage.png'
        assert result.kind == 'file'
        assert result.content_type == 'image/png'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_folder_in_one_level_dir(self, default_project_provider, default_project_metadata):
        path = await default_project_provider.validate_path('/UmDiretorioNaRaiz/')
        url = default_project_provider.build_url('project', default_project_provider.project_id, 'docs')

        aiohttpretty.register_json_uri('GET', url, body=default_project_metadata)

        result = await default_project_provider.metadata(path)

        assert result

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_file_in_two_level_dir(self, default_project_provider, default_project_metadata):
        path = await default_project_provider.validate_path('/UmDiretorioNaRaiz/secondLevel/more.txt')
        url = default_project_provider.build_url('project', default_project_provider.project_id, 'docs')

        aiohttpretty.register_json_uri('GET', url, body=default_project_metadata)

        result = await default_project_provider.metadata(path)

        assert result.name == 'more.txt'
        assert result.kind == 'file'
        assert result.content_type == 'text/plain'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_folder_in_two_level_dir(self, default_project_provider, default_project_metadata):
        path = await default_project_provider.validate_path('/UmDiretorioNaRaiz/secondLevel/')
        url = default_project_provider.build_url('project', default_project_provider.project_id, 'docs')

        aiohttpretty.register_json_uri('GET', url, body=default_project_metadata)

        result = await default_project_provider.metadata(path)

        assert result

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_tex_in_one_level_dir(self, default_project_provider, default_project_metadata):
        path = await default_project_provider.validate_path('/UmDiretorioNaRaiz/example.tex')
        url = default_project_provider.build_url('project', default_project_provider.project_id, 'docs')
        aiohttpretty.register_json_uri('GET', url, body=default_project_metadata)

        result = await default_project_provider.metadata(path)

        assert result.name == 'example.tex'
        assert result.kind == 'file'
        assert result.content_type == 'application/x-tex'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_tex_in_two_level_dir(self, default_project_provider, default_project_metadata):
        path = await default_project_provider.validate_path('/UmDiretorioNaRaiz/secondLevel/document.tex')
        url = default_project_provider.build_url('project', default_project_provider.project_id, 'docs')

        aiohttpretty.register_json_uri('GET', url, body=default_project_metadata)

        result = await default_project_provider.metadata(path)

        assert path.is_file
        assert result.name == 'document.tex'
        assert result.kind == 'file'
        assert result.content_type == 'application/x-tex'

    def test_file_metadata(self):
        name = 'test.txt'
        path = '/one/two/three/test.txt'
        size = '1234'
        mimetype = 'text/plain'
        raw = {
            'name': name,
            'path': path,
            'size': size,
            'mimetype': mimetype
        }

        metadata = ShareLatexFileMetadata(raw)

        self.check_metadata_file(metadata, 'txt')
        assert metadata.name == name
        assert metadata.size == size
        assert metadata.path == path
        assert metadata.content_type == mimetype
        assert metadata.modified == None
        assert metadata.extra['status'] == 'ok'

    def test_folder_metadata(self):
        path = '/one/two/three/four'
        name = 'four'
        raw = {
            'name': name,
            'path': path
        }

        metadata = ShareLatexFolderMetadata(raw)

        self.check_kind_is_folder(metadata)
        assert metadata.name == name
        assert metadata.path == path


class TestCRUD:


    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_error(self, empty_project_provider):
        path = await empty_project_provider.validate_path('/')
        url = empty_project_provider.build_url('project', empty_project_provider.project_id, 'file', path.path)
        aiohttpretty.register_uri('GET', url, status=404)

        with pytest.raises(exceptions.DownloadError) as e:
            await empty_project_provider.download(path)

        assert e.value.code == 404

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_any_content(self, default_project_provider):
        body = b'castle on a cloud'
        path = await default_project_provider.validate_path('/raw.txt')
        url = default_project_provider.build_url('project', default_project_provider.project_id, 'file', path.path)
        aiohttpretty.register_uri('GET', url, body=body)

        result = await default_project_provider.download(path)
        content = await result.read()

        assert content == body

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_when_accept_url(self, default_project_provider):
        path = await default_project_provider.validate_path('/raw.txt')
        url = default_project_provider.build_url('project', default_project_provider.project_id, 'file', path.path)
        aiohttpretty.register_uri('GET', url)

        result = await default_project_provider.download(path, accept_url=True)
        assert result == url


class TestOperations:

    def test_can_intra_copy(self, default_project_provider):
        result = default_project_provider.can_intra_copy(default_project_provider)
        assert result == False

    def test_can_intra_move(self, default_project_provider):
        result = default_project_provider.can_intra_move(default_project_provider)
        assert result == False

    def test_can_duplicate_names(self, default_project_provider):
        result = default_project_provider.can_duplicate_names()
        assert result == False


class TestValidatePath:

    @pytest.mark.asyncio
    async def test_path_generation(self, default_project_provider):
        root_path = '/'
        file_path = '/one/two/three/four.abc'
        folder_path = '/one/two/three/'

        r_path = await default_project_provider.validate_path(root_path)
        fil_path = await default_project_provider.validate_path(file_path)
        fol_path = await default_project_provider.validate_path(folder_path)

        assert r_path.is_root
        assert fil_path.is_file
        assert fol_path.is_dir
        assert r_path.full_path == root_path
        assert fil_path.full_path == file_path
        assert fol_path.full_path == folder_path

    @pytest.mark.asyncio
    async def test_validate_v1_path_generation(self, default_project_provider):
        root_path = '/'
        file_path = '/one/two/three/four.abc'
        folder_path = '/one/two/three/'

        r_path = await default_project_provider.validate_v1_path(root_path)
        fil_path = await default_project_provider.validate_v1_path(file_path)
        fol_path = await default_project_provider.validate_v1_path(folder_path)

        assert r_path.is_root
        assert fil_path.is_file
        assert fol_path.is_dir
        assert r_path.full_path == root_path 
        assert fil_path.full_path == file_path
        assert fol_path.full_path == folder_path
