import pytest

from tests.utils import async

import io
import json
import random

import aiohttpretty

from waterbutler.core import streams
from waterbutler.core import exceptions

from waterbutler.providers.sharelatex import metadata
from waterbutler.providers.sharelatex import provider

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
        'access_key': 'brian',
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

    def contain_file_with_type(self, items, t):
        result = []
        for f in self.only_files(items):
            path = str(f.path)
            if path.find(t) != -1:
                result.append(f)
        return result

    def check_metadata_is_folder_with_path_and_name(self, metadata, path):
        assert metadata[0].kind == 'file'
        assert metadata[0].provider == 'sharelatex'
        assert metadata[0].path == '/projetoprincipal.tex'
        assert metadata[0].size == int('123')
        assert metadata[0].content_type == 'application/x-tex'
        # TODO: test size, mimetime, other files and folders.

    @async
    @pytest.mark.aiohttpretty
    def test_no_root_folder(self, empty_project_provider, empty_metadata):
        root_folder_path = yield from empty_project_provider.validate_path('/')
        root_folder_url = empty_project_provider.build_url('project', empty_project_provider.project_id, 'docs')
        aiohttpretty.register_json_uri('GET', root_folder_url, body=empty_metadata)

        with pytest.raises(exceptions.NotFoundError) as e:
            yield from empty_project_provider.metadata(root_folder_path)

    @async
    @pytest.mark.aiohttpretty
    def test_metadata_not_found(self, default_project_provider, empty_metadata):
        path = yield from default_project_provider.validate_path('/')
        url = default_project_provider.build_url('project', default_project_provider.project_id, 'docs')
        aiohttpretty.register_json_uri('GET', url, status=404)

        with pytest.raises(exceptions.MetadataError) as e:
            yield from default_project_provider.metadata(path)

    @async
    @pytest.mark.aiohttpretty
    def test_root_folder_with_one_folder(self, default_project_provider, default_project_metadata):
        root_folder_path = yield from default_project_provider.validate_path('/')
        root_folder_url = default_project_provider.build_url('project', default_project_provider.project_id, 'docs')

        aiohttpretty.register_json_uri('GET', root_folder_url, body=default_project_metadata)

        result = yield from default_project_provider.metadata(root_folder_path)

        self.check_metadata_is_folder_with_path_and_name(result, root_folder_path)

    @async
    @pytest.mark.aiohttpretty
    def test_root_folder_without_folders(self, default_project_provider, only_files_metadata):
        root_folder_path = yield from default_project_provider.validate_path('/')
        root_folder_url = default_project_provider.build_url('project', default_project_provider.project_id, 'docs')

        aiohttpretty.register_json_uri('GET', root_folder_url, body=only_files_metadata)

        result = yield from default_project_provider.metadata(root_folder_path)
        for f in result:
            assert f.kind == 'file'

    @async
    @pytest.mark.aiohttpretty
    def test_root_folder_without_files(self, default_project_provider, only_folders_metadata):
        root_folder_path = yield from default_project_provider.validate_path('/')
        root_folder_url = default_project_provider.build_url('project', default_project_provider.project_id, 'docs')

        aiohttpretty.register_json_uri('GET', root_folder_url, body=only_folders_metadata)

        result = yield from default_project_provider.metadata(root_folder_path)
        for f in result:
            assert f.kind == 'folder'

    @async
    @pytest.mark.aiohttpretty
    def test_tex_metadata(self, default_project_provider, only_docs_metadata):
        path = yield from default_project_provider.validate_path('/')
        url = default_project_provider.build_url('project', default_project_provider.project_id, 'docs')

        aiohttpretty.register_json_uri('GET', url, body=only_docs_metadata)

        result = yield from default_project_provider.metadata(path)
        for f in result:
            p = str(f.path)
            assert f.content_type == 'application/x-tex'
            assert p.find('.tex') != -1

    @async
    @pytest.mark.aiohttpretty
    def test_other_files_metadata(self, default_project_provider, default_project_metadata):
        path = yield from default_project_provider.validate_path('/')
        url = default_project_provider.build_url('project', default_project_provider.project_id, 'docs')
        aiohttpretty.register_json_uri('GET', url, body=default_project_metadata)

        result = yield from default_project_provider.metadata(path)

        fonts = self.contain_file_with_type(result, 'otf')
        images = self.contain_file_with_type(result, 'jpg')
        files = self.only_files(result)

        for font in fonts:
            assert font.content_type == 'application/x-font-opentype'

        for image in images:
            assert image.content_type == 'application/jpeg'

        for f in files:
            assert f.kind == 'file'

        assert fonts
        assert images
        assert f

class TestCRUD:


    @async
    @pytest.mark.aiohttpretty
    def test_download_error(self, empty_project_provider):
        path = yield from empty_project_provider.validate_path('/')
        url = empty_project_provider.build_url('project', empty_project_provider.project_id, 'file', path.path)
        aiohttpretty.register_uri('GET', url, status=404)

        with pytest.raises(exceptions.DownloadError) as e:
            yield from empty_project_provider.download(path)

    @async
    @pytest.mark.aiohttpretty
    def test_download_any_content(self, default_project_provider):
        body = b'castle on a cloud'
        path = yield from default_project_provider.validate_path('/raw.txt')
        url = default_project_provider.build_url('project', default_project_provider.project_id, 'file', path.path)
        aiohttpretty.register_uri('GET', url, body=body)

        result = yield from default_project_provider.download(path)
        content = yield from result.read()

        assert content == body

    @async
    @pytest.mark.aiohttpretty
    def test_download_when_accept_url(self, default_project_provider):
        path = yield from default_project_provider.validate_path('/raw.txt')
        url = default_project_provider.build_url('project', default_project_provider.project_id, 'file', path.path)
        aiohttpretty.register_uri('GET', url)

        result = yield from default_project_provider.download(path, accept_url=True)
        assert result == url
