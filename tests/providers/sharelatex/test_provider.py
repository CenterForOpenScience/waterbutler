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
        'bucket': 'to define',
    }

@pytest.fixture
def empty_project_settings():
    return {
        'bucket': fixtures.empty_project_id 
    }


@pytest.fixture
def default_provider(auth, credentials, settings):
    return provider.ShareLatexProvider(auth, credentials, settings)

@pytest.fixture
def empty_project_provider(auth, credentials, empty_project_settings):
    return provider.ShareLatexProvider(auth, credentials, empty_project_settings)


@pytest.fixture
def default_metadata():
    return {}

@pytest.fixture
def metadata_empty_project():
    return fixtures.empty_project_metadata


class TestMetadata:


    def check_metadata_is_folder_with_path_and_name(self, metadata, path, name):
        assert isinstance(metadata, metadata.BaseMetadata)
        assert metadata.kind == 'folder'
        assert metadata.provider == 'sharelatex'
        assert metadata.path == path
        assert metadata.name == nam


    @async
    @pytest.mark.aiohttpretty
    def test_metadata_folder_root_with_no_contents(self, empty_project_provider, metadata_empty_project):
        path = yield from empty_project_provider.validate_path('/')
        url = empty_project_provider.build_url('project', empty_project_provider.project_id)
        aiohttpretty.register_json_uri('GET', url, body=metadata_empty_project)

        result = yield from empty_project_provider.metadata(path)

        assert aiohttpretty.has_call(method='GET', uri=url)
        check_metadata_is_folder_with_path_and_name(result, '/', fixtures.empty_project_name)
