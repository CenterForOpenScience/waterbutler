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
def default_provider(auth, credentials, settings):
    return provider.ShareLatexProvider(auth, credentials, settings)

@pytest.fixture
def empty_project_provider(auth, credentials, empty_project_settings):
    return provider.ShareLatexProvider(auth, credentials, empty_project_settings)

@pytest.fixture
def metadata_empty_project():
    return fixtures.empty_project_metadata


class TestMetadata:


    def check_metadata_is_folder_with_path_and_name(self, metadata, path, name):
        assert isinstance(metadata, metadata.BaseMetadata)
        assert metadata.kind == 'folder'
        assert metadata.provider == 'sharelatex'
        assert metadata.path == path
        assert metadata.name == name


    @async
    @pytest.mark.aiohttpretty
    def test_metadata_folder_root_with_no_contents(self, empty_project_provider, metadata_empty_project):
        root_folder_path = yield from empty_project_provider.validate_path('/')
        root_folder_url = empty_project_provider.build_url('project', empty_project_provider.project_id, 'docs')
        aiohttpretty.register_json_uri('GET', root_folder_url, body=metadata_empty_project)

        with pytest.raises(exceptions.NotFoundError) as e:
            yield from empty_project_provider.metadata(root_folder_path)
