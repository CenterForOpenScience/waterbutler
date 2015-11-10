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
def empty_project_settings():
    return {
        'bucket': fixtures.empty_project_id 
    }



@pytest.fixture
def project_provider(auth, credentials, empty_project_settings):
    return provider.ShareLatexProvider(auth, credentials, empty_project_settings)


@pytest.fixture
def metadata_empty_project():
    return fixtures.empty_project


class TestMetadata:

    @async
    @pytest.mark.aiohttpretty
    def test_project_with_no_contents(self, project_provider, metadata_empty_project):
        project_url = project_provider.build_url('project', project_provider.project_id)
        aiohttpretty.register_json_uri('GET', project_url, body=metadata_empty_project)
        expected = metadata_empty_project

        path = yield from project_provider.validate_path(project_url)
        result = yield from project_provider.metadata(path)

        assert aiohttpretty.has_call(method='GET', uri=project_url)
        assert result == expected
