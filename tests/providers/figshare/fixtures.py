import os
import json

import pytest


@pytest.fixture
def root_provider_fixtures():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/root_provider.json'), 'r') as fp:
        return json.load(fp)


@pytest.fixture
def crud_fixtures():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/crud.json'), 'r') as fp:
        return json.load(fp)


@pytest.fixture
def error_fixtures():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/errors.json'), 'r') as fp:
        return json.load(fp)


@pytest.fixture
def project_article_type_1_metadata():
    with open(
        os.path.join(os.path.dirname(__file__), 'fixtures/project_article_type_1_metadata.json'),
        'r'
    ) as fp:
        return json.load(fp)


@pytest.fixture
def project_article_type_3_metadata():
    with open(
        os.path.join(os.path.dirname(__file__), 'fixtures/project_article_type_3_metadata.json'),
        'r'
    ) as fp:
        return json.load(fp)


@pytest.fixture
def project_list_articles():
    with open(
            os.path.join(os.path.dirname(__file__), 'fixtures/project_list_articles.json'),
            'r'
    ) as fp:
        return json.load(fp)


@pytest.fixture
def project_article_type_1_file_metadata():
    with open(
        os.path.join(
            os.path.dirname(__file__),
            'fixtures/project_article_type_1_file_metadata.json'
        ), 'r'
    ) as fp:
        return json.load(fp)


@pytest.fixture
def project_article_type_3_file_metadata():
    with open(
        os.path.join(
            os.path.dirname(__file__),
            'fixtures/project_article_type_3_file_metadata.json'
        ), 'r'
    ) as fp:
        return json.load(fp)
