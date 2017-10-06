import os
import json

import pytest

EMPTY_MD5 = 'd41d8cd98f00b204e9800998ecf8427e'


@pytest.fixture
def root_provider_fixtures():
    """Due to a bug in aiohttpretty, the file stream is not being read from on file upload for the
    Figshare provider.  Because the file stream isn't read, the stream hash calculator never gets
    any data, and the computed md5sum is always that of the empty string.  To work around this, the
    fixtures currently include the empty md5 in the metadata.  Once aiohttpretty is fixed, the
    metadata can be reverted to deliver the actual content hash."""
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/root_provider.json'), 'r') as fp:
        data = json.load(fp)
        # This is the workaround for the bug with aiohttpretty
        data['get_upload_metadata']['md5'] = EMPTY_MD5
        return data


@pytest.fixture
def crud_fixtures():
    """Due to a bug in aiohttpretty, the file stream is not being read from on file upload for the
    Figshare provider.  Because the file stream isn't read, the stream hash calculator never gets
    any data, and the computed md5sum is always that of the empty string.  To work around this, the
    fixtures currently include the empty md5 in the metadata.  Once aiohttpretty is fixed, the
    metadata can be reverted to deliver the actual content hash."""
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/crud.json'), 'r') as fp:
        data = json.load(fp)

        # This is the workaround for the bug with aiohttpretty
        data['upload_article_metadata']['files'][0]['supplied_md5'] = EMPTY_MD5
        data['upload_article_metadata']['files'][0]['computed_md5'] = EMPTY_MD5
        data['upload_folder_article_metadata']['files'][0]['supplied_md5'] = EMPTY_MD5
        data['upload_folder_article_metadata']['files'][0]['computed_md5'] = EMPTY_MD5
        return data


@pytest.fixture
def error_fixtures():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/errors.json'), 'r') as fp:
        return json.load(fp)
