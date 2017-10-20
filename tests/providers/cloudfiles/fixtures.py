import os
import io
import time
import json
from unittest import mock

import pytest
import aiohttp
import aiohttpretty

from waterbutler.core import streams
from waterbutler.providers.cloudfiles import CloudFilesProvider



@pytest.fixture
def auth():
    return {
        'name': 'cat',
        'email': 'cat@cat.com',
    }


@pytest.fixture
def credentials():
    return {
        'username': 'prince',
        'token': 'revolutionary',
        'region': 'iad',
    }


@pytest.fixture
def settings():
    return {'container': 'purple rain'}


@pytest.fixture
def provider(auth, credentials, settings):
    return CloudFilesProvider(auth, credentials, settings)



@pytest.fixture
def token(auth_json):
    return auth_json['access']['token']['id']


@pytest.fixture
def endpoint(auth_json):
    return auth_json['access']['serviceCatalog'][0]['endpoints'][0]['publicURL']


@pytest.fixture
def temp_url_key():
    return 'temporary beret'


@pytest.fixture
def mock_auth(auth_json):
    aiohttpretty.register_json_uri(
        'POST',
        settings.AUTH_URL,
        body=auth_json,
    )


@pytest.fixture
def mock_temp_key(endpoint, temp_url_key):
    aiohttpretty.register_uri(
        'HEAD',
        endpoint,
        status=204,
        headers={'X-Account-Meta-Temp-URL-Key': temp_url_key},
    )


@pytest.fixture
def mock_time(monkeypatch):
    mock_time = mock.Mock()
    mock_time.return_value = 10
    monkeypatch.setattr(time, 'time', mock_time)


@pytest.fixture
def connected_provider(provider, token, endpoint, temp_url_key, mock_time):
    provider.token = token
    provider.endpoint = endpoint
    provider.temp_url_key = temp_url_key.encode()
    return provider


@pytest.fixture
def file_content():
    return b'sleepy'


@pytest.fixture
def file_like(file_content):
    return io.BytesIO(file_content)


@pytest.fixture
def file_stream(file_like):
    return streams.FileStreamReader(file_like)



@pytest.fixture
def folder_root_empty():
    return []

@pytest.fixture
def container_header_metadata_with_verision_location():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/fixtures.json'), 'r') as fp:
        return json.load(fp)['container_header_metadata_with_verision_location']


@pytest.fixture
def container_header_metadata_without_verision_location():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/fixtures.json'), 'r') as fp:
        return json.load(fp)['container_header_metadata_without_verision_location']

@pytest.fixture
def file_metadata():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/fixtures.json'), 'r') as fp:
        return json.load(fp)['file_metadata']

@pytest.fixture
def folder_root():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/fixtures.json'), 'r') as fp:
        return json.load(fp)['folder_root']


@pytest.fixture
def folder_root_level1_level2():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/fixtures.json'), 'r') as fp:
        return json.load(fp)['folder_root_level1_level2']


@pytest.fixture
def folder_root_level1():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/fixtures.json'), 'r') as fp:
        return json.load(fp)['folder_root_level1']


@pytest.fixture
def file_header_metadata():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/fixtures.json'), 'r') as fp:
        return json.load(fp)['file_header_metadata']


@pytest.fixture
def auth_json():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/fixtures.json'), 'r') as fp:
        return json.load(fp)['auth_json']


@pytest.fixture
def folder_root():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/fixtures.json'), 'r') as fp:
        return json.load(fp)['folder_root']

@pytest.fixture
def revision_list():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/fixtures.json'), 'r') as fp:
        return json.load(fp)['revision_list']

@pytest.fixture
def file_root_level1_level2_file2_txt():
    return aiohttp.multidict.CIMultiDict([
        ('ORIGIN', 'https://mycloud.rackspace.com'),
        ('CONTENT-LENGTH', '216945'),
        ('ACCEPT-RANGES', 'bytes'),
        ('LAST-MODIFIED', 'Mon, 22 Dec 2014 19:01:02 GMT'),
        ('ETAG', '44325d4f13b09f3769ede09d7c20a82c'),
        ('X-TIMESTAMP', '1419274861.04433'),
        ('CONTENT-TYPE', 'text/plain'),
        ('X-TRANS-ID', 'tx836375d817a34b558756a-0054987deeiad3'),
        ('DATE', 'Mon, 22 Dec 2014 20:24:14 GMT')
    ])


@pytest.fixture
def folder_root_level1_empty():
    return aiohttp.multidict.CIMultiDict([
        ('ORIGIN', 'https://mycloud.rackspace.com'),
        ('CONTENT-LENGTH', '0'),
        ('ACCEPT-RANGES', 'bytes'),
        ('LAST-MODIFIED', 'Mon, 22 Dec 2014 18:58:56 GMT'),
        ('ETAG', 'd41d8cd98f00b204e9800998ecf8427e'),
        ('X-TIMESTAMP', '1419274735.03160'),
        ('CONTENT-TYPE', 'application/directory'),
        ('X-TRANS-ID', 'txd78273e328fc4ba3a98e3-0054987eeeiad3'),
        ('DATE', 'Mon, 22 Dec 2014 20:28:30 GMT')
    ])


@pytest.fixture
def file_root_similar():
    return aiohttp.multidict.CIMultiDict([
        ('ORIGIN', 'https://mycloud.rackspace.com'),
        ('CONTENT-LENGTH', '190'),
        ('ACCEPT-RANGES', 'bytes'),
        ('LAST-MODIFIED', 'Fri, 19 Dec 2014 23:22:24 GMT'),
        ('ETAG', 'edfa12d00b779b4b37b81fe5b61b2b3f'),
        ('X-TIMESTAMP', '1419031343.23224'),
        ('CONTENT-TYPE', 'application/x-www-form-urlencoded;charset=utf-8'),
        ('X-TRANS-ID', 'tx7cfeef941f244807aec37-005498754diad3'),
        ('DATE', 'Mon, 22 Dec 2014 19:47:25 GMT')
    ])


@pytest.fixture
def file_root_similar_name():
    return aiohttp.multidict.CIMultiDict([
        ('ORIGIN', 'https://mycloud.rackspace.com'),
        ('CONTENT-LENGTH', '190'),
        ('ACCEPT-RANGES', 'bytes'),
        ('LAST-MODIFIED', 'Mon, 22 Dec 2014 19:07:12 GMT'),
        ('ETAG', 'edfa12d00b779b4b37b81fe5b61b2b3f'),
        ('X-TIMESTAMP', '1419275231.66160'),
        ('CONTENT-TYPE', 'application/x-www-form-urlencoded;charset=utf-8'),
        ('X-TRANS-ID', 'tx438cbb32b5344d63b267c-0054987f3biad3'),
        ('DATE', 'Mon, 22 Dec 2014 20:29:47 GMT')
    ])



@pytest.fixture
def file_header_metadata_txt():
    return aiohttp.multidict.CIMultiDict([
        ('ORIGIN', 'https://mycloud.rackspace.com'),
        ('CONTENT-LENGTH', '216945'),
        ('ACCEPT-RANGES', 'bytes'),
        ('LAST-MODIFIED', 'Mon, 22 Dec 2014 19:01:02 GMT'),
        ('ETAG', '44325d4f13b09f3769ede09d7c20a82c'),
        ('X-TIMESTAMP', '1419274861.04433'),
        ('CONTENT-TYPE', 'text/plain'),
        ('X-TRANS-ID', 'tx836375d817a34b558756a-0054987deeiad3'),
        ('DATE', 'Mon, 22 Dec 2014 20:24:14 GMT')
    ])
