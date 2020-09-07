import pytest
import multidict

from waterbutler.core.path import WaterButlerPath
from waterbutler.providers.cloudfiles.metadata import (CloudFilesFileMetadata,
                                                       CloudFilesHeaderMetadata,
                                                       CloudFilesFolderMetadata, )


@pytest.fixture
def file_header_metadata_txt():
    return multidict.CIMultiDict([
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
def file_metadata():
    return {
        'last_modified': '2014-12-19T23:22:14.728640',
        'content_type': 'application/x-www-form-urlencoded;charset=utf-8',
        'hash': 'edfa12d00b779b4b37b81fe5b61b2b3f',
        'name': 'similar.file',
        'bytes': 190
    }


class TestCloudfilesMetadata:

    def test_header_metadata(self, file_header_metadata_txt):

        path = WaterButlerPath('/file.txt')
        data = CloudFilesHeaderMetadata(file_header_metadata_txt, path.path)
        assert data.name == 'file.txt'
        assert data.path == '/file.txt'
        assert data.provider == 'cloudfiles'
        assert data.size == 216945
        assert type(data.size_as_int) == int
        assert data.size_as_int == 216945
        assert data.modified == 'Mon, 22 Dec 2014 19:01:02 GMT'
        assert data.created_utc is None
        assert data.content_type == 'text/plain'
        assert data.etag == '44325d4f13b09f3769ede09d7c20a82c'

        assert data.kind == 'file'
        assert data.modified_utc == '2014-12-22T19:01:02+00:00'
        assert data.extra == {
            'hashes': {
                'md5': '44325d4f13b09f3769ede09d7c20a82c'
            }
        }

        assert data.serialized() == {
            'extra': {
                'hashes': {'md5': '44325d4f13b09f3769ede09d7c20a82c'}},
            'kind': 'file',
            'name': 'file.txt',
            'path': '/file.txt',
            'provider': 'cloudfiles',
            'materialized': '/file.txt',
            'etag': 'b63e337b97c11034ab9db4635143754b807ad6f06b8bcdda06867096ab90b6fa',
            'contentType': 'text/plain',
            'modified': 'Mon, 22 Dec 2014 19:01:02 GMT',
            'modified_utc': '2014-12-22T19:01:02+00:00',
            'created_utc': None,
            'size': 216945,
            'sizeInt': 216945,
        }

        assert data.json_api_serialized('cn42d') == {
            'id': 'cloudfiles/file.txt',
            'type': 'files',
            'attributes': {
                'extra': {
                    'hashes': {
                        'md5': '44325d4f13b09f3769ede09d7c20a82c'
                    }
                },
                'kind': 'file',
                'name': 'file.txt',
                'path': '/file.txt',
                'provider': 'cloudfiles',
                'materialized': '/file.txt',
                'etag': 'b63e337b97c11034ab9db4635143754b807ad6f06b8bcdda06867096ab90b6fa',
                'contentType': 'text/plain',
                'modified': 'Mon, 22 Dec 2014 19:01:02 GMT',
                'modified_utc': '2014-12-22T19:01:02+00:00',
                'created_utc': None,
                'size': 216945,
                'sizeInt': 216945,
                'resource': 'cn42d'
            },
            'links': {
                'move': 'http://localhost:7777/v1/resources/cn42d/providers/cloudfiles/file.txt',
                'upload': ('http://localhost:7777/v1/'
                    'resources/cn42d/providers/cloudfiles/file.txt?kind=file'),
                'delete': 'http://localhost:7777/v1/resources/cn42d/providers/cloudfiles/file.txt',
                'download': 'http://localhost:7777/v1/resources/cn42d/providers/cloudfiles/file.txt'
            }
        }

        assert data._json_api_links('cn42d') == {
            'move': 'http://localhost:7777/v1/resources/cn42d/providers/cloudfiles/file.txt',
            'upload': ('http://localhost:7777/v1/resources/'
                'cn42d/providers/cloudfiles/file.txt?kind=file'),
            'delete': 'http://localhost:7777/v1/resources/cn42d/providers/cloudfiles/file.txt',
            'download': 'http://localhost:7777/v1/resources/cn42d/providers/cloudfiles/file.txt'
        }

    def test_file_metadata(self, file_metadata):
        data = CloudFilesFileMetadata(file_metadata)
        assert data.name == 'similar.file'
        assert data.provider == 'cloudfiles'
        assert data.path == '/similar.file'
        assert data.size == 190
        assert data.size_as_int == 190
        assert type(data.size_as_int) == int
        assert data.modified == '2014-12-19T23:22:14.728640'
        assert data.created_utc is None
        assert data.content_type == 'application/x-www-form-urlencoded;charset=utf-8'
        assert data.etag == 'edfa12d00b779b4b37b81fe5b61b2b3f'
        assert data.kind == 'file'
        assert data.modified_utc == '2014-12-19T23:22:14+00:00'
        assert data.extra == {
            'hashes': {
                'md5': 'edfa12d00b779b4b37b81fe5b61b2b3f'
            }
        }

        assert data.serialized() == {
            'extra': {
                'hashes': {'md5': 'edfa12d00b779b4b37b81fe5b61b2b3f'}},
            'kind': 'file',
            'name': 'similar.file',
            'path': '/similar.file',
            'provider': 'cloudfiles',
            'materialized': '/similar.file',
            'etag': '6ec17a9d5ecaf7f61ed46fa983361d2317fce07d36b40739b2f7529a1c0f47e0',
            'contentType': 'application/x-www-form-urlencoded;charset=utf-8',
            'modified': '2014-12-19T23:22:14.728640',
            'modified_utc': '2014-12-19T23:22:14+00:00',
            'created_utc': None,
            'size': 190,
            'sizeInt': 190,
        }

        assert data.json_api_serialized('cn42d') == {
            'id': 'cloudfiles/similar.file',
            'type': 'files',
            'attributes': {
                'extra': {
                    'hashes': {
                        'md5': 'edfa12d00b779b4b37b81fe5b61b2b3f'
                    }
                },
                'kind': 'file',
                'name': 'similar.file',
                'path': '/similar.file',
                'provider': 'cloudfiles',
                'materialized': '/similar.file',
                'etag': '6ec17a9d5ecaf7f61ed46fa983361d2317fce07d36b40739b2f7529a1c0f47e0',
                'contentType': 'application/x-www-form-urlencoded;charset=utf-8',
                'modified': '2014-12-19T23:22:14.728640',
                'modified_utc': '2014-12-19T23:22:14+00:00',
                'created_utc': None,
                'size': 190,
                'sizeInt': 190,
                'resource': 'cn42d'
            },
            'links': {
                'move': ('http://localhost:7777/v1/resources/cn42d'
                    '/providers/cloudfiles/similar.file'),
                'upload': ('http://localhost:7777/v1/resources/cn42d'
                    '/providers/cloudfiles/similar.file?kind=file'),
                'delete': ('http://localhost:7777/v1/resources/cn42d'
                    '/providers/cloudfiles/similar.file'),
                'download': ('http://localhost:7777/v1/resources/cn42d'
                    '/providers/cloudfiles/similar.file')
            }
        }

        assert data._json_api_links('cn42d') == {
            'move': 'http://localhost:7777/v1/resources/cn42d/providers/cloudfiles/similar.file',
            'upload': ('http://localhost:7777/v1/resources/'
                'cn42d/providers/cloudfiles/similar.file?kind=file'),
            'delete': 'http://localhost:7777/v1/resources/cn42d/providers/cloudfiles/similar.file',
            'download': 'http://localhost:7777/v1/resources/cn42d/providers/cloudfiles/similar.file'
        }

    def test_folder_metadata(self):
        data = CloudFilesFolderMetadata({'subdir': 'level1/'})

        assert data.name == 'level1'
        assert data.path == '/level1/'
        assert data.provider == 'cloudfiles'

        assert data.build_path('') == '/'
        assert data.materialized_path == '/level1/'
        assert data.is_folder is True
        assert data.children is None
        assert data.kind == 'folder'
        assert data.etag is None
        assert data.serialized() == {
            'extra': {},
            'kind': 'folder',
            'name': 'level1',
            'path': '/level1/',
            'provider': 'cloudfiles',
            'materialized': '/level1/',
            'etag': '69cf764abe6f2e90dc81fb4218e15f202f9b99bcd1963cf2d5f011629d6f0d8a'
        }

        assert data.json_api_serialized('cn42d') == {
            'id': 'cloudfiles/level1/',
            'type': 'files',
            'attributes': {
                'extra': {},
                'kind': 'folder',
                'name': 'level1',
                'path': '/level1/',
                'provider': 'cloudfiles',
                'materialized': '/level1/',
                'etag': '69cf764abe6f2e90dc81fb4218e15f202f9b99bcd1963cf2d5f011629d6f0d8a',
                'resource': 'cn42d',
                'size': None,
                'sizeInt': None
            },
            'links': {
                'move': 'http://localhost:7777/v1/resources/cn42d/providers/cloudfiles/level1/',
                'upload': ('http://localhost:7777/v1/resources/'
                    'cn42d/providers/cloudfiles/level1/?kind=file'),
                'delete': 'http://localhost:7777/v1/resources/cn42d/providers/cloudfiles/level1/',
                'new_folder': ('http://localhost:7777/v1/resources/'
                    'cn42d/providers/cloudfiles/level1/?kind=folder')
            }
        }

        assert data._json_api_links('cn42d') == {
            'move': 'http://localhost:7777/v1/resources/cn42d/providers/cloudfiles/level1/',
            'upload': ('http://localhost:7777/v1/resources/'
                'cn42d/providers/cloudfiles/level1/?kind=file'),
            'delete': 'http://localhost:7777/v1/resources/cn42d/providers/cloudfiles/level1/',
            'new_folder': ('http://localhost:7777/v1/resources/'
                'cn42d/providers/cloudfiles/level1/?kind=folder')
        }
