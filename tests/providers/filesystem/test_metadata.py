import pytest
import os

from waterbutler.providers.filesystem.metadata import FileSystemFolderMetadata
from waterbutler.providers.filesystem.metadata import FileSystemFileMetadata


@pytest.fixture
def file_metadata():
    return {
        'path': '/code/website/osfstoragecache/77094244-aa24-48da-9437-d8ce6f7a94e9',
        'modified_utc': '2017-09-20T15:16:02.601916+00:00',
        'mime_type': None,
        'size': 35981,
        'modified': 'Wed, 20 Sep 2017 15:16:02 +0000'
    }


@pytest.fixture
def folder_metadata():
    return {
        'path': os.path.join('/', 'folder1/')
    }


class TestMetadata:

    def test_file_metadata(self, file_metadata):
        data = FileSystemFileMetadata(file_metadata, '/')
        assert data.path == '/code/website/osfstoragecache/77094244-aa24-48da-9437-d8ce6f7a94e9'
        assert data.provider == 'filesystem'
        assert data.modified == 'Wed, 20 Sep 2017 15:16:02 +0000'
        assert data.modified_utc == '2017-09-20T15:16:02.601916+00:00'
        assert data.created_utc is None
        assert data.content_type is None
        assert data.name == '77094244-aa24-48da-9437-d8ce6f7a94e9'
        assert data.size == 35981
        assert data.etag == ('Wed, 20 Sep 2017 15:16:02 +0000::/'
            'code/website/osfstoragecache/77094244-aa24-48da-9437-d8ce6f7a94e9')
        assert data.kind == 'file'
        assert data.modified_utc == '2017-09-20T15:16:02.601916+00:00'
        assert data.extra == {}
        assert data.serialized() == {
            'extra': {},
            'kind': 'file',
            'name': '77094244-aa24-48da-9437-d8ce6f7a94e9',
            'path': '/code/website/osfstoragecache/77094244-aa24-48da-9437-d8ce6f7a94e9',
            'provider': 'filesystem',
            'materialized': '/code/website/osfstoragecache/77094244-aa24-48da-9437-d8ce6f7a94e9',
            'etag': '1cbfe0429bd8cd51d517ecfa22f92d28c86a0c687334c1f5acf70cdde75526fd',
            'contentType': None,
            'modified': 'Wed, 20 Sep 2017 15:16:02 +0000',
            'modified_utc': '2017-09-20T15:16:02.601916+00:00',
            'created_utc': None,
            'size': 35981
        }

        assert data.json_api_serialized('cn42d') == {
            'id': 'filesystem/code/website/osfstoragecache/77094244-aa24-48da-9437-d8ce6f7a94e9',
            'type': 'files',
            'attributes': {
                'extra': {},
                'kind': 'file',
                'name': '77094244-aa24-48da-9437-d8ce6f7a94e9',
                'path': '/code/website/osfstoragecache/77094244-aa24-48da-9437-d8ce6f7a94e9',
                'provider': 'filesystem',
                'materialized': ('/code/website/osfstoragecache/'
                    '77094244-aa24-48da-9437-d8ce6f7a94e9'),
                'etag': '1cbfe0429bd8cd51d517ecfa22f92d28c86a0c687334c1f5acf70cdde75526fd',
                'contentType': None,
                'modified': 'Wed, 20 Sep 2017 15:16:02 +0000',
                'modified_utc': '2017-09-20T15:16:02.601916+00:00',
                'created_utc': None,
                'size': 35981,
                'resource': 'cn42d'
            },
            'links': {
                'move': ('http://localhost:7777/v1/resources/cn42d/providers/filesystem'
                    '/code/website/osfstoragecache/77094244-aa24-48da-9437-d8ce6f7a94e9'),
                'upload': ('http://localhost:7777/v1/resources/cn42d/providers/filesystem'
                    '/code/website/osfstoragecache/77094244-aa24-48da-9437-d8ce6f7a94e9?kind=file'),
                'delete': ('http://localhost:7777/v1/resources/cn42d/providers/filesystem'
                    '/code/website/osfstoragecache/77094244-aa24-48da-9437-d8ce6f7a94e9'),
                'download': ('http://localhost:7777/v1/resources/cn42d/providers/filesystem'
                    '/code/website/osfstoragecache/77094244-aa24-48da-9437-d8ce6f7a94e9')
            }
        }

        assert data._json_api_links('cn42d') == {
            'move': ('http://localhost:7777/v1/resources/cn42d/providers/'
                'filesystem/code/website/osfstoragecache/77094244-aa24-48da-9437-d8ce6f7a94e9'),
            'upload': ('http://localhost:7777/v1/resources/cn42d/providers/filesystem'
                '/code/website/osfstoragecache/77094244-aa24-48da-9437-d8ce6f7a94e9?kind=file'),
            'delete': ('http://localhost:7777/v1/resources/cn42d/providers/'
                'filesystem/code/website/osfstoragecache/77094244-aa24-48da-9437-d8ce6f7a94e9'),
            'download': ('http://localhost:7777/v1/resources/cn42d/providers/'
                'filesystem/code/website/osfstoragecache/77094244-aa24-48da-9437-d8ce6f7a94e9')
        }

    def test_folder_metadata(self, folder_metadata):
        data = FileSystemFolderMetadata(folder_metadata, '/')
        assert data.path == '/folder1/'
        assert data.name == ''
        assert data.provider == 'filesystem'
        assert data.build_path('') == '/'
        assert data.materialized_path == '/folder1/'
        assert data.is_folder is True
        assert data.children is None
        assert data.kind == 'folder'
        assert data.etag is None
        assert data.serialized() == {
            'extra': {},
            'kind': 'folder',
            'name': '',
            'path': '/folder1/',
            'provider': 'filesystem',
            'materialized': '/folder1/',
            'etag': '6a2b72b88f67692ff6f4cc3a52798cdc54a6e7c7e6dcbf8463fcb5105b6b949e'
        }

        assert data.json_api_serialized('7ycmyr') == {
            'id': 'filesystem/folder1/',
            'type': 'files',
            'attributes': {
                'extra': {},
                'kind': 'folder',
                'name': '',
                'path': '/folder1/',
                'provider': 'filesystem',
                'materialized': '/folder1/',
                'etag': '6a2b72b88f67692ff6f4cc3a52798cdc54a6e7c7e6dcbf8463fcb5105b6b949e',
                'resource': '7ycmyr',
                'size': None
            },
            'links': {
                'move': 'http://localhost:7777/v1/resources/7ycmyr/providers/filesystem/folder1/',
                'upload': ('http://localhost:7777/v1/resources/'
                    '7ycmyr/providers/filesystem/folder1/?kind=file'),
                'delete': 'http://localhost:7777/v1/resources/7ycmyr/providers/filesystem/folder1/',
                'new_folder': ('http://localhost:7777/v1/resources/'
                    '7ycmyr/providers/filesystem/folder1/?kind=folder')
            }
        }

        assert data._json_api_links('cn42d') == {
            'move': 'http://localhost:7777/v1/resources/cn42d/providers/filesystem/folder1/',
            'upload': ('http://localhost:7777/v1/resources/'
                'cn42d/providers/filesystem/folder1/?kind=file'),
            'delete': 'http://localhost:7777/v1/resources/cn42d/providers/filesystem/folder1/',
            'new_folder': ('http://localhost:7777/v1/resources/'
                'cn42d/providers/filesystem/folder1/?kind=folder')
        }
