import re

import pytest

from waterbutler.providers.github.metadata import GitHubFileTreeMetadata
from waterbutler.providers.github.metadata import GitHubFolderTreeMetadata
from waterbutler.providers.github.metadata import GitHubFileContentMetadata
from waterbutler.providers.github.metadata import GitHubFolderContentMetadata


@pytest.fixture
def file_metadata_content_endpoint():
    return {
        '_links': {
            'git': 'scrubbed',
            'html': 'scrubbed',
            'self': 'scrubbed',
        },
        'download_url': 'scrubbed',
        'git_url': 'scrubbed',
        'html_url': 'scrubbed',
        'name': 'epsilon',
        'path': 'epsilon',
        'sha': 'bd4fb614678f544acb22bac6861a21108f1e5d10',
        'size': 15,
        'type': 'file',
        'url': 'scrubbed',
    }

@pytest.fixture
def file_metadata_tree_endpoint():
    return {
        'mode': '100644',
        'path': 'README.md',
        'sha': 'd863d70539aa9fcb6b44b057221706f2ab18e341',
        'size': 38,
        'type': 'blob',
        'url': 'scrubbed',
    }

@pytest.fixture
def folder_metadata_content_endpoint():
    return {
        '_links': {
            'git': 'scrubbed',
            'html': 'scrubbed',
            'self': 'scrubbed'
        },
        'download_url': None,
        'git_url': 'scrubbed',
        'html_url': 'scrubbed',
        'name': 'manyfiles',
        'path': 'manyfiles',
        'sha': '4a16a05d8be24814483feeb9fa7f0baa37004cc3',
        'size': 0,
        'type': 'dir',
        'url': 'scrubbed'
    }

@pytest.fixture
def folder_metadata_tree_endpoint():
    return {
        'mode': '040000',
        'path': 'foldera/folderb/lorch',
        'sha': 'd564d0bc3dd917926892c55e3706cc116d5b165e',
        'type': 'tree',
        'url': 'scrubbed'
    }


class TestGitHubMetadata:

    def test_build_file_metadata_from_tree(self, file_metadata_tree_endpoint):
        try:
            metadata = GitHubFileTreeMetadata(file_metadata_tree_endpoint)
        except Exception as exc:
            pytest.fail(str(exc))

        assert metadata.name == 'README.md'
        assert metadata.path == '/README.md'
        assert metadata.modified is None
        assert metadata.content_type is None
        assert metadata.size == 38
        assert metadata.etag == '/README.md::d863d70539aa9fcb6b44b057221706f2ab18e341'
        assert metadata.extra == {
            'fileSha': 'd863d70539aa9fcb6b44b057221706f2ab18e341',
            'webView': None,
        }
        assert metadata.provider == 'github'

        assert metadata.commit is None
        assert metadata.ref is None
        assert metadata.web_view is None

    def test_build_file_metadata_from_contents(self, file_metadata_content_endpoint):
        try:
            metadata = GitHubFileContentMetadata(file_metadata_content_endpoint)
        except Exception as exc:
            pytest.fail(str(exc))

        assert metadata.name == 'epsilon'
        assert metadata.path == '/epsilon'
        assert metadata.modified is None
        assert metadata.content_type is None
        assert metadata.size == 15
        assert metadata.etag == '/epsilon::bd4fb614678f544acb22bac6861a21108f1e5d10'
        assert metadata.extra == {
            'fileSha': 'bd4fb614678f544acb22bac6861a21108f1e5d10',
            'webView': None,
        }
        assert metadata.provider == 'github'

        assert metadata.commit is None
        assert metadata.ref is None
        assert metadata.web_view is None

    def test_build_folder_metadata_from_tree(self, folder_metadata_tree_endpoint):
        try:
            metadata = GitHubFolderTreeMetadata(folder_metadata_tree_endpoint)
        except Exception as exc:
            pytest.fail(str(exc))

        assert metadata.name == 'lorch'
        assert metadata.path == '/foldera/folderb/lorch/'
        assert metadata.extra == {}
        assert metadata.provider == 'github'

        assert metadata.commit is None
        assert metadata.ref is None

    def test_build_folder_metadata_from_content(self, folder_metadata_content_endpoint):
        try:
            metadata = GitHubFolderContentMetadata(folder_metadata_content_endpoint)
        except Exception as exc:
            pytest.fail(str(exc))

        assert metadata.name == 'manyfiles'
        assert metadata.path == '/manyfiles/'
        assert metadata.extra == {}
        assert metadata.provider == 'github'

        assert metadata.commit is None
        assert metadata.ref is None

    def test_file_metadata_with_ref(self, file_metadata_tree_endpoint):
        try:
            metadata = GitHubFileTreeMetadata(file_metadata_tree_endpoint, ref="some-branch")
        except Exception as exc:
            pytest.fail(str(exc))

        assert metadata.name == 'README.md'
        assert metadata.path == '/README.md'
        assert metadata.modified is None
        assert metadata.content_type is None
        assert metadata.size == 38
        assert metadata.etag == '/README.md::d863d70539aa9fcb6b44b057221706f2ab18e341'
        assert metadata.extra == {
            'fileSha': 'd863d70539aa9fcb6b44b057221706f2ab18e341',
            'webView': None,
            'ref': 'some-branch',
        }
        assert metadata.provider == 'github'

        assert metadata.commit is None
        assert metadata.ref == 'some-branch'
        assert metadata.web_view is None

        json_api = metadata.json_api_serialized('mst3k')
        for actions, link in json_api['links'].items():
            assert re.search('[?&]ref=some-branch', link)
