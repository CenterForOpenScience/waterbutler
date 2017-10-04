import re

import pytest

from waterbutler.providers.github.metadata import GitHubFileTreeMetadata
from waterbutler.providers.github.metadata import GitHubFolderTreeMetadata
from waterbutler.providers.github.metadata import GitHubFileContentMetadata
from waterbutler.providers.github.metadata import GitHubFolderContentMetadata



from tests.providers.github.fixtures import metadata_fixtures


class TestGitHubMetadata:

    def test_build_file_metadata_from_tree(self, metadata_fixtures):
        try:
            metadata = GitHubFileTreeMetadata(metadata_fixtures['file_metadata_tree_endpoint'])
        except Exception as exc:
            pytest.fail(str(exc))

        assert metadata.name == 'README.md'
        assert metadata.path == '/README.md'
        assert metadata.modified is None
        assert metadata.content_type is None
        assert metadata.size == 38
        assert metadata.size_as_int == 38
        assert type(metadata.size_as_int) == int
        assert metadata.etag == '/README.md::d863d70539aa9fcb6b44b057221706f2ab18e341'
        assert metadata.extra == {
            'fileSha': 'd863d70539aa9fcb6b44b057221706f2ab18e341',
            'webView': None,
            'hashes': {
                'git': 'd863d70539aa9fcb6b44b057221706f2ab18e341',
            },
        }
        assert metadata.provider == 'github'

        assert metadata.commit is None
        assert metadata.ref is None
        assert metadata.web_view is None

    def test_build_file_metadata_from_contents(self, metadata_fixtures):
        try:
            metadata = GitHubFileContentMetadata(metadata_fixtures['file_metadata_content_endpoint'])
        except Exception as exc:
            pytest.fail(str(exc))

        assert metadata.name == 'epsilon'
        assert metadata.path == '/epsilon'
        assert metadata.modified is None
        assert metadata.content_type is None
        assert metadata.size == 15
        assert metadata.size_as_int == 15
        assert type(metadata.size_as_int) == int
        assert metadata.etag == '/epsilon::bd4fb614678f544acb22bac6861a21108f1e5d10'
        assert metadata.extra == {
            'fileSha': 'bd4fb614678f544acb22bac6861a21108f1e5d10',
            'webView': None,
            'hashes': {
                'git': 'bd4fb614678f544acb22bac6861a21108f1e5d10',
            },
        }
        assert metadata.provider == 'github'

        assert metadata.commit is None
        assert metadata.ref is None
        assert metadata.web_view is None

    def test_build_folder_metadata_from_tree(self, metadata_fixtures):
        try:
            metadata = GitHubFolderTreeMetadata(metadata_fixtures['folder_metadata_tree_endpoint'])
        except Exception as exc:
            pytest.fail(str(exc))

        assert metadata.name == 'lorch'
        assert metadata.path == '/foldera/folderb/lorch/'
        assert metadata.extra == {}
        assert metadata.provider == 'github'

        assert metadata.commit is None
        assert metadata.ref is None

    def test_build_folder_metadata_from_content(self, metadata_fixtures):
        try:
            metadata = GitHubFolderContentMetadata(metadata_fixtures['folder_metadata_content_endpoint'])
        except Exception as exc:
            pytest.fail(str(exc))

        assert metadata.name == 'manyfiles'
        assert metadata.path == '/manyfiles/'
        assert metadata.extra == {}
        assert metadata.provider == 'github'

        assert metadata.commit is None
        assert metadata.ref is None

    def test_file_metadata_with_ref(self, metadata_fixtures):
        try:
            metadata = GitHubFileTreeMetadata(metadata_fixtures['file_metadata_tree_endpoint'], ref="some-branch")
        except Exception as exc:
            pytest.fail(str(exc))

        assert metadata.name == 'README.md'
        assert metadata.path == '/README.md'
        assert metadata.modified is None
        assert metadata.content_type is None
        assert metadata.size == 38
        assert metadata.size_as_int == 38
        assert type(metadata.size_as_int) == int
        assert metadata.etag == '/README.md::d863d70539aa9fcb6b44b057221706f2ab18e341'
        assert metadata.extra == {
            'fileSha': 'd863d70539aa9fcb6b44b057221706f2ab18e341',
            'webView': None,
            'ref': 'some-branch',
            'hashes': {
                'git': 'd863d70539aa9fcb6b44b057221706f2ab18e341',
            },

        }
        assert metadata.provider == 'github'

        assert metadata.commit is None
        assert metadata.ref == 'some-branch'
        assert metadata.web_view is None

        json_api = metadata.json_api_serialized('mst3k')
        for actions, link in json_api['links'].items():
            assert re.search('[?&]ref=some-branch', link)
