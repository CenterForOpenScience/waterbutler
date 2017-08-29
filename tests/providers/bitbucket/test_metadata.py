import pytest

from waterbutler.providers.bitbucket.path import BitbucketPath
from waterbutler.providers.bitbucket.metadata import BitbucketFileMetadata
from waterbutler.providers.bitbucket.metadata import BitbucketFolderMetadata
from waterbutler.providers.bitbucket.metadata import BitbucketRevisionMetadata

from .fixtures import owner, repo, file_metadata, folder_metadata, revision_metadata

COMMIT_SHA = '123abc456def'


class TestBitbucketMetadata:

    def test_build_file_metadata(self, file_metadata, owner, repo):
        name = 'aaa-01-2.txt'
        subdir = 'plaster'
        full_path = '/{}/{}'.format(subdir, name)
        branch = 'master'

        path = BitbucketPath(full_path, _ids=[
            (COMMIT_SHA, branch), (COMMIT_SHA, branch), (COMMIT_SHA, branch)
        ])

        try:
            metadata = BitbucketFileMetadata(file_metadata, path, owner=owner, repo=repo)
        except Exception as exc:
            pytest.fail(str(exc))

        assert metadata.name == name
        assert metadata.path == full_path
        assert metadata.kind == 'file'
        assert metadata.modified == '2016-10-14T00:37:55Z'
        assert metadata.modified_utc == '2016-10-14T00:37:55+00:00'
        assert metadata.created_utc is None
        assert metadata.content_type is None
        assert metadata.size == 13
        assert metadata.etag == '{}::{}'.format(full_path,COMMIT_SHA)
        assert metadata.provider == 'bitbucket'
        assert metadata.last_commit_sha == '90c8f7eef948'
        assert metadata.commit_sha == COMMIT_SHA
        assert metadata.branch_name == branch

        web_view = ('https://bitbucket.org/{}/{}/src/{}{}?'
                    'fileviewer=file-view-default'.format(owner, repo, COMMIT_SHA, full_path))
        assert metadata.web_view == web_view

        assert metadata.extra == {
            'commitSha': COMMIT_SHA,
            'branch': 'master',
            'webView': web_view,
            'lastCommitSha': '90c8f7eef948',
        }

        resource = 'mst3k'
        assert metadata._json_api_links(resource) == {
            'delete': None,
            'upload': None,
            'move': 'http://localhost:7777/v1/resources/{}/providers/bitbucket{}?commitSha={}'.format(resource, full_path, COMMIT_SHA),
            'download': 'http://localhost:7777/v1/resources/{}/providers/bitbucket{}?commitSha={}'.format(resource, full_path, COMMIT_SHA),
        }

    def test_build_folder_metadata(self, folder_metadata, owner, repo):
        branch = 'master'
        name = 'plaster'

        path = BitbucketPath('/{}/'.format(name), _ids=[(None, branch), (None, branch)])

        try:
            metadata = BitbucketFolderMetadata(folder_metadata, path, owner=owner, repo=repo)
        except Exception as exc:
            pytest.fail(str(exc))

        assert metadata.name == name
        assert metadata.path == '/{}/'.format(name)
        assert metadata.kind == 'folder'
        assert metadata.children is None
        assert metadata.extra == {
            'commitSha': None,
            'branch': branch,
        }
        assert metadata.provider == 'bitbucket'

        assert metadata.commit_sha is None
        assert metadata.branch_name == branch

        assert metadata._json_api_links('mst3k') == {
            'delete': None,
            'upload': None,
            'move': 'http://localhost:7777/v1/resources/mst3k/providers/bitbucket/{}/?branch={}'.format(name, branch),
            'new_folder': None,
        }

    def test_build_revision_metadata(self, revision_metadata):
        try:
            metadata = BitbucketRevisionMetadata(revision_metadata)
        except Exception as exc:
            pytest.fail(str(exc))

        assert metadata.modified == '2016-09-08 21:20:59'
        assert metadata.modified_utc == '2016-09-08T19:20:59+00:00'
        assert metadata.version_identifier == 'commitSha'
        assert metadata.version == '522a6be9f98ddf7938d7e9568a6375cd0f88e40e'
        assert metadata.extra == {
            'user': {
                'name': 'Fitz Elliott',
            },
            'branch': 'smallbranch-a',
        }
