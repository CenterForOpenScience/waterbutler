import pytest

from waterbutler.providers.bitbucket.path import BitbucketPath
from waterbutler.providers.bitbucket.metadata import (BitbucketFileMetadata,
                                                      BitbucketFolderMetadata,
                                                      BitbucketRevisionMetadata)

from .metadata_fixtures import owner, repo, file_metadata, folder_metadata, revision_metadata


COMMIT_SHA = 'abc123def456'
BRANCH = 'develop'


class TestBitbucketMetadata:

    def test_build_file_metadata(self, file_metadata, owner, repo):

        name = 'file0002.20bytes.txt'
        subdir = 'folder2-lvl1/folder1-lvl2/folder1-lvl3'
        full_path = '/{}/{}'.format(subdir, name)
        # Note: When building Bitbucket Path, the length of ``_ids`` array must be equal to the
        #       number of path segments, including the root.
        path = BitbucketPath(full_path, _ids=[(COMMIT_SHA, BRANCH) for _ in full_path.split('/')])

        try:
            metadata = BitbucketFileMetadata(file_metadata, path, owner=owner, repo=repo)
        except Exception as exc:
            pytest.fail(str(exc))
            return

        assert metadata.name == name
        assert metadata.path == full_path
        assert metadata.kind == 'file'
        assert metadata.modified == '2019-04-26T15:13:12+00:00'
        assert metadata.modified_utc == '2019-04-26T15:13:12+00:00'
        assert metadata.created_utc == '2019-04-25T06:18:21+00:00'
        assert metadata.content_type is None
        assert metadata.size == 20
        assert metadata.size_as_int == 20
        assert metadata.etag == '{}::{}'.format(full_path, COMMIT_SHA)
        assert metadata.provider == 'bitbucket'
        assert metadata.last_commit_sha == 'dd8c7b642e32'
        assert metadata.commit_sha == COMMIT_SHA
        assert metadata.branch_name == BRANCH

        web_view = ('https://bitbucket.org/{}/{}/src/{}{}?'
                    'fileviewer=file-view-default'.format(owner, repo, COMMIT_SHA, full_path))
        assert metadata.web_view == web_view

        assert metadata.extra == {
            'commitSha': COMMIT_SHA,
            'branch': BRANCH,
            'webView': web_view,
            'lastCommitSha': 'dd8c7b642e32',
        }

        resource = 'mst3k'
        assert metadata._json_api_links(resource) == {
            'delete': None,
            'upload': None,
            'move': 'http://localhost:7777/v1/resources/{}/providers/bitbucket{}?'
                    'commitSha={}'.format(resource, full_path, COMMIT_SHA),
            'download': 'http://localhost:7777/v1/resources/{}/providers/bitbucket{}?'
                        'commitSha={}'.format(resource, full_path, COMMIT_SHA),
        }

    def test_build_folder_metadata(self, folder_metadata, owner, repo):
        name = 'folder1-lvl3'
        subdir = 'folder2-lvl1/folder1-lvl2'
        full_path = '/{}/{}'.format(subdir, name)
        path = BitbucketPath(full_path, _ids=[(None, BRANCH) for _ in full_path.split('/')])

        try:
            metadata = BitbucketFolderMetadata(folder_metadata, path, owner=owner, repo=repo)
        except Exception as exc:
            pytest.fail(str(exc))
            return

        assert metadata.name == name
        assert metadata.path == '{}/'.format(full_path)
        assert metadata.kind == 'folder'
        assert metadata.children is None
        assert metadata.extra == {
            'commitSha': None,
            'branch': BRANCH,
        }
        assert metadata.provider == 'bitbucket'

        assert metadata.commit_sha is None
        assert metadata.branch_name == BRANCH

        assert metadata._json_api_links('mst3k') == {
            'delete': None,
            'upload': None,
            'move': 'http://localhost:7777/v1/resources/mst3k/providers/bitbucket{}/?'
                    'branch={}'.format(full_path, BRANCH),
            'new_folder': None,
        }

    def test_build_revision_metadata(self, revision_metadata):

        try:
            metadata = BitbucketRevisionMetadata(revision_metadata)
        except Exception as exc:
            pytest.fail(str(exc))
            return

        assert metadata.modified == '2019-04-25T11:58:30+00:00'
        assert metadata.modified_utc == '2019-04-25T11:58:30+00:00'
        assert metadata.version_identifier == 'commitSha'
        assert metadata.version == 'ad0412ab6f8e6d614701e290843e160d002cc124'
        assert metadata.extra == {'user': {'name': 'longze chen'}, 'branch': None}
