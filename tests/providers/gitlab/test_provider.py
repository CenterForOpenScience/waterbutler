import hashlib

import pytest
import aiohttpretty

from waterbutler.core import exceptions

from waterbutler.providers.gitlab import GitLabProvider
from waterbutler.providers.gitlab.path import GitLabPath
from waterbutler.providers.gitlab.metadata import GitLabFileMetadata
from waterbutler.providers.gitlab.metadata import GitLabFolderMetadata

from tests.providers.gitlab.fixtures import (simple_tree, simple_file_metadata, subfolder_tree,
                                             revisions_for_file, default_branches, )


@pytest.fixture
def auth():
    return {
        'name': 'cat',
        'email': 'cat@cat.com',
    }


@pytest.fixture
def credentials():
    return {'token': 'naps'}


@pytest.fixture
def settings():
    return {
        'owner': 'cat',
        'repo': 'food',
        'repo_id': '123',
        'host': 'http://base.url',
    }


@pytest.fixture
def repo_metadata():
    return {
        'full_name': 'octocat/Hello-World',
        'settings': {
            'push': False,
            'admin': False,
            'pull': True
        }
    }


@pytest.fixture
def provider(auth, credentials, settings, repo_metadata):
    provider = GitLabProvider(auth, credentials, settings)
    return provider


@pytest.fixture
def other(auth, credentials, settings, repo_metadata):
    provider = GitLabProvider(auth, credentials, settings)
    return provider


class TestHelpers:
    def test_build_repo_url(self, provider, settings):
        expected = 'http://base.url/api/v4/projects/123/contents'
        assert provider._build_repo_url('contents') == expected


class TestValidatePath:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_root(self, provider, default_branches):
        path = '/'
        default_branch_url = 'http://base.url/api/v4/projects/123'
        body = default_branches['default_branch']
        aiohttpretty.register_json_uri('GET', default_branch_url, body=body, status=200)

        commit_sha_url = 'http://base.url/api/v4/projects/123/repository/branches/master'
        commit_sha_body = default_branches['get_commit_sha']
        aiohttpretty.register_json_uri('GET', commit_sha_url, body=commit_sha_body, status=200)

        root_path = await provider.validate_v1_path(path)

        assert root_path.is_dir
        assert root_path.is_root
        assert root_path.commit_sha == '5e4718bd52874cf373dad0e9ca602a9a36f87e5c'
        assert root_path.branch_name == 'master'
        assert root_path.extra == {
            'commitSha': '5e4718bd52874cf373dad0e9ca602a9a36f87e5c',
            'branchName': 'master',
        }

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_root_by_branch(self, provider, default_branches):
        commit_sha_url = 'http://base.url/api/v4/projects/123/repository/branches/otherbranch'
        commit_sha_body = default_branches['get_commit_sha']
        aiohttpretty.register_json_uri('GET', commit_sha_url, body=commit_sha_body, status=200)

        root_path = await provider.validate_v1_path('/', branch='otherbranch')

        assert root_path.is_dir
        assert root_path.is_root
        assert root_path.commit_sha == '5e4718bd52874cf373dad0e9ca602a9a36f87e5c'
        assert root_path.branch_name == 'otherbranch'
        assert root_path.extra == {
            'commitSha': '5e4718bd52874cf373dad0e9ca602a9a36f87e5c',
            'branchName': 'otherbranch',
        }

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_root_by_commit_sha(self, provider):
        path = '/'
        root_path = await provider.validate_v1_path(path, commitSha='a1b2c3d4')

        assert root_path.is_dir
        assert root_path.is_root
        assert root_path.commit_sha == 'a1b2c3d4'
        assert root_path.branch_name is None
        assert root_path.extra == {
            'commitSha': 'a1b2c3d4',
            'branchName': None,
        }

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_root_by_revision_sha(self, provider):
        path = '/'
        root_path = await provider.validate_v1_path(path, revision='a1b2c3d4')

        assert root_path.is_dir
        assert root_path.is_root
        assert root_path.commit_sha == 'a1b2c3d4'
        assert root_path.branch_name is None
        assert root_path.extra == {
            'commitSha': 'a1b2c3d4',
            'branchName': None,
        }

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_root_by_revision_branch(self, provider, default_branches):
        commit_sha_url = 'http://base.url/api/v4/projects/123/repository/branches/otherbranch'
        commit_sha_body = default_branches['get_commit_sha']
        aiohttpretty.register_json_uri('GET', commit_sha_url, body=commit_sha_body, status=200)

        root_path = await provider.validate_v1_path('/', revision='otherbranch')

        assert root_path.is_dir
        assert root_path.is_root
        assert root_path.commit_sha == '5e4718bd52874cf373dad0e9ca602a9a36f87e5c'
        assert root_path.branch_name == 'otherbranch'
        assert root_path.extra == {
            'commitSha': '5e4718bd52874cf373dad0e9ca602a9a36f87e5c',
            'branchName': 'otherbranch',
        }

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_file(self, provider, simple_tree):
        path = '/folder1/file1'
        url = ('http://base.url/api/v4/projects/123/repository/tree'
               '?path=folder1/&page=1&per_page={}&ref=a1b2c3d4'.format(provider.MAX_PAGE_SIZE))
        aiohttpretty.register_json_uri('GET', url, body=simple_tree)

        try:
            file_path = await provider.validate_v1_path(path, commitSha='a1b2c3d4',
                                                        branch='master')
        except Exception as exc:
            pytest.fail(str(exc))

        assert file_path.is_file
        assert not file_path.is_root
        assert file_path.commit_sha == 'a1b2c3d4'
        assert file_path.branch_name == 'master'
        assert file_path.extra == {
            'commitSha': 'a1b2c3d4',
            'branchName': 'master',
        }

        parent_path = file_path.parent
        assert parent_path.commit_sha == 'a1b2c3d4'
        assert parent_path.branch_name == 'master'
        assert parent_path.extra == {
            'commitSha': 'a1b2c3d4',
            'branchName': 'master',
        }

        root_path = parent_path.parent
        assert root_path.commit_sha == 'a1b2c3d4'
        assert root_path.branch_name == 'master'
        assert root_path.extra == {
            'commitSha': 'a1b2c3d4',
            'branchName': 'master',
        }

        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider.validate_v1_path(path + '/', commitSha='a1b2c3d4')

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_folder(self, provider, subfolder_tree):
        path = '/files/lfs/'
        url = ('http://base.url/api/v4/projects/123/repository/tree'
               '?path=files/&page=1&per_page={}&ref=a1b2c3d4'.format(provider.MAX_PAGE_SIZE))
        aiohttpretty.register_json_uri('GET', url, body=subfolder_tree)

        try:
            folder_path = await provider.validate_v1_path(path, commitSha='a1b2c3d4',
                                                          branch='master')
        except Exception as exc:
            pytest.fail(str(exc))

        assert folder_path.is_folder
        assert not folder_path.is_root
        assert folder_path.commit_sha == 'a1b2c3d4'
        assert folder_path.branch_name == 'master'

        parent_path = folder_path.parent
        assert parent_path.commit_sha == 'a1b2c3d4'
        assert parent_path.branch_name == 'master'

        root_path = parent_path.parent
        assert root_path.commit_sha == 'a1b2c3d4'
        assert root_path.branch_name == 'master'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_no_such_repository(self, provider):
        provider.repo_id = '456'
        path = '/'
        default_branch_url = 'http://base.url/api/v4/projects/456'
        aiohttpretty.register_json_uri('GET', default_branch_url, body={}, status=404)

        with pytest.raises(exceptions.NotFoundError) as exc:
            _ = await provider.validate_v1_path(path)
        assert exc.value.code == 404

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_uninitialized_repository(self, provider):
        provider.repo_id = '456'
        path = '/'
        default_branch_url = 'http://base.url/api/v4/projects/456'
        aiohttpretty.register_json_uri('GET', default_branch_url, body={"default_branch": None})

        with pytest.raises(exceptions.UninitializedRepositoryError) as exc:
            _ = await provider.validate_v1_path(path)
        assert exc.value.code == 400


class TestMetadata:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file_with_default_ref(self, provider, simple_file_metadata,
                                                  revisions_for_file):
        path = '/folder1/folder2/file'
        gl_path = GitLabPath(path, _ids=([('a1b2c3d4', 'master')] * 4))

        url = ('http://base.url/api/v4/projects/123/repository/files/'
               'folder1%2Ffolder2%2Ffile?ref=a1b2c3d4')
        aiohttpretty.register_json_uri('GET', url, body=simple_file_metadata)

        history_url = ('http://base.url/api/v4/projects/123/repository/commits'
                       '?path=folder1/folder2/file&ref_name=a1b2c3d4&page=1'
                       '&per_page={}'.format(provider.MAX_PAGE_SIZE))
        aiohttpretty.register_json_uri('GET', history_url, body=revisions_for_file)

        etag = hashlib.sha256('{}::{}::{}'.format('gitlab', path, 'a1b2c3d4').encode('utf-8'))\
                      .hexdigest()
        result = await provider.metadata(gl_path)
        assert result.serialized() == {
            'name': 'file',
            'kind': 'file',
            'size': 123,
            'sizeInt': 123,
            'provider':'gitlab',
            'path': path,
            'materialized': path,
            'modified': '2017-07-24T16:02:17.000-04:00',
            'modified_utc': '2017-07-24T20:02:17+00:00',
            'created_utc': '2016-11-30T18:30:23+00:00',
            'contentType': None,
            'etag': etag,
            'extra': {
                'commitSha': 'a1b2c3d4',
                'branch': 'master',
                'webView': 'http://base.url/cat/food/blob/master/folder1/folder2/file',
            },
        }
        assert result.json_api_serialized('mst3k')['links'] == {
            'move': ('http://localhost:7777/v1/resources/mst3k/providers/gitlab'
                     '/folder1/folder2/file?commitSha=a1b2c3d4'),
            'upload': None,
            'download': ('http://localhost:7777/v1/resources/mst3k/providers/gitlab'
                         '/folder1/folder2/file?commitSha=a1b2c3d4'),
            'delete': None,
        }

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file_with_branch(self, provider,
                                             simple_file_metadata, revisions_for_file):
        path = '/folder1/folder2/file'
        gl_path = GitLabPath(path, _ids=([(None, 'my-branch')] * 4))

        url = ('http://base.url/api/v4/projects/123/repository/files/'
               'folder1%2Ffolder2%2Ffile?ref=my-branch')
        aiohttpretty.register_json_uri('GET', url, body=simple_file_metadata)

        history_url = ('http://base.url/api/v4/projects/123/repository/commits'
                       '?path=folder1/folder2/file&ref_name=my-branch&page=1'
                       '&per_page={}'.format(provider.MAX_PAGE_SIZE))
        aiohttpretty.register_json_uri('GET', history_url, body=revisions_for_file)

        result = await provider.metadata(gl_path)
        assert result.json_api_serialized('mst3k')['links'] == {
            'move': ('http://localhost:7777/v1/resources/mst3k/providers/gitlab'
                     '/folder1/folder2/file?branch=my-branch'),
            'upload': None,
            'download': ('http://localhost:7777/v1/resources/mst3k/providers/gitlab'
                         '/folder1/folder2/file?branch=my-branch'),
            'delete': None,
        }

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file_no_such_file(self, provider):
        path = '/folder1/folder2/file'
        gl_path = GitLabPath(path, _ids=([('a1b2c3d4', 'master')] * 4))

        url = ('http://base.url/api/v4/projects/123/repository/files/'
               'folder1%2Ffolder2%2Ffile?ref=a1b2c3d4')
        aiohttpretty.register_json_uri('GET', url, body={}, status=404)

        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider.metadata(gl_path)

        assert exc.value.code == 404

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder(self, provider):
        path = '/folder1/folder2/folder3/'
        gl_path = GitLabPath(path, _ids=([('a1b2c3d4', 'master')] * 4))

        url = ('http://base.url/api/v4/projects/123/repository/tree'
               '?path=folder1/folder2/folder3/&ref=a1b2c3d4&page=1'
               '&per_page={}'.format(provider.MAX_PAGE_SIZE))
        aiohttpretty.register_json_uri('GET', url, body=[
            {
                'id': '123',
                'type': 'tree',
                'name': 'my folder'
            },
            {
                'id': '1234',
                'type': 'file',
                'name': 'my file'
            }
        ])

        result = await provider.metadata(gl_path)

        assert isinstance(result[0], GitLabFolderMetadata)
        assert result[0].name == 'my folder'
        assert result[0].json_api_serialized('mst3k')['links'] == {
            'move': ('http://localhost:7777/v1/resources/mst3k/providers/gitlab'
                     '/folder1/folder2/folder3/my%20folder/?commitSha=a1b2c3d4'),
            'upload': None,
            'delete': None,
            'new_folder': None,
        }

        assert result[1].name == 'my file'
        assert isinstance(result[1], GitLabFileMetadata)

        child_path = provider.path_from_metadata(gl_path, result[1])
        child_path.name == 'my file'
        child_path.commit_sha == 'a1b2c3d4'
        child_path.branch_name == 'master'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder_no_such_folder_200(self, provider):
        path = '/folder1/folder2/folder3/'
        gl_path = GitLabPath(path, _ids=([('a1b2c3d4', 'master')] * 4))

        url = ('http://base.url/api/v4/projects/123/repository/tree'
               '?path=folder1/folder2/folder3/&ref=a1b2c3d4&page=1'
               '&per_page={}'.format(provider.MAX_PAGE_SIZE))
        aiohttpretty.register_json_uri('GET', url, body=[])

        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider.metadata(gl_path)

        assert exc.value.code == 404

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder_no_such_folder_404(self, provider):
        path = '/folder1/folder2/folder3/'
        gl_path = GitLabPath(path, _ids=([('a1b2c3d4', 'master')] * 4))

        url = ('http://base.url/api/v4/projects/123/repository/tree'
               '?path=folder1/folder2/folder3/&ref=a1b2c3d4&page=1'
               '&per_page={}'.format(provider.MAX_PAGE_SIZE))
        aiohttpretty.register_json_uri('GET', url, body={}, status=404)

        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider.metadata(gl_path)

        assert exc.value.code == 404


class TestRevisions:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_revisions(self, provider, revisions_for_file):
        path = '/folder1/folder2/file'
        gl_path = GitLabPath(path, _ids=([('a1b2c3d4', 'master')] * 4))

        url = ('http://base.url/api/v4/projects/123/repository/commits'
               '?path=folder1/folder2/file&ref_name=a1b2c3d4')
        aiohttpretty.register_json_uri('GET', url, body=revisions_for_file)

        revisions = await provider.revisions(gl_path)
        assert len(revisions) == 3

        assert revisions[0].serialized() == {
            'version': '931aece9275c0d084dfa7f6e0b3b2bb250e4b089',
            'modified': '2017-07-24T16:02:17.000-04:00',
            'modified_utc': '2017-07-24T20:02:17+00:00',
            'versionIdentifier': 'commitSha',
            'extra': {
                'user': {
                    'name': 'Fitz Elliott',
                },
            },
        }

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_no_such_revision(self, provider):
        path = '/folder1/folder2/file'
        gl_path = GitLabPath(path, _ids=([('a1b2c3d4', 'master')] * 4))

        url = ('http://base.url/api/v4/projects/123/repository/commits'
               '?path=folder1/folder2/file&ref_name=a1b2c3d4')
        aiohttpretty.register_json_uri('GET', url, body=[])

        with pytest.raises(exceptions.RevisionsError) as exc:
            await provider.revisions(gl_path)
        assert exc.value.code == 404


class TestDownload:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download(self, provider):
        path = '/folder1/file.py'
        gl_path = GitLabPath(path, _ids=([('a1b2c3d4', 'master')] * 3))

        url = ('http://base.url/api/v4/projects/123/repository/files'
               '/folder1%2Ffile.py/raw?ref=a1b2c3d4')
        aiohttpretty.register_uri('GET', url, body=b'hello', headers={'X-Gitlab-Size': '5'})

        result = await provider.download(gl_path, branch='master')
        assert await result.read() == b'hello'


class TestReadOnlyProvider:

    def test_can_duplicate_names(self, provider):
        assert provider.can_duplicate_names() == False

    @pytest.mark.asyncio
    async def test_upload(self, provider):
        with pytest.raises(exceptions.ReadOnlyProviderError) as e:
            await provider.upload('/foo-file.txt')
        assert e.value.code == 501

    @pytest.mark.asyncio
    async def test_create_folder(self, provider):
        with pytest.raises(exceptions.ReadOnlyProviderError) as e:
            await provider.create_folder('foo')
        assert e.value.code == 501

    @pytest.mark.asyncio
    async def test_delete(self, provider):
        with pytest.raises(exceptions.ReadOnlyProviderError) as e:
            await provider.delete()
        assert e.value.code == 501

    @pytest.mark.asyncio
    async def test_move(self, provider):
        with pytest.raises(exceptions.ReadOnlyProviderError) as e:
            await provider.move()
        assert e.value.code == 501

    @pytest.mark.asyncio
    async def test_copy_to(self, provider):
        with pytest.raises(exceptions.ReadOnlyProviderError) as e:
            await provider.copy(provider)
        assert e.value.code == 501

    def test_can_intra_move(self, provider):
        assert provider.can_intra_move(provider) == False

    def test_can_intra_copy(self, provider):
        assert provider.can_intra_copy(provider) == False
