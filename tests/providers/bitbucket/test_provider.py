import json
import pytest
from urllib.parse import urlencode

import aiohttpretty

from waterbutler.core import exceptions

from waterbutler.providers.bitbucket import BitbucketProvider
from waterbutler.providers.bitbucket.provider import BitbucketPath
from waterbutler.providers.bitbucket.metadata import BitbucketFileMetadata

from tests.utils import MockCoroutine
from .provider_fixtures import (repo_metadata, folder_contents_page_1, folder_contents_page_2,
                                branch_metadata, path_metadata_file, folder_full_contents_list,
                                path_metadata_folder, file_history_page_1, file_history_page_2, )
from .metadata_fixtures import owner, repo, file_metadata, folder_metadata

COMMIT_SHA = 'abc123def456abc123def456'
BRANCH = 'develop'


@pytest.fixture
def auth():
    return {
        'name': 'fake_auth',
        'email': 'fake_auth@cos.io',
    }


@pytest.fixture
def credentials():
    return {'token': 'fake_token'}


@pytest.fixture
def settings():
    return {
        'owner': 'cslzchen',
        'repo': 'waterbutler-public',
    }


@pytest.fixture
def provider(auth, credentials, settings):
    provider = BitbucketProvider(auth, credentials, settings)
    provider.RESP_PAGE_LEN = 10
    return provider


class TestValidatePath:

    @pytest.mark.asyncio
    async def test_validate_v1_path_root(self, provider, repo_metadata):
        # Mock ``_fetch_default_branch()`` instead of using ``aiohttpretty``
        repo_metadata = json.loads(repo_metadata)
        provider._fetch_default_branch = MockCoroutine(
            return_value=repo_metadata['mainbranch']['name'])

        try:
            wb_path_v1 = await provider.validate_v1_path('/')
        except Exception as exc:
            return pytest.fail(str(exc))
        wb_path_v0 = await provider.validate_path('/')
        assert wb_path_v1 == wb_path_v0
        assert wb_path_v1.branch_name == repo_metadata['mainbranch']['name']
        assert wb_path_v1.commit_sha is None

    @pytest.mark.asyncio
    async def test_validate_v1_path_file(self, provider, repo_metadata,
                                         path_metadata_folder, folder_full_contents_list):
        file_path = '/folder2-lvl1/folder1-lvl2/folder1-lvl3/file0001.20bytes.txt'

        # Mock ``_fetch_default_branch()`` instead of using ``aiohttpretty``
        repo_metadata = json.loads(repo_metadata)
        provider._fetch_default_branch = MockCoroutine(
            return_value=repo_metadata['mainbranch']['name'])
        # Mock ``_fetch_path_metadata()`` instead of using ``aiohttpretty``
        parent_folder_metadata = json.loads(path_metadata_folder)['non_root_lvl3']
        provider._fetch_path_metadata = MockCoroutine(return_value=parent_folder_metadata)
        # Mock ``_fetch_dir_listing()`` instead of using ``aiohttpretty``
        parent_dir_listing = json.loads(folder_full_contents_list)['file_parent']
        provider._fetch_dir_listing = MockCoroutine(return_value=parent_dir_listing)

        try:
            wb_path_v1 = await provider.validate_v1_path(file_path)
        except Exception as exc:
            return pytest.fail(str(exc))
        wb_path_v0 = await provider.validate_path(file_path)

        assert wb_path_v1 == wb_path_v0
        assert wb_path_v1.branch_name == repo_metadata['mainbranch']['name']
        assert wb_path_v1.commit_sha == parent_folder_metadata['commit']['hash'][:12]

        bad_path = '{}/'.format(file_path)
        with pytest.raises(exceptions.NotFoundError):
            await provider.validate_v1_path(bad_path)

    @pytest.mark.asyncio
    async def test_validate_v1_path_folder(self, provider, repo_metadata,
                                           path_metadata_folder, folder_full_contents_list):
        folder_path = '/folder2-lvl1/folder1-lvl2/folder1-lvl3/'

        repo_metadata = json.loads(repo_metadata)
        provider._fetch_default_branch = MockCoroutine(
            return_value=repo_metadata['mainbranch']['name'])
        parent_folder_metadata = json.loads(path_metadata_folder)['non_root_lvl2']
        provider._fetch_path_metadata = MockCoroutine(return_value=parent_folder_metadata)
        parent_dir_listing = json.loads(folder_full_contents_list)['folder_parent']
        provider._fetch_dir_listing = MockCoroutine(return_value=parent_dir_listing)

        try:
            wb_path_v1 = await provider.validate_v1_path(folder_path)
        except Exception as exc:
            return pytest.fail(str(exc))
        wb_path_v0 = await provider.validate_path(folder_path)
        assert wb_path_v1 == wb_path_v0
        assert wb_path_v1.branch_name == repo_metadata['mainbranch']['name']
        assert wb_path_v1.commit_sha == parent_folder_metadata['commit']['hash'][:12]

        bad_path = folder_path.rstrip('/')
        with pytest.raises(exceptions.NotFoundError):
            await provider.validate_v1_path(bad_path)

    @pytest.mark.asyncio
    async def test_validate_v1_commit_sha(self, provider, repo_metadata,
                                           path_metadata_folder, folder_full_contents_list):
        file_path = '/folder2-lvl1/folder1-lvl2/folder1-lvl3/file0001.20bytes.txt'

        repo_metadata = json.loads(repo_metadata)
        provider._fetch_default_branch = MockCoroutine(
            return_value=repo_metadata['mainbranch']['name'])
        parent_folder_metadata = json.loads(path_metadata_folder)['non_root_lvl3']
        provider._fetch_path_metadata = MockCoroutine(return_value=parent_folder_metadata)
        parent_dir_listing = json.loads(folder_full_contents_list)['file_parent']
        provider._fetch_dir_listing = MockCoroutine(return_value=parent_dir_listing)

        try:
            wb_path_v1 = await provider.validate_v1_path(file_path, commitSha=COMMIT_SHA)
        except Exception as exc:
            return pytest.fail(str(exc))
        wb_path_v0 = await provider.validate_path(file_path, commitSha=COMMIT_SHA)
        assert wb_path_v1 == wb_path_v0
        assert wb_path_v1.branch_name is None
        assert wb_path_v1.commit_sha == COMMIT_SHA
        assert wb_path_v1.ref == wb_path_v1.commit_sha

        try:
            wb_path_v1 = await provider.validate_v1_path(file_path, branch='other-branch')
        except Exception as exc:
            return pytest.fail(str(exc))
        wb_path_v0 = await provider.validate_path(file_path, branch='other-branch')
        assert wb_path_v1 == wb_path_v0
        assert wb_path_v1.branch_name == 'other-branch'
        assert wb_path_v1.commit_sha == parent_folder_metadata['commit']['hash'][:12]
        assert wb_path_v1.ref == wb_path_v1.commit_sha

        try:
            wb_path_v1 = await provider.validate_v1_path(file_path, revision='abc123def456')
        except Exception as exc:
            return pytest.fail(str(exc))
        wb_path_v0 = await provider.validate_path(file_path, revision='abc123def456')
        assert wb_path_v1 == wb_path_v0
        assert wb_path_v1.branch_name is None
        assert wb_path_v1.commit_sha == 'abc123def456'
        assert wb_path_v1.ref == wb_path_v1.commit_sha

        try:
            wb_path_v1 = await provider.validate_v1_path(file_path, revision='revision-abc123')
        except Exception as exc:
            return pytest.fail(str(exc))
        wb_path_v0 = await provider.validate_path(file_path, revision='revision-abc123')
        assert wb_path_v1 == wb_path_v0
        assert wb_path_v1.branch_name == 'revision-abc123'
        assert wb_path_v1.commit_sha == parent_folder_metadata['commit']['hash'][:12]
        assert wb_path_v1.ref == wb_path_v1.commit_sha


class TestRepo:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_get_default_branch(self, provider, repo_metadata):
        repo_metadata = json.loads(repo_metadata)
        repo_url = '{}/?{}'.format(provider._build_v2_repo_url(),
                                   urlencode({'fields': 'mainbranch.name'}))
        aiohttpretty.register_json_uri('GET', repo_url, body=repo_metadata)
        result = await provider._fetch_default_branch()
        assert result == repo_metadata['mainbranch']['name']


class TestRevisions:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_get_revisions(self, provider, file_history_page_1, file_history_page_2):
        path = BitbucketPath('/file0001.20bytes.txt',
                             _ids=[(COMMIT_SHA, 'develop'), (COMMIT_SHA, 'develop')])

        file_history_page_1 = json.loads(file_history_page_1)
        query_params = {
            'pagelen': provider.RESP_PAGE_LEN,
            'fields': 'values.commit.hash,values.commit.date,values.commit.author.raw,'
                      'values.size,values.path,values.type,next'
        }
        file_history_first_url = provider._build_v2_repo_url('filehistory', COMMIT_SHA,
                                                             'file0001.20bytes.txt', **query_params)
        aiohttpretty.register_json_uri('GET', file_history_first_url, body=file_history_page_1)

        file_history_page_2 = json.loads(file_history_page_2)
        file_history_next_url = file_history_page_1['next']
        aiohttpretty.register_json_uri('GET', file_history_next_url, body=file_history_page_2)

        result = await provider.revisions(path)

        assert len(result) == 14
        assert result[0].modified == '2019-04-25T11:58:30+00:00'
        assert result[0].modified_utc == '2019-04-25T11:58:30+00:00'
        assert result[0].version_identifier == 'commitSha'
        assert result[0].version == 'ad0412ab6f8e6d614701e290843e160d002cc124'
        assert result[0].extra == {'user': {'name': 'longze chen'}, 'branch': None}


class TestMetadata:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_get_metadata_for_file(self, provider, path_metadata_file,
                                         file_history_page_1, file_history_page_2):
        path = BitbucketPath('/file0001.20bytes.txt',
                             _ids=[(COMMIT_SHA, 'develop'), (COMMIT_SHA, 'develop')])

        provider._fetch_branch_commit_sha = MockCoroutine(return_value=COMMIT_SHA)

        file_metadata = json.loads(path_metadata_file)['root']
        query_params = {
            'format': 'meta',
            'fields': 'commit.hash,commit.date,path,size,links.history.href'
        }
        path_meta_url = '{}/?{}'.format(
            provider._build_v2_repo_url('src', COMMIT_SHA, *path.path_tuple()),
            urlencode(query_params)
        )
        aiohttpretty.register_json_uri('GET', path_meta_url, body=file_metadata)

        file_history_page_1 = json.loads(file_history_page_1)
        query_params = {
            'pagelen': provider.RESP_PAGE_LEN,
            'fields': 'values.commit.hash,values.commit.date,values.commit.author.raw,'
                      'values.size,values.path,values.type,next'
        }
        file_history_first_url = '{}?{}'.format(file_metadata['links']['history']['href'],
                                                 urlencode(query_params))
        aiohttpretty.register_json_uri('GET', file_history_first_url, body=file_history_page_1)

        file_history_page_2 = json.loads(file_history_page_2)
        file_history_next_url = file_history_page_1['next']
        aiohttpretty.register_json_uri('GET', file_history_next_url, body=file_history_page_2)

        metadata = await provider.metadata(path)

        assert not provider._fetch_branch_commit_sha.called
        assert metadata is not None
        assert metadata.name == 'file0001.20bytes.txt'
        assert metadata.path == '/file0001.20bytes.txt'
        assert metadata.kind == 'file'
        assert metadata.modified == '2019-04-25T11:58:30+00:00'
        assert metadata.modified_utc == '2019-04-25T11:58:30+00:00'
        assert metadata.created_utc == '2019-04-24T12:18:21+00:00'
        assert metadata.content_type is None
        assert metadata.size == 20
        assert metadata.size_as_int == 20
        assert metadata.etag == '{}::{}'.format('/file0001.20bytes.txt', COMMIT_SHA)
        assert metadata.provider == 'bitbucket'
        assert metadata.last_commit_sha == 'ad0412ab6f8e'
        assert metadata.commit_sha == COMMIT_SHA
        assert metadata.branch_name == BRANCH

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_get_metadata_for_folder(self, provider, path_metadata_folder,
                                           folder_contents_page_1, folder_contents_page_2, ):

        path = BitbucketPath('/', _ids=[(None, 'develop')], folder=True)

        provider._fetch_branch_commit_sha = MockCoroutine(return_value=COMMIT_SHA)

        folder_metadata = json.loads(path_metadata_folder)['root']
        query_params = {
            'format': 'meta',
            'fields': 'commit.hash,commit.date,path,size,links.history.href'
        }
        path_meta_url = '{}/?{}'.format(
            provider._build_v2_repo_url('src', COMMIT_SHA, *path.path_tuple()),
            urlencode(query_params)
        )
        aiohttpretty.register_json_uri('GET', path_meta_url, body=folder_metadata)

        dir_contents_first_page = json.loads(folder_contents_page_1)
        query_params = {
            'pagelen': provider.RESP_PAGE_LEN,
            'fields': 'values.path,values.size,values.type,next',
        }
        dir_list_base_url = provider._build_v2_repo_url('src', COMMIT_SHA, *path.path_tuple())
        dir_list_first_url = '{}/?{}'.format(dir_list_base_url, urlencode(query_params))
        aiohttpretty.register_json_uri('GET', dir_list_first_url, body=dir_contents_first_page)

        dir_contents_next_page = json.loads(folder_contents_page_2)
        dir_list_next_url = dir_contents_first_page['next']
        aiohttpretty.register_json_uri('GET', dir_list_next_url, body=dir_contents_next_page)

        result = await provider.metadata(path)
        assert provider._fetch_branch_commit_sha.called
        assert len(result) == 15

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_get_path_metadata_without_commit_sha(self, provider, path_metadata_file):

        path = BitbucketPath('/file0001.20bytes.txt',
                             _ids=[(None, 'develop'), (None, 'develop')])

        provider._fetch_branch_commit_sha = MockCoroutine(return_value=COMMIT_SHA)

        file_metadata = json.loads(path_metadata_file)['root']
        query_params = {
            'format': 'meta',
            'fields': 'commit.hash,commit.date,path,size,links.history.href'
        }
        path_meta_url = '{}/?{}'.format(
            provider._build_v2_repo_url('src', COMMIT_SHA, *path.path_tuple()),
            urlencode(query_params)
        )
        aiohttpretty.register_json_uri('GET', path_meta_url, body=file_metadata)

        await provider._fetch_path_metadata(path)
        assert provider._fetch_branch_commit_sha.called
        assert path.commit_sha == COMMIT_SHA

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_get_path_metadata_with_commit_sha(self, provider, branch_metadata,
                                                        path_metadata_file):
        path = BitbucketPath('/file0001.20bytes.txt',
                             _ids=[(COMMIT_SHA, 'develop'), (COMMIT_SHA, 'develop')])

        assert path.commit_sha == COMMIT_SHA
        provider._fetch_branch_commit_sha = MockCoroutine(return_value=COMMIT_SHA)

        file_metadata = json.loads(path_metadata_file)['root']
        query_params = {
            'format': 'meta',
            'fields': 'commit.hash,commit.date,path,size,links.history.href'
        }
        path_meta_url = '{}/?{}'.format(
            provider._build_v2_repo_url('src', COMMIT_SHA, *path.path_tuple()),
            urlencode(query_params)
        )
        aiohttpretty.register_json_uri('GET', path_meta_url, body=file_metadata)

        _ = await provider._fetch_path_metadata(path)
        assert not provider._fetch_branch_commit_sha.called

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_get_branch_commit_sha(self, provider, branch_metadata):

        branch_metadata = json.loads(branch_metadata)
        query_params = {'fields': 'target.hash'}
        branch_url = '{}?{}'.format(
            provider._build_v2_repo_url('refs', 'branches', BRANCH),
            urlencode(query_params)
        )
        aiohttpretty.register_json_uri('GET', branch_url, body=branch_metadata)

        commit_sha = await provider._fetch_branch_commit_sha(BRANCH)
        assert commit_sha == branch_metadata['target']['hash']


class TestDownload:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_file_download(self, provider, file_metadata):

        full_path = '/folder2-lvl1/folder1-lvl2/folder1-lvl3/ile0002.20bytes.txt'
        bb_path = BitbucketPath(full_path, _ids=[(COMMIT_SHA, BRANCH) for _ in full_path.split('/')])
        bb_file_metadata = BitbucketFileMetadata(file_metadata, bb_path, owner=owner, repo=repo)

        # Use mock coroutine instead of aiohttpretty here since file metadata has already been
        # covered in another test. See ``TestMetadata::test_get_metadata_for_file()``.
        provider.metadata = MockCoroutine(return_value=bb_file_metadata)

        file_data = b'file0002.20bytes.txt'
        download_url = provider._build_v2_repo_url('src', bb_path.ref, *bb_path.path_tuple())
        aiohttpretty.register_uri('GET', download_url, body=file_data)

        result = await provider.download(bb_path)
        content = await result.response.read()
        assert content == file_data


class TestReadOnlyProvider:

    @pytest.mark.asyncio
    async def test_upload(self, provider):
        with pytest.raises(exceptions.ReadOnlyProviderError) as e:
            await provider.upload('/foo-file.txt')
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
        assert provider.can_intra_move(provider) is False

    def test_can_intra_copy(self, provider):
        assert provider.can_intra_copy(provider) is False


class TestMisc:

    def test_can_duplicate_name(self, provider):
        assert provider.can_duplicate_names() is False

    def test_path_from_metadata(self, provider, file_metadata, owner, repo):
        name = 'file0002.20bytes.txt'
        subdir = 'folder2-lvl1/folder1-lvl2/folder1-lvl3'
        full_path = '/{}/{}'.format(subdir, name)

        # Note: When building Bitbucket Path, the length of ``_ids`` array must be equal to the
        #       number of path segments, including the root.
        path = BitbucketPath(full_path, _ids=[(COMMIT_SHA, BRANCH) for _ in full_path.split('/')])

        metadata = BitbucketFileMetadata(file_metadata, path, owner=owner, repo=repo)
        child_path = provider.path_from_metadata(path.parent, metadata)

        assert child_path.full_path == path.full_path
        assert child_path == path
