import json
import pytest
from urllib.parse import urlencode

import aiohttpretty

from waterbutler.core import exceptions

from waterbutler.providers.bitbucket import BitbucketProvider
from waterbutler.providers.bitbucket.provider import BitbucketPath
from waterbutler.providers.bitbucket import settings as bitbucket_settings
from waterbutler.providers.bitbucket.metadata import (BitbucketFileMetadata,
                                                      BitbucketFolderMetadata,
                                                      BitbucketRevisionMetadata, )

from tests.utils import MockCoroutine
from .provider_fixtures import (repo_metadata,
                                path_metadata_file, path_metadata_folder,
                                folder_contents_page_1, folder_contents_page_2,
                                file_history_revisions, file_history_last_commit, )
from .metadata_fixtures import owner, repo, file_metadata, folder_metadata, revision_metadata

COMMIT_SHA = 'abc123def456'
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
        'repo': 'develop',
    }


@pytest.fixture
def provider(auth, credentials, settings):
    provider = BitbucketProvider(auth, credentials, settings)
    return provider


# class TestValidatePath:
#
#     @pytest.mark.asyncio
#     @pytest.mark.aiohttpretty
#     async def test_validate_v1_path_root(self, provider):
#         test_fixtures = fixtures.validate_path
#
#         default_branch_body = test_fixtures['default_branch']
#         default_branch_url = provider._build_v1_repo_url('main-branch')
#         aiohttpretty.register_json_uri('GET', default_branch_url, body=default_branch_body)
#
#         try:
#             wb_path_v1 = await provider.validate_v1_path('/')
#         except Exception as exc:
#             pytest.fail(str(exc))
#
#         wb_path_v0 = await provider.validate_path('/')
#
#         assert wb_path_v1 == wb_path_v0
#         assert wb_path_v1.branch_name == default_branch_body['name']
#         assert wb_path_v1.commit_sha == None
#
#     @pytest.mark.asyncio
#     @pytest.mark.aiohttpretty
#     @pytest.mark.parametrize('path,kind', [
#         ('/foo-file.txt', 'file'),
#         ('/foo-dir/',     'folder'),
#     ])
#     async def test_validate_v1_path(self, provider, path, kind):
#         test_fixtures = fixtures.validate_path
#
#         default_branch_body = test_fixtures['default_branch']
#         default_branch = default_branch_body['name']
#         default_branch_url = provider._build_v1_repo_url('main-branch')
#         aiohttpretty.register_json_uri('GET', default_branch_url, body=default_branch_body)
#
#         dir_listing_body =  test_fixtures['root_dir_listing']
#         dir_listing_url = provider._build_v1_repo_url('src', default_branch)  + '/'
#         aiohttpretty.register_json_uri('GET', dir_listing_url, body=dir_listing_body)
#
#         try:
#             wb_path_v1 = await provider.validate_v1_path(path)
#         except Exception as exc:
#             pytest.fail(str(exc))
#
#         wb_path_v0 = await provider.validate_path(path)
#
#         assert wb_path_v1 == wb_path_v0
#         assert wb_path_v1.branch_name == default_branch
#         # TODO: assert commitSha
#
#         bad_path = path.rstrip('/') if kind == 'folder' else path + '/'
#         with pytest.raises(exceptions.NotFoundError) as exc:
#             await provider.validate_v1_path(bad_path)
#
#     @pytest.mark.asyncio
#     @pytest.mark.aiohttpretty
#     @pytest.mark.parametrize('arg_name,arg_val,attr_name', [
#         ('commitSha', 'a1b2c3d4',     'commit_sha', ),
#         ('branch',    'other-branch', 'branch_name'),
#         ('revision',  'bleep-blorp',  'branch_name'),
#         ('revision',  '345def023ab29', 'commit_sha'),
#     ])
#     async def test_validate_v1_path_commit_sha(self, provider, arg_name, arg_val, attr_name):
#         test_fixtures = fixtures.validate_path
#
#         dir_listing_body =  test_fixtures['root_dir_listing']
#         base_commit = dir_listing_body['node']
#         dir_listing_url = provider._build_v1_repo_url('src', arg_val)  + '/'
#         aiohttpretty.register_json_uri('GET', dir_listing_url, body=dir_listing_body)
#
#         path = '/foo-file.txt'
#         kwargs = {arg_name: arg_val}
#         try:
#             wb_path_v1 = await provider.validate_v1_path(path, **kwargs)
#         except Exception as exc:
#             pytest.fail(str(exc))
#
#         ref_val = arg_val
#         if attr_name == 'commit_sha' and len(arg_val) < len(base_commit):
#             arg_val = base_commit
#             ref_val = base_commit
#
#         if attr_name != 'commit_sha':
#             ref_val = base_commit
#
#         commit_sha = ref_val
#         branch_name = None if attr_name == 'commit_sha' else arg_val
#
#         assert getattr(wb_path_v1, attr_name) == arg_val
#         assert wb_path_v1.ref == ref_val
#         assert wb_path_v1.extra == {
#             'commitSha': commit_sha,
#             'branchName': branch_name,
#         }
#
#         wb_path_v0 = await provider.validate_path(path, **kwargs)
#         assert wb_path_v1 == wb_path_v0
#
#     @pytest.mark.asyncio
#     @pytest.mark.aiohttpretty
#     async def test_validate_v1_path_subfolder(self, provider):
#         test_fixtures = fixtures.validate_path
#
#         dir_listing_body =  test_fixtures['subfolder_dir_listing']
#         base_commit = dir_listing_body['node']
#         dir_listing_url = provider._build_v1_repo_url('src', 'main-branch', 'subfolder')  + '/'
#         aiohttpretty.register_json_uri('GET', dir_listing_url, body=dir_listing_body)
#
#         path = '/subfolder/.gitkeep'
#         try:
#             wb_path_v1 = await provider.validate_v1_path(path, branch='main-branch')
#         except Exception as exc:
#             pytest.fail(str(exc))
#
#         wb_path_v0 = await provider.validate_path(path, branch='main-branch')
#         assert wb_path_v1 == wb_path_v0


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
    async def test_get_revisions(self, provider, file_history_revisions):
        path = BitbucketPath('/file0001.20bytes.txt',
                             _ids=[(COMMIT_SHA, 'develop'), (COMMIT_SHA, 'develop')])

        file_history_revisions = json.loads(file_history_revisions)
        query_params = {
            'fields': 'values.commit.hash,values.commit.date,values.commit.author.raw,'
                      'values.size,values.path,values.type'
        }
        revisions_url = provider._build_v2_repo_url('filehistory', COMMIT_SHA,
                                                    'file0001.20bytes.txt', **query_params)
        aiohttpretty.register_json_uri('GET', revisions_url, body=file_history_revisions)
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
    async def test_get_metadata_for_file(self, provider, path_metadata_file):
        name = 'file0002.20bytes.txt'
        subdir = 'folder2-lvl1/folder1-lvl2/folder1-lvl3'
        full_path = '/{}/{}'.format(subdir, name)
        path = BitbucketPath(full_path, _ids=[(COMMIT_SHA, BRANCH) for _ in full_path.split('/')])

        file_metadata = json.loads(path_metadata_file)['non_root']
        query_params = {'format': 'meta', 'fields': 'commit.hash,commit.date,path,size'}
        path_meta_url = '{}/?{}'.format(
            provider._build_v2_repo_url('src', path.ref, *path.path_tuple()),
            urlencode(query_params)
        )
        aiohttpretty.register_json_uri('GET', path_meta_url, body=file_metadata)
        metadata = await provider.metadata(path)

        assert metadata is not None
        assert metadata.name == name
        assert metadata.path == full_path
        assert metadata.kind == 'file'
        assert metadata.modified == '2019-04-26T15:13:12+00:00'
        assert metadata.modified_utc == '2019-04-26T15:13:12+00:00'
        assert metadata.created_utc is None
        assert metadata.content_type is None
        assert metadata.size == 20
        assert metadata.size_as_int == 20
        assert metadata.etag == '{}::{}'.format(full_path, COMMIT_SHA)
        assert metadata.provider == 'bitbucket'
        assert metadata.last_commit_sha == 'dd8c7b642e32'
        assert metadata.commit_sha == COMMIT_SHA
        assert metadata.branch_name == BRANCH

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_get_metadata_for_folder(self, provider, path_metadata_folder,
                                           folder_contents_page_1, folder_contents_page_2,
                                           file_history_last_commit):

        path = BitbucketPath('/', _ids=[(None, 'develop')], folder=True)

        folder_metadata = json.loads(path_metadata_folder)['root']
        query_params = {'format': 'meta', 'fields': 'commit.hash,commit.date,path,size'}
        path_meta_url = '{}/?{}'.format(
            provider._build_v2_repo_url('src', path.ref, *path.path_tuple()),
            urlencode(query_params)
        )
        aiohttpretty.register_json_uri('GET', path_meta_url, body=folder_metadata)

        dir_contents_first_page = json.loads(folder_contents_page_1)
        dir_list_first_url = '{}/'.format(provider._build_v2_repo_url('src', path.ref,
                                                                      *path.path_tuple()))
        aiohttpretty.register_json_uri('GET', dir_list_first_url, body=dir_contents_first_page)

        dir_contents_next_page = json.loads(folder_contents_page_2)
        dir_list_next_url = dir_contents_first_page['next']
        aiohttpretty.register_json_uri('GET', dir_list_next_url, body=dir_contents_next_page)

        # It is not worthwhile to create fixtures for each file and to register every file history
        # url.  Simply mock ``_fetch_last_commit()`` with the same return value.  The method itself
        # is tested in its own test case: ``test_file_history_last_commit()``.
        last_commit = json.loads(file_history_last_commit)['values'][0]['commit']
        provider._fetch_last_commit = MockCoroutine(return_value=(last_commit['hash'],
                                                                  last_commit['date']))

        result = await provider.metadata(path)
        assert len(result) == 15

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_file_history_last_commit(self, provider, folder_contents_page_1,
                                            file_history_last_commit):

        dir_contents_first_page = json.loads(folder_contents_page_1)
        file_history_url_to_call = dir_contents_first_page['values'][5]['links']['history']['href']
        query_params = {'fields': 'values.commit.hash,values.commit.date'}
        file_history_url_to_register = '{}?{}'.format(file_history_url_to_call,
                                                      urlencode(query_params))
        file_history_commit_list = json.loads(file_history_last_commit)
        aiohttpretty.register_json_uri('GET', file_history_url_to_register,
                                       body=file_history_commit_list)
        commit_hash, commit_date = await provider._fetch_last_commit(file_history_url_to_call)
        assert commit_hash == file_history_commit_list['values'][0]['commit']['hash']
        assert commit_date == file_history_commit_list['values'][0]['commit']['date']


class TestDownload:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_file_download(self, provider, path_metadata_file):

        name = 'file0002.20bytes.txt'
        subdir = 'folder2-lvl1/folder1-lvl2/folder1-lvl3'
        full_path = '/{}/{}'.format(subdir, name)
        path = BitbucketPath(full_path, _ids=[(COMMIT_SHA, BRANCH) for _ in full_path.split('/')])

        file_metadata = json.loads(path_metadata_file)['non_root']
        query_params = {'format': 'meta', 'fields': 'commit.hash,commit.date,path,size'}
        path_meta_url = '{}/?{}'.format(
            provider._build_v2_repo_url('src', path.ref, *path.path_tuple()),
            urlencode(query_params)
        )
        aiohttpretty.register_json_uri('GET', path_meta_url, body=file_metadata)

        file_data = b'file0002.20bytes.txt'
        download_url = provider._build_v2_repo_url('src', path.ref, *path.path_tuple())
        aiohttpretty.register_uri('GET', download_url, body=file_data)

        result = await provider.download(path)
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
