import pytest

import aiohttpretty

from waterbutler.core import exceptions

from waterbutler.providers.bitbucket import BitbucketProvider
from waterbutler.providers.bitbucket.provider import BitbucketPath
from waterbutler.providers.bitbucket import settings as bitbucket_settings
from waterbutler.providers.bitbucket.metadata import BitbucketFileMetadata
from waterbutler.providers.bitbucket.metadata import BitbucketFolderMetadata
from waterbutler.providers.bitbucket.metadata import BitbucketRevisionMetadata

from tests.providers.bitbucket import fixtures


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
        'owner': 'fitz_cos',
        'repo': 'wb-testing',
    }


@pytest.fixture
def provider(auth, credentials, settings):
    provider = BitbucketProvider(auth, credentials, settings)
    return provider


class TestValidatePath:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_root(self, provider):
        test_fixtures = fixtures.validate_path

        default_branch_body = test_fixtures['default_branch']
        default_branch_url = provider._build_v1_repo_url('main-branch')
        aiohttpretty.register_json_uri('GET', default_branch_url, body=default_branch_body)

        try:
            wb_path_v1 = await provider.validate_v1_path('/')
        except Exception as exc:
            pytest.fail(str(exc))

        wb_path_v0 = await provider.validate_path('/')

        assert wb_path_v1 == wb_path_v0
        assert wb_path_v1.branch_name == default_branch_body['name']
        assert wb_path_v1.commit_sha == None

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('path,kind', [
        ('/foo-file.txt', 'file'),
        ('/foo-dir/',     'folder'),
    ])
    async def test_validate_v1_path(self, provider, path, kind):
        test_fixtures = fixtures.validate_path

        default_branch_body = test_fixtures['default_branch']
        default_branch = default_branch_body['name']
        default_branch_url = provider._build_v1_repo_url('main-branch')
        aiohttpretty.register_json_uri('GET', default_branch_url, body=default_branch_body)

        dir_listing_body =  test_fixtures['root_dir_listing']
        dir_listing_url = provider._build_v1_repo_url('src', default_branch)  + '/'
        aiohttpretty.register_json_uri('GET', dir_listing_url, body=dir_listing_body)

        try:
            wb_path_v1 = await provider.validate_v1_path(path)
        except Exception as exc:
            pytest.fail(str(exc))

        wb_path_v0 = await provider.validate_path(path)

        assert wb_path_v1 == wb_path_v0
        assert wb_path_v1.branch_name == default_branch
        # TODO: assert commitSha

        bad_path = path.rstrip('/') if kind == 'folder' else path + '/'
        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider.validate_v1_path(bad_path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('arg_name,arg_val,attr_name', [
        ('commitSha', 'a1b2c3d4',     'commit_sha', ),
        ('branch',    'other-branch', 'branch_name'),
        ('revision',  'bleep-blorp',  'branch_name'),
        ('revision',  '345def023ab29', 'commit_sha'),
    ])
    async def test_validate_v1_path_commit_sha(self, provider, arg_name, arg_val, attr_name):
        test_fixtures = fixtures.validate_path

        dir_listing_body =  test_fixtures['root_dir_listing']
        base_commit = dir_listing_body['node']
        dir_listing_url = provider._build_v1_repo_url('src', arg_val)  + '/'
        aiohttpretty.register_json_uri('GET', dir_listing_url, body=dir_listing_body)

        path = '/foo-file.txt'
        kwargs = {arg_name: arg_val}
        try:
            wb_path_v1 = await provider.validate_v1_path(path, **kwargs)
        except Exception as exc:
            pytest.fail(str(exc))

        ref_val = arg_val
        if attr_name == 'commit_sha' and len(arg_val) < len(base_commit):
            arg_val = base_commit
            ref_val = base_commit

        if attr_name != 'commit_sha':
            ref_val = base_commit

        commit_sha = ref_val
        branch_name = None if attr_name == 'commit_sha' else arg_val

        assert getattr(wb_path_v1, attr_name) == arg_val
        assert wb_path_v1.ref == ref_val
        assert wb_path_v1.extra == {
            'commitSha': commit_sha,
            'branchName': branch_name,
        }

        wb_path_v0 = await provider.validate_path(path, **kwargs)
        assert wb_path_v1 == wb_path_v0

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_subfolder(self, provider):
        test_fixtures = fixtures.validate_path

        dir_listing_body =  test_fixtures['subfolder_dir_listing']
        base_commit = dir_listing_body['node']
        dir_listing_url = provider._build_v1_repo_url('src', 'main-branch', 'subfolder')  + '/'
        aiohttpretty.register_json_uri('GET', dir_listing_url, body=dir_listing_body)

        path = '/subfolder/.gitkeep'
        try:
            wb_path_v1 = await provider.validate_v1_path(path, branch='main-branch')
        except Exception as exc:
            pytest.fail(str(exc))

        wb_path_v0 = await provider.validate_path(path, branch='main-branch')
        assert wb_path_v1 == wb_path_v0


class TestRevisions:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_get_revisions(self, provider):
        path = BitbucketPath('/aaa-01.txt', _ids=[('a1b2c3d4', 'master'), ('a1b2c3d4', 'master')])

        filehistory = fixtures.revisions['filehistory_complex']

        revisions_url = provider._build_v1_repo_url('filehistory', 'a1b2c3d4', 'aaa-01.txt')
        aiohttpretty.register_json_uri('GET', revisions_url, body=filehistory)

        result = await provider.revisions(path)

        assert len(result) == 4


class TestMetadata:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_get_metadata_for_file(self, provider):
        base_ref = 'a1b2c3d4'
        path = BitbucketPath('/foo-file.txt', _ids=[(base_ref, 'develop'), (base_ref, 'develop')])

        test_fixtures = fixtures.validate_path
        dir_listing_body =  test_fixtures['root_dir_listing']
        dir_listing_url = provider._build_v1_repo_url('src', base_ref)  + '/'
        aiohttpretty.register_json_uri('GET', dir_listing_url, body=dir_listing_body)

        result = await provider.metadata(path)

        assert result is not None

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_get_metadata_for_file_cached(self, provider):
        base_ref = 'a1b2c3d4'
        path = BitbucketPath('/foo-file.txt', _ids=[(base_ref, 'develop'), (base_ref, 'develop')])
        provider._parent_dir = fixtures.validate_path['root_dir_listing']
        result = await provider.metadata(path)

        assert result is not None

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_get_metadata_for_folder(self, provider):

        path = BitbucketPath('/', _ids=[(None, 'develop')], folder=True)

        test_fixtures = fixtures.validate_path
        dir_listing_body =  test_fixtures['root_dir_listing']
        dir_listing_url = provider._build_v1_repo_url('src', 'develop')  + '/'
        aiohttpretty.register_json_uri('GET', dir_listing_url, body=dir_listing_body)

        result = await provider.metadata(path)

        assert len(result) == 4


class TestDownload:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_get_metadata_for_file(self, provider):
        base_ref = 'a1b2c3d4'
        path = BitbucketPath('/foo-file.txt', _ids=[(base_ref, 'develop'), (base_ref, 'develop')])

        test_fixtures = fixtures.validate_path
        dir_listing_body =  test_fixtures['root_dir_listing']
        dir_listing_url = provider._build_v1_repo_url('src', base_ref)  + '/'
        aiohttpretty.register_json_uri('GET', dir_listing_url, body=dir_listing_body)

        download_url = provider._build_v1_repo_url('raw', path.commit_sha, *path.path_tuple())
        aiohttpretty.register_uri('GET', download_url, body=b'better')

        result = await provider.download(path)
        content = await result.response.read()
        assert content == b'better'


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
        assert provider.can_intra_move(provider) == False

    def test_can_intra_copy(self, provider):
        assert provider.can_intra_copy(provider) == False


# leftover bits
class TestMisc:

    def test_can_duplicate_name(self, provider):
        assert provider.can_duplicate_names() == False

    def test_path_from_metadata(self, provider):
        name = 'aaa-01-2.txt'
        subdir = 'plaster'
        full_path = '/{}/{}'.format(subdir, name)
        branch = 'master'
        commit_sha = '123abc456def'

        path = BitbucketPath(full_path, _ids=[
            (commit_sha, branch), (commit_sha, branch), (commit_sha, branch)
        ])

        metadata = BitbucketFileMetadata(fixtures.file_metadata, path, owner=fixtures.owner, repo=fixtures.repo)
        child_path =  provider.path_from_metadata(path.parent, metadata)

        assert child_path.full_path == path.full_path
        assert child_path == path
