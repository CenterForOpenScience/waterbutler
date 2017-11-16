import pytest

import io
import os
import copy
import furl
import json
import base64
import hashlib
from http import client

import aiohttpretty

from waterbutler.core import streams
from waterbutler.core import exceptions

from waterbutler.providers.github import GitHubProvider
from waterbutler.providers.github.path import GitHubPath
from waterbutler.providers.github.metadata import (GitHubRevision,
                                                   GitHubFileTreeMetadata,
                                                   GitHubFolderTreeMetadata,
                                                   GitHubFileContentMetadata,
                                                   GitHubFolderContentMetadata)
from waterbutler.providers.github import settings as github_settings
from waterbutler.providers.github.exceptions import GitHubUnsupportedRepoError


from tests.providers.github.fixtures import(crud_fixtures,
                                            revision_fixtures,
                                            root_provider_fixtures)


@pytest.fixture
def auth():
    return {
        'name': 'cat',
        'email': 'cat@cat.com',
    }


@pytest.fixture
def other_auth():
    return {
        'name': 'notcat',
        'email': 'notcat@notcat.com',
    }


@pytest.fixture
def credentials():
    return {'token': 'naps'}


@pytest.fixture
def other_credentials():
    return {'token': 'i\'ll have you know that I don\'t take naps. I was just resting my eyes'}


@pytest.fixture
def settings():
    return {
        'owner': 'cat',
        'repo': 'food',
    }


@pytest.fixture
def other_settings():
    return {
        'owner': 'notcat',
        'repo': 'might be food',
    }


@pytest.fixture
def file_content():
    return b'hungry'


@pytest.fixture
def file_like(file_content):
    return io.BytesIO(file_content)


@pytest.fixture
def file_stream(file_like):
    return streams.FileStreamReader(file_like)


@pytest.fixture
def provider(auth, credentials, settings, root_provider_fixtures):
    provider = GitHubProvider(auth, credentials, settings)
    provider._repo = root_provider_fixtures['repo_metadata']
    provider.default_branch = root_provider_fixtures['repo_metadata']['default_branch']
    return provider


@pytest.fixture
def other_provider(other_auth, other_credentials, other_settings, root_provider_fixtures):
    provider = GitHubProvider(other_auth, other_credentials, other_settings)
    provider._repo = root_provider_fixtures['repo_metadata']
    provider.default_branch = root_provider_fixtures['repo_metadata']['default_branch']
    return provider


class TestHelpers:

    async def test_build_repo_url(self, provider, settings):
        expected = provider.build_url('repos', settings['owner'], settings['repo'], 'contents')
        assert provider.build_repo_url('contents') == expected

    async def test_committer(self, auth, provider):
        expected = {
            'name': auth['name'],
            'email': auth['email'],
        }
        assert provider.committer == expected


class TestValidatePath:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_file(self, provider, root_provider_fixtures):
        branch_url = provider.build_repo_url('branches', provider.default_branch)
        tree_url = provider.build_repo_url(
            'git', 'trees',
            root_provider_fixtures['branch_metadata']['commit']['commit']['tree']['sha'],
            recursive=1
        )

        aiohttpretty.register_json_uri('GET', branch_url,
                body=root_provider_fixtures['branch_metadata'])
        aiohttpretty.register_json_uri(
            'GET', tree_url, body=root_provider_fixtures['repo_tree_metadata_root']
        )

        blob_path = 'file.txt'

        result = await provider.validate_v1_path('/' + blob_path)

        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider.validate_v1_path('/' + blob_path + '/')

        expected = GitHubPath('/' + blob_path, _ids=[(provider.default_branch, '')])

        assert exc.value.code == client.NOT_FOUND
        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_root(self, provider):
        path = '/'

        result = await provider.validate_v1_path(path, branch=provider.default_branch)
        no_branch_result = await provider.validate_v1_path(path)

        expected = GitHubPath(path, _ids=[(provider.default_branch, '')])

        assert result == expected
        assert expected == no_branch_result

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_folder(self, provider, root_provider_fixtures):
        branch_url = provider.build_repo_url('branches', provider.default_branch)
        tree_url = provider.build_repo_url(
            'git', 'trees',
            root_provider_fixtures['branch_metadata']['commit']['commit']['tree']['sha'],
            recursive=1
        )

        aiohttpretty.register_json_uri(
            'GET', branch_url, body=root_provider_fixtures['branch_metadata']
        )
        aiohttpretty.register_json_uri(
            'GET', tree_url, body=root_provider_fixtures['repo_tree_metadata_root']
        )

        tree_path = 'level1'

        expected = GitHubPath(
            '/' + tree_path + '/', _ids=[(provider.default_branch, ''),
            (provider.default_branch, None)]
        )

        result = await provider.validate_v1_path('/' + tree_path + '/')

        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider.validate_v1_path('/' + tree_path)

        assert exc.value.code == client.NOT_FOUND
        assert result == expected
        assert result.extra == expected.extra

    @pytest.mark.asyncio
    async def test_reject_multiargs(self, provider):

        with pytest.raises(exceptions.InvalidParameters) as exc:
            await provider.validate_v1_path('/foo', ref=['bar', 'baz'])

        assert exc.value.code == client.BAD_REQUEST

        with pytest.raises(exceptions.InvalidParameters) as exc:
            await provider.validate_path('/foo', ref=['bar', 'baz'])

        assert exc.value.code == client.BAD_REQUEST

    @pytest.mark.asyncio
    async def test_validate_path(self, provider):
        path = await provider.validate_path('/this/is/my/path')

        assert path.is_dir is False
        assert path.is_file is True
        assert path.name == 'path'
        assert isinstance(path.identifier, tuple)
        assert path.identifier == (provider.default_branch, None)
        assert path.parts[0].identifier == (provider.default_branch, None)

    @pytest.mark.asyncio
    async def test_validate_path_passes_branch(self, provider):
        path = await provider.validate_path('/this/is/my/path', branch='NotMaster')

        assert path.is_dir is False
        assert path.is_file is True
        assert path.name == 'path'
        assert isinstance(path.identifier, tuple)
        assert path.identifier == ('NotMaster', None)
        assert path.parts[0].identifier == ('NotMaster', None)

    @pytest.mark.asyncio
    async def test_validate_path_passes_ref(self, provider):
        path = await provider.validate_path('/this/is/my/path', ref='NotMaster')

        assert path.is_dir is False
        assert path.is_file is True
        assert path.name == 'path'
        assert isinstance(path.identifier, tuple)
        assert path.identifier == ('NotMaster', None)
        assert path.parts[0].identifier == ('NotMaster', None)

    @pytest.mark.asyncio
    async def test_validate_path_passes_file_sha(self, provider):
        path = await provider.validate_path('/this/is/my/path', fileSha='Thisisasha')

        assert path.is_dir is False
        assert path.is_file is True
        assert path.name == 'path'
        assert isinstance(path.identifier, tuple)
        assert path.identifier == (provider.default_branch, 'Thisisasha')
        assert path.parts[0].identifier == (provider.default_branch, None)

    @pytest.mark.asyncio
    async def test_revalidate_path(self, provider):
        path = '/'
        child_path = 'grass.txt'
        github_path = GitHubPath(path, _ids=[(provider.default_branch, '')])

        result = await provider.revalidate_path(github_path, child_path)

        assert result.path == child_path


class TestCRUD:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_by_path(self, provider, root_provider_fixtures):
        ref = hashlib.sha1().hexdigest()
        file_sha = root_provider_fixtures['repo_tree_metadata_root']['tree'][0]['sha']
        path = GitHubPath(
            '/file.txt', _ids=[(provider.default_branch, ''), (provider.default_branch, '')]
        )

        url = provider.build_repo_url('git', 'blobs', file_sha)
        tree_url = provider.build_repo_url('git', 'trees', ref, recursive=1)
        latest_sha_url = provider.build_repo_url('git', 'refs', 'heads', path.identifier[0])
        commit_url = provider.build_repo_url(
            'commits', path=path.path.lstrip('/'), sha=path.identifier[0]
        )

        aiohttpretty.register_uri('GET', url, body=b'delicious')
        aiohttpretty.register_json_uri(
            'GET', tree_url, body=root_provider_fixtures['repo_tree_metadata_root']
        )

        aiohttpretty.register_json_uri('GET', commit_url, body=[{'commit': {'tree': {'sha': ref}}}])

        result = await provider.download(path)
        content = await result.read()

        assert content == b'delicious'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_by_path_ref_branch(self, provider, root_provider_fixtures):
        ref = hashlib.sha1().hexdigest()
        file_sha = root_provider_fixtures['repo_tree_metadata_root']['tree'][0]['sha']
        path = GitHubPath('/file.txt', _ids=[(provider.default_branch, ''), ('other_branch', '')])

        url = provider.build_repo_url('git', 'blobs', file_sha)
        tree_url = provider.build_repo_url('git', 'trees', ref, recursive=1)
        commit_url = provider.build_repo_url(
            'commits', path=path.path.lstrip('/'), sha=path.identifier[0]
        )

        aiohttpretty.register_uri('GET', url, body=b'delicious')
        aiohttpretty.register_json_uri(
            'GET', tree_url, body=root_provider_fixtures['repo_tree_metadata_root']
        )

        aiohttpretty.register_json_uri('GET', commit_url, body=[{'commit': {'tree': {'sha': ref}}}])

        result = await provider.download(path)
        content = await result.read()

        assert content == b'delicious'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_by_path_revision(self, provider, root_provider_fixtures):
        ref = hashlib.sha1().hexdigest()
        file_sha = root_provider_fixtures['repo_tree_metadata_root']['tree'][0]['sha']
        path = GitHubPath('/file.txt', _ids=[(provider.default_branch, ''), ('other_branch', '')])

        url = provider.build_repo_url('git', 'blobs', file_sha)
        tree_url = provider.build_repo_url('git', 'trees', ref, recursive=1)
        commit_url = provider.build_repo_url(
            'commits', path=path.path.lstrip('/'), sha='Just a test'
        )

        aiohttpretty.register_uri('GET', url, body=b'delicious')
        aiohttpretty.register_json_uri(
            'GET', tree_url, body=root_provider_fixtures['repo_tree_metadata_root']
        )
        aiohttpretty.register_json_uri('GET', commit_url, body=[{'commit': {'tree': {'sha': ref}}}])

        result = await provider.download(path, revision='Just a test')
        content = await result.read()

        assert content == b'delicious'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_create(self, provider, root_provider_fixtures,
                                 crud_fixtures, file_content, file_stream):
        message = 'so hungry'
        item = root_provider_fixtures['upload_response']
        path = GitHubPath(
            '/' + item['content']['path'],
            _ids=[(provider.default_branch, ''), ('master', ''), ('master', '')]
        )

        commit_url = provider.build_repo_url('commits', path=path.path, sha=path.branch_ref)
        sha_url = provider.build_repo_url('git', 'refs', 'heads', path.branch_ref)
        blob_url = provider.build_repo_url('git', 'blobs')
        create_tree_url = provider.build_repo_url('git', 'trees')
        create_commit_url = provider.build_repo_url('git', 'commits')

        aiohttpretty.register_json_uri('GET', commit_url, status=404)
        aiohttpretty.register_json_uri('GET', sha_url, body=crud_fixtures['latest_sha_metadata'])
        aiohttpretty.register_json_uri(
            'POST', blob_url, body=crud_fixtures['blob_data'], status=201
        )

        aiohttpretty.register_json_uri(
            'POST', sha_url, body=crud_fixtures['latest_sha_metadata'], status=200
        )

        aiohttpretty.register_json_uri(
            'POST', create_tree_url,
            body=root_provider_fixtures['repo_tree_metadata_root'], status=201
        )

        aiohttpretty.register_json_uri(
            'POST', create_commit_url, status=201,
            body=root_provider_fixtures['new_head_commit_metadata']
        )

        result = await provider.upload(file_stream, path, message)

        expected = GitHubFileTreeMetadata({
            'path': path.path,
            'sha': crud_fixtures['blob_data']['sha'],
            'size': file_stream.size,
        }, commit=root_provider_fixtures['new_head_commit_metadata'], ref=path.branch_ref), True

        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_update(self, provider, root_provider_fixtures,
                                 crud_fixtures, file_content, file_stream):
        message = 'so hungry'
        path = GitHubPath('/file.txt', _ids=[(provider.default_branch, ''), ('master', '')])
        tree_meta = root_provider_fixtures['repo_tree_metadata_root']
        commit_meta = crud_fixtures['all_commits_metadata']

        tree_url = furl.furl(
            provider.build_repo_url('git', 'trees', commit_meta[0]['commit']['tree']['sha'])
        )

        tree_url.args.update({'recursive': 1})

        commit_url = provider.build_repo_url('commits', path=path.path, sha=path.branch_ref)
        sha_url = provider.build_repo_url('git', 'refs', 'heads', path.branch_ref)
        blob_url = provider.build_repo_url('git', 'blobs')
        create_tree_url = provider.build_repo_url('git', 'trees')
        blob_tree_url = provider.build_repo_url(
            'git', 'trees') + '/{}:?recursive=99999'.format(path.branch_ref)

        aiohttpretty.register_json_uri(
            'GET', commit_url, body=crud_fixtures['all_commits_metadata'], status=200
        )

        aiohttpretty.register_json_uri('GET', tree_url, body=tree_meta)
        aiohttpretty.register_json_uri('GET', sha_url, body=crud_fixtures['latest_sha_metadata'])
        aiohttpretty.register_json_uri(
            'POST', blob_url, body=crud_fixtures['blob_data'], status=201
        )

        aiohttpretty.register_json_uri(
            'GET', blob_tree_url, body=crud_fixtures['crud_repo_tree_metadata_root']
        )

        aiohttpretty.register_json_uri(
            'POST', create_tree_url,
            body=crud_fixtures['crud_repo_tree_metadata_root'], status=201
        )

        result = await provider.upload(file_stream, path, message)

        expected = GitHubFileTreeMetadata({
            'path': path.path,
            'sha': crud_fixtures['blob_data']['sha'],
            'size': file_stream.size,
        }, ref=path.branch_ref), False

        assert result[0].serialized() == expected[0].serialized()

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_empty_repo(self, provider, root_provider_fixtures,
                                     crud_fixtures, file_content, file_stream):
        message = 'so hungry'
        item = root_provider_fixtures['upload_response']
        path = GitHubPath(
            '/' + item['content']['path'],
            _ids=[(provider.default_branch, ''), ('master', ''), ('master', '')]
        )

        commit_url = provider.build_repo_url('commits', path=path.path, sha=path.branch_ref)
        sha_url = provider.build_repo_url('git', 'refs', 'heads', path.branch_ref)
        blob_url = provider.build_repo_url('git', 'blobs')
        create_tree_url = provider.build_repo_url('git', 'trees')
        create_commit_url = provider.build_repo_url('git', 'commits')
        git_keep_url = provider.build_repo_url('contents', '.gitkeep')

        aiohttpretty.register_json_uri(
            'GET', commit_url,
            body={
                "message": "Git Repository is empty.",
                "documentation_url": "https://developer.github.com/v3"
            },
            status=409
        )

        aiohttpretty.register_json_uri('GET', sha_url, body=crud_fixtures['latest_sha_metadata'])
        aiohttpretty.register_json_uri(
            'POST', blob_url, body=crud_fixtures['blob_data'], status=201
        )

        aiohttpretty.register_json_uri(
            'POST', create_tree_url,
            body=root_provider_fixtures['repo_tree_metadata_root'], status=201
        )

        aiohttpretty.register_json_uri(
            'POST', create_commit_url,
            body=root_provider_fixtures['new_head_commit_metadata'], status=201
        )

        aiohttpretty.register_json_uri(
            'POST', sha_url, body=crud_fixtures['latest_sha_metadata'], status=200
        )

        aiohttpretty.register_json_uri(
            'PUT', git_keep_url, body=root_provider_fixtures['branch_metadata'], status=201
        )

        result = await provider.upload(file_stream, path, message)

        expected = GitHubFileTreeMetadata({
            'path': path.path,
            'sha': crud_fixtures['blob_data']['sha'],
            'size': file_stream.size,
        }, commit=root_provider_fixtures['new_head_commit_metadata'], ref=path.branch_ref), True

        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_checksum_mismatch(self, provider, root_provider_fixtures,
                                            crud_fixtures, file_content, file_stream):
        item = root_provider_fixtures['upload_response']
        path = GitHubPath(
            '/' + item['content']['path'],
            _ids=[(provider.default_branch, ''), ('master', ''), ('master', '')]
        )

        commit_url = provider.build_repo_url('commits', path=path.path, sha=path.branch_ref)
        sha_url = provider.build_repo_url('git', 'refs', 'heads', path.branch_ref)
        blob_url = provider.build_repo_url('git', 'blobs')

        aiohttpretty.register_json_uri('GET', commit_url, status=404)
        aiohttpretty.register_json_uri('GET', sha_url, body=crud_fixtures['latest_sha_metadata'])
        aiohttpretty.register_json_uri(
            'POST', blob_url, body=crud_fixtures['checksum_mismatch_blob_data'], status=201
        )

        with pytest.raises(exceptions.UploadChecksumMismatchError) as exc:
            await provider.upload(file_stream, path)

        assert aiohttpretty.has_call(method='GET', uri=commit_url)
        assert aiohttpretty.has_call(method='GET', uri=sha_url)
        assert aiohttpretty.has_call(method='POST', uri=blob_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_root_no_confirm(self, provider, root_provider_fixtures):
        path = GitHubPath('/', _ids=[('master', '')])

        with pytest.raises(exceptions.DeleteError) as e:
            await provider.delete(path)

        assert e.value.code == 400
        assert e.value.message == 'confirm_delete=1 is required for deleting root provider folder'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_root(self, provider, crud_fixtures):
        path = GitHubPath('/', _ids=[('master', '')])

        branch_url = provider.build_repo_url('branches', 'master')
        commit_url = provider.build_repo_url('git', 'commits')
        patch_url = provider.build_repo_url('git', 'refs', 'heads', path.branch_ref)

        aiohttpretty.register_json_uri(
            'GET', branch_url, body=crud_fixtures['deleted_branch_metadata']
        )

        aiohttpretty.register_json_uri(
            'POST', commit_url, body=crud_fixtures['deleted_commit_metadata'], status=201
        )

        aiohttpretty.register_json_uri('PATCH', patch_url)

        await provider.delete(path, confirm_delete=1)

        assert aiohttpretty.has_call(method='PATCH', uri=patch_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_file_with_sha(self, provider, root_provider_fixtures):
        item = root_provider_fixtures['upload_response']
        sha = item['content']['sha']
        path = GitHubPath('/file.txt', _ids=[('master', sha), ('master', sha)])
        branch = 'master'
        message = 'deleted'

        url = provider.build_repo_url('contents', path.path)

        aiohttpretty.register_json_uri('DELETE', url)

        await provider.delete(path, sha, message, branch=branch)

        assert aiohttpretty.has_call(method='DELETE', uri=url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_file_no_sha(self, provider, root_provider_fixtures, crud_fixtures):
        item = root_provider_fixtures['upload_response']
        sha = item['content']['sha']
        path = GitHubPath('/file.txt', _ids=[('master', ''), ('master', '')])
        branch = 'master'
        message = 'deleted'

        url = provider.build_repo_url('contents', path.path)
        commit_url = provider.build_repo_url('commits', path=path.path, sha=path.branch_ref)
        tree_url = furl.furl(provider.build_repo_url(
            'git', 'trees', crud_fixtures['all_commits_metadata'][0]['commit']['tree']['sha'])
        )

        tree_url.args.update({'recursive': 1})
        aiohttpretty.register_json_uri(
            'GET', tree_url, body=root_provider_fixtures['repo_tree_metadata_root']
        )

        aiohttpretty.register_json_uri('DELETE', url)
        aiohttpretty.register_json_uri(
            'GET', commit_url, body=crud_fixtures['all_commits_metadata']
        )

        await provider.delete(path, sha, message, branch=branch)

        assert aiohttpretty.has_call(method='DELETE', uri=url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_folder(self, provider, root_provider_fixtures, crud_fixtures):
        sha = crud_fixtures['deleted_tree_metadata']['tree'][2]['sha']
        path = GitHubPath('/deletedfolder/', _ids=[('master', sha), ('master', sha)])
        branch = 'master'
        message = 'deleted'

        branch_url = provider.build_repo_url('branches', 'master')
        tree_url = furl.furl(
            provider.build_repo_url(
                'git', 'trees',
                crud_fixtures['deleted_branch_metadata']['commit']['commit']['tree']['sha']
            )
        )

        create_tree_url = provider.build_repo_url('git', 'trees')
        commit_url = provider.build_repo_url('git', 'commits')
        patch_url = provider.build_repo_url('git', 'refs', 'heads', path.branch_ref)

        aiohttpretty.register_json_uri(
            'GET', branch_url, body=crud_fixtures['deleted_branch_metadata']
        )

        aiohttpretty.register_json_uri('GET', tree_url, body=crud_fixtures['deleted_tree_metadata'])
        aiohttpretty.register_json_uri(
            'POST', create_tree_url, body=crud_fixtures['deleted_tree_metadata_2'], status=201
        )

        aiohttpretty.register_json_uri(
            'POST', commit_url, body=crud_fixtures['deleted_commit_metadata'], status=201
        )

        aiohttpretty.register_json_uri('PATCH', patch_url)

        await provider.delete(path, sha, message, branch=branch)

        assert aiohttpretty.has_call(method='PATCH', uri=patch_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_folder_error_case(self, provider, root_provider_fixtures, crud_fixtures):
        sha = crud_fixtures['deleted_tree_metadata']['tree'][2]['sha']
        path = GitHubPath('/deletedfolder/', _ids=[('master', sha), ('master', sha)])
        branch = 'master'
        message = 'deleted'

        branch_url = provider.build_repo_url('branches', 'master')
        tree_url = furl.furl(provider.build_repo_url(
            'git', 'trees',
            crud_fixtures['deleted_branch_metadata']['commit']['commit']['tree']['sha'])
        )

        create_tree_url = provider.build_repo_url('git', 'trees')
        commit_url = provider.build_repo_url('git', 'commits')
        patch_url = provider.build_repo_url('git', 'refs', 'heads', path.branch_ref)

        aiohttpretty.register_json_uri(
            'GET', branch_url, body=crud_fixtures['deleted_branch_metadata']
        )

        aiohttpretty.register_json_uri(
            'GET', tree_url, body=crud_fixtures['deleted_tree_metadata_2']
        )

        aiohttpretty.register_json_uri(
            'POST', create_tree_url, body=crud_fixtures['deleted_tree_metadata_2'], status=201
        )

        with pytest.raises(exceptions.NotFoundError) as e:
            await provider.delete(path, sha, message, branch=branch)

        assert e.value.code == 404
        assert e.value.message == "Could not retrieve file or directory /deletedfolder/"

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_subfolder(self, provider, crud_fixtures):
        sha = crud_fixtures['deleted_tree_metadata']['tree'][2]['sha']
        path = GitHubPath(
            '/folder1/deletedfolder/', _ids=[('master', sha), ('master', sha), ('master', sha)]
        )
        branch = 'master'
        message = 'deleted'

        branch_url = provider.build_repo_url('branches', 'master')
        tree_url = furl.furl(
            provider.build_repo_url('git', 'trees',
            crud_fixtures['deleted_subfolder_branch_metadata']['commit']['commit']['tree']['sha']
            )
        )

        idx_tree_url = furl.furl(
            provider.build_repo_url('git', 'trees',
            crud_fixtures['deleted_subfolder_main_tree_metadata']['tree'][3]['sha']
            )
        )

        create_tree_url = provider.build_repo_url('git', 'trees')
        commit_url = provider.build_repo_url('git', 'commits')
        patch_url = provider.build_repo_url('git', 'refs', 'heads', path.branch_ref)

        aiohttpretty.register_json_uri(
            'GET', branch_url, body=crud_fixtures['deleted_subfolder_branch_metadata']
        )

        aiohttpretty.register_json_uri(
            'GET', tree_url, body=crud_fixtures['deleted_subfolder_main_tree_metadata']
        )

        aiohttpretty.register_json_uri(
            'GET', idx_tree_url, body=crud_fixtures['deleted_subfolder_idx_tree_metadata']
        )

        aiohttpretty.register_json_uri('POST', create_tree_url, **{
            "responses": [
                {'body': json.dumps(
                    crud_fixtures['deleted_subfolder_tree_data_1']).encode('utf-8'), 'status': 201},
                {'body': json.dumps(
                    crud_fixtures['deleted_subfolder_tree_data_2']).encode('utf-8'), 'status': 201},
            ]})

        aiohttpretty.register_json_uri(
            'POST', commit_url, body=crud_fixtures['deleted_subfolder_commit_metadata'], status=201
        )

        aiohttpretty.register_json_uri('PATCH', patch_url)

        await provider.delete(path, sha, message, branch=branch)

        assert aiohttpretty.has_call(method='PATCH', uri=patch_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_subfolder_stop_iteration_error(self, provider, crud_fixtures):
        sha = crud_fixtures['deleted_tree_metadata']['tree'][2]['sha']
        path = GitHubPath(
            '/folder1/deletedfolder/', _ids=[('master', sha), ('master', sha), ('master', sha)]
        )
        branch = 'master'
        message = 'deleted'

        branch_url = provider.build_repo_url('branches', 'master')
        tree_url = furl.furl(
            provider.build_repo_url('git', 'trees',
            crud_fixtures['deleted_subfolder_branch_metadata']['commit']['commit']['tree']['sha']
            )
        )

        idx_tree_url = furl.furl(
            provider.build_repo_url('git', 'trees',
            crud_fixtures['deleted_subfolder_main_tree_metadata']['tree'][3]['sha']
            )
        )

        create_tree_url = provider.build_repo_url('git', 'trees')
        commit_url = provider.build_repo_url('git', 'commits')
        patch_url = provider.build_repo_url('git', 'refs', 'heads', path.branch_ref)

        aiohttpretty.register_json_uri(
            'GET', branch_url, body=crud_fixtures['deleted_subfolder_branch_metadata']
        )

        aiohttpretty.register_json_uri(
            'GET', tree_url, body=crud_fixtures['deleted_subfolder_bad_tree_metadata']
        )

        with pytest.raises(exceptions.MetadataError) as e:
            await provider.delete(path, sha, message, branch=branch)

        assert e.value.code == 404
        assert e.value.message == 'Could not delete folder \'{0}\''.format(path)


class TestMetadata:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file(self, provider, root_provider_fixtures):
        ref = hashlib.sha1().hexdigest()
        path = GitHubPath('/file.txt', _ids=[(provider.default_branch, ''), ('other_branch', '')])

        tree_url = provider.build_repo_url('git', 'trees', ref, recursive=1)
        commit_url = provider.build_repo_url(
            'commits', path=path.path.lstrip('/'), sha=path.identifier[0]
        )

        aiohttpretty.register_json_uri(
            'GET', tree_url, body=root_provider_fixtures['repo_tree_metadata_root']
        )

        aiohttpretty.register_json_uri('GET', commit_url, body=[{
            'commit': {
                'tree': {'sha': ref},
                'author': {'date': '1970-01-02T03:04:05Z'}
            },
        }])

        result = await provider.metadata(path)
        item = root_provider_fixtures['repo_tree_metadata_root']['tree'][0]
        web_view = provider._web_view(path=path)

        assert result == GitHubFileTreeMetadata(item, web_view=web_view, commit={
            'tree': {'sha': ref}, 'author': {'date': '1970-01-02T03:04:05Z'}
        }, ref=path.identifier[0])

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file_error(self, provider, root_provider_fixtures):
        path = GitHubPath(
            '/file.txt', _ids=[(provider.default_branch, ''), (provider.default_branch, '')]
        )

        tree_url = provider.build_repo_url('git', 'trees', path.branch_ref, recursive=1)
        commit_url = provider.build_repo_url(
            'commits', path=path.path.lstrip('/'), sha=path.identifier[0]
        )

        aiohttpretty.register_json_uri('GET', tree_url, body={'tree': [], 'truncated': False})
        aiohttpretty.register_json_uri('GET', commit_url, body=[{
            'commit': {
                'tree': {'sha': path.branch_ref},
                'author': {'date': '1970-01-02T03:04:05Z'}
            },
        }])

        with pytest.raises(exceptions.NotFoundError) as e:
            await provider.metadata(path)

        assert e.value.code == 404
        assert e.value.message == 'Could not retrieve file or directory {0}'.format(str(path))

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_doesnt_exist(self, provider, root_provider_fixtures):
        ref = hashlib.sha1().hexdigest()
        path = GitHubPath('/file.txt', _ids=[(provider.default_branch, ''), ('other_branch', '')])

        tree_url = provider.build_repo_url('git', 'trees', ref, recursive=1)
        commit_url = provider.build_repo_url(
            'commits', path=path.path.lstrip('/'), sha=path.identifier[0]
        )

        aiohttpretty.register_json_uri(
            'GET', tree_url, body=root_provider_fixtures['repo_tree_metadata_root']
        )

        aiohttpretty.register_json_uri('GET', commit_url, body=[])

        with pytest.raises(exceptions.NotFoundError):
            await provider.metadata(path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder_root(self, provider, root_provider_fixtures):
        path = GitHubPath('/', _ids=[(provider.default_branch, '')])

        url = provider.build_repo_url('contents', path.path, ref=provider.default_branch)
        aiohttpretty.register_json_uri(
            'GET', url, body=root_provider_fixtures['content_repo_metadata_root']
        )

        result = await provider.metadata(path)

        ret = []
        for item in root_provider_fixtures['content_repo_metadata_root']:
            if item['type'] == 'dir':
                ret.append(GitHubFolderContentMetadata(item, ref=provider.default_branch))
            else:
                ret.append(
                    GitHubFileContentMetadata(
                        item, web_view=item['html_url'], ref=provider.default_branch
                    )
                )

        assert result == ret

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_path_from_metadata(self, provider, root_provider_fixtures):
        path = GitHubPath('/', _ids=[(provider.default_branch, '')])
        item = root_provider_fixtures['content_repo_metadata_root'][0]
        metadata = GitHubFileContentMetadata(
            item, web_view=item['html_url'], ref=provider.default_branch
        )

        result = provider.path_from_metadata(path, metadata)

        assert result.path == item['path']
        # note, more asserst here?

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder_fetch_error(self, provider, root_provider_fixtures):
        path = GitHubPath(
            '/test/', _ids=[(provider.default_branch, ''), (provider.default_branch, '')]
        )
        url = furl.furl(provider.build_repo_url('contents', '/test/'))
        url.args.update({'ref': path.branch_ref})

        message = 'This repository is not empty.'

        aiohttpretty.register_json_uri('GET', url, body={
            'message': message
        }, status=404)

        with pytest.raises(exceptions.MetadataError) as e:
            await provider.metadata(path)

        assert e.value.code == 404

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder_error_empty_repo(self, provider, root_provider_fixtures):
        path = GitHubPath(
            '/test/', _ids=[(provider.default_branch, ''), (provider.default_branch, '')]
        )
        url = furl.furl(provider.build_repo_url('contents', '/test/'))
        url.args.update({'ref': path.branch_ref})
        message = 'This repository is empty.'
        aiohttpretty.register_json_uri('GET', url, body={
            'message': message
        }, status=404)

        result = await provider.metadata(path)
        expected = []

        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder_error_dict_return(self, provider, root_provider_fixtures):
        path = GitHubPath(
            '/test/', _ids=[(provider.default_branch, ''), (provider.default_branch, '')]
        )
        url = furl.furl(provider.build_repo_url('contents', '/test/'))
        url.args.update({'ref': path.branch_ref})

        message = 'This repository is empty.'

        aiohttpretty.register_json_uri('GET', url, body={})

        with pytest.raises(exceptions.MetadataError) as e:
            await provider.metadata(path)

        assert e.value.code == 404
        assert e.value.message == 'Could not retrieve folder "{0}"'.format(str(path))

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_revision_metadata(self, provider, revision_fixtures):
        metadata = revision_fixtures['revision_metadata']
        path = GitHubPath(
            '/file.txt', _ids=[("master", metadata[0]['sha']), ('master', metadata[0]['sha'])]
        )

        url = provider.build_repo_url('commits', path=path.path, sha=path.file_sha)

        aiohttpretty.register_json_uri('GET', url, body=metadata)

        result = await provider.revisions(path)

        expected = [
            GitHubRevision(item)
            for item in metadata
        ]

        assert result == expected


class TestIntra:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_copy_file(self, provider, root_provider_fixtures):
        branch_meta = root_provider_fixtures['branch_metadata']
        tree_meta = root_provider_fixtures['repo_tree_metadata_root']
        new_tree_meta = root_provider_fixtures['repo_tree_metadata_root_updated']
        src_path = GitHubPath('/file.txt', _ids=[("master", ''), (branch_meta['name'], '')])
        dest_path = GitHubPath(
            '/truefacts/file.txt',
            _ids=[
                ("master", ''), (branch_meta['name'], ''), (branch_meta['name'], '')
            ])

        tree_url = furl.furl(
            provider.build_repo_url('git', 'trees', branch_meta['commit']['commit']['tree']['sha'])
        )

        tree_url.args.update({'recursive': 1})

        branch_url = provider.build_repo_url('branches', branch_meta['name'])

        create_tree_url = provider.build_repo_url('git', 'trees')
        commit_url = provider.build_repo_url('git', 'commits')

        headers = {'Content-Type': 'application/json'}
        update_ref_url = provider.build_repo_url('git', 'refs', 'heads', src_path.branch_ref)

        aiohttpretty.register_json_uri(
            'POST', commit_url, headers=headers,
            body=root_provider_fixtures['new_head_commit_metadata'], status=201
        )

        aiohttpretty.register_json_uri(
            'POST', create_tree_url, headers=headers, body=new_tree_meta, status=201
        )

        aiohttpretty.register_json_uri('POST', update_ref_url)
        aiohttpretty.register_json_uri('GET', branch_url, body=branch_meta)
        aiohttpretty.register_json_uri('GET', tree_url, body=tree_meta)

        result = await provider.intra_copy(provider, src_path, dest_path)

        blobs = [new_tree_meta['tree'][0]]
        blobs[0]['path'] = dest_path.path
        commit = root_provider_fixtures['new_head_commit_metadata']
        expected = (GitHubFileTreeMetadata(
            blobs[0], commit=commit, ref=dest_path.branch_ref
        ), True)

        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_copy_file_no_commit(self, provider, root_provider_fixtures):
        branch_meta = root_provider_fixtures['branch_metadata']
        tree_meta = root_provider_fixtures['repo_tree_metadata_root']
        src_path = GitHubPath('/file.txt', _ids=[("master", ''), (branch_meta['name'], '')])
        dest_path = GitHubPath(
            '/truefacts/file.txt',
            _ids=[
                ("master", ''), (branch_meta['name'], ''), (branch_meta['name'], '')
            ])

        tree_url = furl.furl(
            provider.build_repo_url('git', 'trees', branch_meta['commit']['commit']['tree']['sha'])
        )

        tree_url.args.update({'recursive': 1})
        branch_url = provider.build_repo_url('branches', branch_meta['name'])
        create_tree_url = provider.build_repo_url('git', 'trees')
        commit_url = provider.build_repo_url('git', 'commits')

        headers = {'Content-Type': 'application/json'}
        update_ref_url = provider.build_repo_url('git', 'refs', 'heads', src_path.branch_ref)
        aiohttpretty.register_json_uri(
            'POST', commit_url, headers=headers,
            body=root_provider_fixtures['new_head_commit_metadata'], status=201
        )

        aiohttpretty.register_json_uri(
            'POST', create_tree_url, headers=headers, body=tree_meta, status=201
        )

        aiohttpretty.register_json_uri('POST', update_ref_url)
        aiohttpretty.register_json_uri('GET', branch_url, body=branch_meta)
        aiohttpretty.register_json_uri('GET', tree_url, body=tree_meta)

        result = await provider.intra_copy(provider, src_path, dest_path)

        blobs = [tree_meta['tree'][0]]
        blobs[0]['path'] = dest_path.path
        commit = None
        expected = (GitHubFileTreeMetadata(
            blobs[0], commit=commit, ref=dest_path.branch_ref
        ), True)

        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_copy_and_move_folder(self, provider, root_provider_fixtures):
        branch_meta = root_provider_fixtures['branch_metadata']
        tree_meta = root_provider_fixtures['repo_tree_metadata_root_with_folder']
        new_tree_meta = root_provider_fixtures['repo_tree_metadata_root_with_folder_updated']
        src_path = GitHubPath('/file/', _ids=[("master", ''), (branch_meta['name'], '')])
        dest_path = GitHubPath(
            '/truefacts/file/',
            _ids=[
                ("master", ''), (branch_meta['name'], ''), (branch_meta['name'], '')
            ])

        tree_url = furl.furl(
            provider.build_repo_url('git', 'trees', branch_meta['commit']['commit']['tree']['sha'])
        )

        tree_url.args.update({'recursive': 1})
        branch_url = provider.build_repo_url('branches', branch_meta['name'])
        create_tree_url = provider.build_repo_url('git', 'trees')
        commit_url = provider.build_repo_url('git', 'commits')
        headers = {'Content-Type': 'application/json'}
        update_ref_url = provider.build_repo_url('git', 'refs', 'heads', src_path.branch_ref)

        aiohttpretty.register_json_uri(
            'POST', commit_url, headers=headers,
            body=root_provider_fixtures['new_head_commit_metadata'], status=201
        )

        aiohttpretty.register_json_uri(
            'POST', create_tree_url, headers=headers, body=new_tree_meta, status=201
        )

        aiohttpretty.register_json_uri('POST', update_ref_url)
        aiohttpretty.register_json_uri('GET', branch_url, body=branch_meta)
        aiohttpretty.register_json_uri('GET', tree_url, body=tree_meta)

        result = await provider.intra_copy(provider, src_path, dest_path)
        other_result = await provider.intra_move(provider, src_path, dest_path)

        blobs = new_tree_meta['tree'][:3]
        provider._reparent_blobs(blobs, src_path, dest_path)
        commit = root_provider_fixtures['new_head_commit_metadata']

        expected = GitHubFolderTreeMetadata({
            'path': dest_path.path.strip('/')
        }, commit=commit, ref=dest_path.branch_ref)

        expected.children = []

        for item in blobs:
            if item['path'] == dest_path.path.rstrip('/'):
                continue
            if item['type'] == 'tree':
                expected.children.append(GitHubFolderTreeMetadata(item, ref=dest_path.branch_ref))
            else:
                expected.children.append(GitHubFileTreeMetadata(item, ref=dest_path.branch_ref))

        assert result == (expected, True) == other_result

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_copy_not_found_error(self, provider, root_provider_fixtures):
        branch_meta = root_provider_fixtures['branch_metadata']
        tree_meta = root_provider_fixtures['repo_tree_metadata_root_with_folder']
        src_path = GitHubPath('/filenotfound.txt', _ids=[("master", ''), (branch_meta['name'], '')])
        dest_path = GitHubPath(
            '/truefacts/filenotfound.txt',
            _ids=[
                ("master", ''), (branch_meta['name'], ''), (branch_meta['name'], '')
            ])

        tree_url = furl.furl(
            provider.build_repo_url('git', 'trees', branch_meta['commit']['commit']['tree']['sha'])
        )

        tree_url.args.update({'recursive': 1})
        branch_url = provider.build_repo_url('branches', branch_meta['name'])

        aiohttpretty.register_json_uri('GET', branch_url, body=branch_meta)
        aiohttpretty.register_json_uri('GET', tree_url, body=tree_meta)

        with pytest.raises(exceptions.NotFoundError) as e:
            await provider.intra_copy(provider, src_path, dest_path)

        assert e.value.code == 404
        assert e.value.message == 'Could not retrieve file or directory ' + '/' + src_path.path

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_move_file(self, provider, root_provider_fixtures):
        branch_meta = root_provider_fixtures['branch_metadata']
        tree_meta = root_provider_fixtures['repo_tree_metadata_root']
        new_tree_meta = root_provider_fixtures['repo_tree_metadata_root_updated']
        src_path = GitHubPath('/file.txt', _ids=[("master", ''), (branch_meta['name'], '')])
        dest_path = GitHubPath(
            '/truefacts/file.txt',
            _ids=[
                ("master", ''), (branch_meta['name'], ''), (branch_meta['name'], '')
            ])

        tree_url = furl.furl(
            provider.build_repo_url('git', 'trees', branch_meta['commit']['commit']['tree']['sha'])
        )

        tree_url.args.update({'recursive': 1})
        branch_url = provider.build_repo_url('branches', branch_meta['name'])
        create_tree_url = provider.build_repo_url('git', 'trees')
        commit_url = provider.build_repo_url('git', 'commits')
        headers = {'Content-Type': 'application/json'}
        update_ref_url = provider.build_repo_url('git', 'refs', 'heads', src_path.branch_ref)

        aiohttpretty.register_json_uri(
            'POST', commit_url, headers=headers,
            body=root_provider_fixtures['new_head_commit_metadata'], status=201
        )

        aiohttpretty.register_json_uri(
            'POST', create_tree_url, headers=headers, body=new_tree_meta, status=201
        )

        aiohttpretty.register_json_uri('POST', update_ref_url)
        aiohttpretty.register_json_uri('GET', branch_url, body=branch_meta)
        aiohttpretty.register_json_uri('GET', tree_url, body=tree_meta)

        result = await provider.intra_move(provider, src_path, dest_path)

        blobs = [new_tree_meta['tree'][0]]
        blobs[0]['path'] = dest_path.path
        commit = root_provider_fixtures['new_head_commit_metadata']
        expected = (GitHubFileTreeMetadata(
            blobs[0], commit=commit, ref=dest_path.branch_ref
        ), True)

        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_move_file_different_branch(self, provider, root_provider_fixtures):
        branch_meta = root_provider_fixtures['branch_metadata']
        tree_meta = root_provider_fixtures['repo_tree_metadata_root']
        new_tree_meta = root_provider_fixtures['repo_tree_metadata_root_updated']
        src_path = GitHubPath('/file.txt', _ids=[("master", ''), (branch_meta['name'], '')])
        dest_path = GitHubPath(
            '/truefacts/file.txt',
            _ids=[
                ("master", ''), (branch_meta['name'] + '2', ''), (branch_meta['name'] + '2', '')
            ])

        tree_url = furl.furl(
            provider.build_repo_url('git', 'trees', branch_meta['commit']['commit']['tree']['sha'])
        )

        tree_url.args.update({'recursive': 1})
        branch_url = provider.build_repo_url('branches', branch_meta['name'])
        branch_url_2 = provider.build_repo_url('branches', branch_meta['name'] + '2')
        create_tree_url = provider.build_repo_url('git', 'trees')
        commit_url = provider.build_repo_url('git', 'commits')
        headers = {'Content-Type': 'application/json'}
        update_ref_url = provider.build_repo_url('git', 'refs', 'heads', src_path.branch_ref)
        update_ref_url_2 = provider.build_repo_url('git', 'refs', 'heads', dest_path.branch_ref)

        aiohttpretty.register_json_uri(
            'POST', commit_url, headers=headers,
            body=root_provider_fixtures['new_head_commit_metadata'], status=201
        )

        aiohttpretty.register_json_uri(
            'POST', create_tree_url, headers=headers, body=new_tree_meta, status=201
        )

        aiohttpretty.register_json_uri('POST', update_ref_url)
        aiohttpretty.register_json_uri('POST', update_ref_url_2)
        aiohttpretty.register_json_uri('GET', branch_url, body=branch_meta)
        aiohttpretty.register_json_uri('GET', branch_url_2, body=branch_meta)
        aiohttpretty.register_json_uri('GET', tree_url, body=tree_meta)

        result = await provider.intra_move(provider, src_path, dest_path)

        blobs = [new_tree_meta['tree'][0]]
        blobs[0]['path'] = dest_path.path
        commit = root_provider_fixtures['new_head_commit_metadata']

        expected = (GitHubFileTreeMetadata(
            blobs[0], commit=commit, ref=dest_path.branch_ref
        ), True)

        assert result == expected


class TestCreateFolder:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_errors_out(self, provider):
        path = GitHubPath(
            '/Imarealboy/', _ids=[(provider.default_branch, ''), ('other_branch', '')]
        )

        url = provider.build_repo_url('contents', path.child('.gitkeep').path)

        aiohttpretty.register_uri('PUT', url, status=400)

        with pytest.raises(exceptions.CreateFolderError) as e:
            await provider.create_folder(path)

        assert e.value.code == 400

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_must_be_folder(self, provider):
        path = GitHubPath('/Imarealboy', _ids=[(provider.default_branch, ''), ('other_branch', '')])

        with pytest.raises(exceptions.CreateFolderError) as e:
            await provider.create_folder(path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_already_exists(self, provider):
        path = GitHubPath(
            '/Imarealboy/', _ids=[(provider.default_branch, ''), ('other_branch', '')]
        )

        url = provider.build_repo_url('contents', os.path.join(path.path, '.gitkeep'))

        aiohttpretty.register_json_uri('PUT', url, status=422, body={
            'message': 'Invalid request.\n\n"sha" wasn\'t supplied.'
        })

        with pytest.raises(exceptions.FolderNamingConflict) as e:
            await provider.create_folder(path)

        assert e.value.code == 409
        assert e.value.message == ('Cannot create folder "Imarealboy", because a file or folder '
                                   'already exists with that name')

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_raises_other_422(self, provider):
        path = GitHubPath(
            '/Imarealboy/', _ids=[(provider.default_branch, ''), ('other_branch', '')]
        )

        url = provider.build_repo_url('contents', os.path.join(path.path, '.gitkeep'))

        aiohttpretty.register_json_uri('PUT', url, status=422, body={
            'message': 'github no likey'
        })

        with pytest.raises(exceptions.CreateFolderError) as e:
            await provider.create_folder(path)

        assert e.value.code == 422
        assert e.value.data == {'message': 'github no likey'}

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_returns_metadata(self, provider, root_provider_fixtures):
        path = GitHubPath(
            '/i/like/trains/', _ids=[(provider.default_branch, ''),
            ('other_branch', ''), ('other_branch', ''), ('other_branch', '')]
        )

        url = provider.build_repo_url('contents', os.path.join(path.path, '.gitkeep'))

        aiohttpretty.register_json_uri(
            'PUT', url, status=201, body=root_provider_fixtures['create_folder_response']
        )

        metadata = await provider.create_folder(path)

        assert metadata.kind == 'folder'
        assert metadata.name == 'trains'
        assert metadata.path == '/i/like/trains/'


class TestOperations:

    def test_can_duplicate_names(self, provider):
        assert provider.can_duplicate_names() is False

    def test_can_intra_move(self, provider, other_provider):
        assert provider.can_intra_move(other_provider) is False
        assert provider.can_intra_move(provider) is True

    def test_can_intra_copy(self, provider, other_provider):
        assert provider.can_intra_copy(other_provider) is False
        assert provider.can_intra_copy(provider) is True

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test__fetch_branch_error(self, provider):
        url = provider.build_repo_url('branches', 'master')
        aiohttpretty.register_json_uri('GET', url, status=404)

        with pytest.raises(exceptions.NotFoundError) as e:
            await provider._fetch_branch('master')

        assert e.value.code == 404
        assert e.value.message == 'Could not retrieve file or directory . No such branch \'master\''

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test__fetch_tree_truncated_error(self, provider):
        sha = 'TotallyASha'
        url = furl.furl(provider.build_repo_url('git', 'trees', sha))
        aiohttpretty.register_json_uri('GET', url, body={'truncated': True})

        with pytest.raises(GitHubUnsupportedRepoError) as e:
            await provider._fetch_tree(sha)

        assert e.value.code == 501
        assert e.value.message == (
            'Some folder operations on large GitHub repositories cannot be supported without'
            ' data loss.  To carry out this operation, please perform it in a local git'
            ' repository, then push to the target repository on GitHub.'
        )


class TestUtilities:

    def test__path_exists_in_tree(self, provider, root_provider_fixtures):
        _ids = [('master', '')]

        assert provider._path_exists_in_tree(
            root_provider_fixtures['nested_tree_metadata']['tree'],
            GitHubPath('/alpha.txt', _ids=_ids)
        )

        assert provider._path_exists_in_tree(
            root_provider_fixtures['nested_tree_metadata']['tree'],
            GitHubPath('/beta/', _ids=_ids)
        )

        assert not provider._path_exists_in_tree(
            root_provider_fixtures['nested_tree_metadata']['tree'],
            GitHubPath('/gaw-gai.txt', _ids=_ids)
        )

        assert not provider._path_exists_in_tree(
            root_provider_fixtures['nested_tree_metadata']['tree'],
            GitHubPath('/kaw-kai/', _ids=_ids)
        )

    def test__remove_path_from_tree(self, provider, root_provider_fixtures):
        _ids = [('master', '')]

        simple_file_tree = provider._remove_path_from_tree(
            root_provider_fixtures['nested_tree_metadata']['tree'],
            GitHubPath('/alpha.txt', _ids=_ids)
        )

        assert len(simple_file_tree) == (
            len(root_provider_fixtures['nested_tree_metadata']['tree']) - 1
        )

        assert 'alpha.txt' not in [x['path'] for x in simple_file_tree]

        simple_folder_tree = provider._remove_path_from_tree(
            root_provider_fixtures['nested_tree_metadata']['tree'], GitHubPath('/beta/', _ids=_ids)
        )

        assert len(simple_folder_tree) == 1
        assert simple_folder_tree[0]['path'] == 'alpha.txt'

        nested_file_tree = provider._remove_path_from_tree(
            root_provider_fixtures['nested_tree_metadata']['tree'],
            GitHubPath('/beta/gamma.txt', _ids=_ids)
        )

        assert len(nested_file_tree) == (
            len(root_provider_fixtures['nested_tree_metadata']['tree']) - 1
        )

        assert 'beta/gamma.txt' not in [x['path'] for x in nested_file_tree]

        nested_folder_tree = provider._remove_path_from_tree(
            root_provider_fixtures['nested_tree_metadata']['tree'],
            GitHubPath('/beta/delta/', _ids=_ids)
        )

        assert len(nested_folder_tree) == 3
        assert len([x for x in nested_folder_tree if x['path'].startswith('beta/delta')]) == 0

        missing_file_tree = provider._remove_path_from_tree(
            root_provider_fixtures['nested_tree_metadata']['tree'],
            GitHubPath('/bet', _ids=_ids)
        )

        assert missing_file_tree == root_provider_fixtures['nested_tree_metadata']['tree']

        missing_folder_tree = provider._remove_path_from_tree(
            root_provider_fixtures['nested_tree_metadata']['tree'],
            GitHubPath('/beta/gam/', _ids=_ids)
        )

        assert missing_file_tree == root_provider_fixtures['nested_tree_metadata']['tree']

    def test__reparent_blobs(self, provider, root_provider_fixtures):
        _ids = [('master', '')]

        file_rename_blobs = copy.deepcopy(
            [x for x in
            root_provider_fixtures['nested_tree_metadata']['tree'] if x['path'] == 'alpha.txt']
        )

        provider._reparent_blobs(
            file_rename_blobs, GitHubPath('/alpha.txt', _ids=_ids),
            GitHubPath('/zeta.txt', _ids=_ids)
        )

        assert len(file_rename_blobs) == 1
        assert file_rename_blobs[0]['path'] == 'zeta.txt'

        folder_rename_blobs = copy.deepcopy(
            [x for x in root_provider_fixtures['nested_tree_metadata']['tree']
            if x['path'].startswith('beta')]
        )

        provider._reparent_blobs(
            folder_rename_blobs, GitHubPath('/beta/', _ids=_ids),
            GitHubPath('/theta/', _ids=_ids)
        )

        assert len(folder_rename_blobs) == 4  # beta/, gamma.txt, delta/, epsilon.txt
        assert len(
            [x for x in folder_rename_blobs if x['path'].startswith('theta/')]
        ) == 3  # gamma.txt, delta/, epsilon.txt

        assert len([x for x in folder_rename_blobs if x['path'] == 'theta']) == 1  # theta/

    def test__prune_subtrees(self, provider, root_provider_fixtures):
        pruned_tree = provider._prune_subtrees(
            root_provider_fixtures['nested_tree_metadata']['tree']
        )

        assert len(pruned_tree) == 3  # alpha.txt, gamma.txt, epsilon.txt
        assert len([x for x in pruned_tree if x['type'] == 'tree']) == 0
