import pytest
import aiohttpretty

from waterbutler.core import streams
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.gitlab import GitLabProvider
from waterbutler.providers.gitlab import settings as gitlab_settings
from waterbutler.providers.gitlab.metadata import GitLabFileMetadata
from waterbutler.providers.gitlab.metadata import GitLabFolderMetadata


@pytest.fixture
def auth():
    return {
        'name': 'cat',
        'email': 'cat@cat.com',
    }


@pytest.fixture
def gitlab_simple_project_tree():
    return [
            {
                "id": "a1e8f8d745cc87e3a9248358d9352bb7f9a0aeba",
                "name": "folder1",
                "type": "tree",
                "path": "folder1",
                "mode": "040000"
            },
            {
                "id":"a1e8f8d745cc87e3a9248358d9352bb7f9a0aeba",
                "name": "file1",
                "type": "blob",
                "path": "folder1/file1",
                "mode": "040000"
            }
        ]

@pytest.fixture
def gitlab_example_sub_project_tree():
    return [
            {
                "id": "a1e8f8d745cc87e3a9248358d9352bb7f9a0aeba",
                "name": ".gitkeep",
                "type": "blob",
                "path": "files/html/.gitkeep",
                "mode": "040000"
            },
            {
                "id": "a1e8f8d745cc87e3a9248358d9352bb7f9a0aeba",
                "name": ".gitkeep",
                "type": "tree",
                "path": "files/html/static",
                "mode": "040000"
            }
        ]

@pytest.fixture
def gitlab_example_project_tree():
    return [
            {
                "id": "a1e8f8d745cc87e3a9248358d9352bb7f9a0aeba",
                "name": "html",
                "type": "tree",
                "path": "files/html",
                "mode": "040000"
            },
            {
                "id": "4535904260b1082e14f867f7a24fd8c21495bde3",
                "name": "images",
                "type": "tree",
                "path": "files/images",
                "mode": "040000"
            },
            {
                "id": "31405c5ddef582c5a9b7a85230413ff90e2fe720",
                "name": "js",
                "type": "tree",
                "path": "files/js",
                "mode": "040000"
            },
            {
                "id": "cc71111cfad871212dc99572599a568bfe1e7e00",
                "name": "lfs",
                "type": "tree",
                "path": "files/lfs",
                "mode": "040000"
            },
            {
                "id": "fd581c619bf59cfdfa9c8282377bb09c2f897520",
                "name": "markdown",
                "type": "tree",
                "path": "files/markdown",
                "mode": "040000"
            },
            {
                "id": "23ea4d11a4bdd960ee5320c5cb65b5b3fdbc60db",
                "name": "ruby",
                "type": "tree",
                "path": "files/ruby",
                "mode": "040000"
            },
            {
                "id": "7d70e02340bac451f281cecf0a980907974bd8be",
                "name": "whitespace",
                "type": "blob",
                "path": "files/whitespace",
                "mode": "100644"
            }
        ]

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
        expected = 'http://base.url/api/v3/projects/123/contents'
        assert provider.build_repo_url('contents') == expected

class TestMetadata:
    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file_with_default_ref(self, provider):
        path = '/folder1/folder2/file'

        waterbutler_path = WaterButlerPath(path)

        url = 'http://base.url/api/v3/projects/123/repository/files?ref=master&file_path=folder1/folder2/file'

        aiohttpretty.register_json_uri('GET', url, body={
                'file_name': 'file',
                'blob_id': 'abc123',
                'commit_id': 'xxxyyy',
                'file_path': '/folder1/folder2/file',
                'size': '123'
            }
        )

        result = await provider.metadata(waterbutler_path)

        assert result.name == 'file'
        assert result.size == '123'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file_with_ref(self, provider):
        path = '/folder1/folder2/file'

        waterbutler_path = WaterButlerPath(path)

        url = 'http://base.url/api/v3/projects/123/repository/files?ref=my-branch&file_path=folder1/folder2/file'

        aiohttpretty.register_json_uri('GET', url, body={
                'file_name': 'file',
                'blob_id': 'abc123',
                'commit_id': 'xxxyyy',
                'file_path': '/folder1/folder2/file',
                'size': '123'
            }
        )

        result = await provider.metadata(waterbutler_path, 'my-branch')

        assert result.name == 'file'
        assert result.size == '123'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder(self, provider):
        path = '/folder1/folder2/folder3/'

        waterbutler_path = WaterButlerPath(path)

        url = 'http://base.url/api/v3/projects/123/repository/tree?path=folder1/folder2/folder3/'

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

        result = await provider.metadata(waterbutler_path)

        assert isinstance(result[0], GitLabFolderContentMetadata)
        assert result[0].name == 'my folder'

        assert result[1].name == 'my file'
        assert isinstance(result[1], GitLabFileContentMetadata)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder_with_sha(self, provider):
        path = '/folder1/folder2/folder3/'

        waterbutler_path = WaterButlerPath(path)

        result = await provider.metadata(waterbutler_path, ref='4b825dc642cb6eb9a060e54bf8d69288fbee4904')

        assert result == None

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder_recursive(self, provider):
        path = '/folder1/folder2/folder3/'

        waterbutler_path = WaterButlerPath(path)

        result = await provider.metadata(waterbutler_path, recursive=True)

        assert result == None

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder_with_no_dict_response(self, provider):
        path = '/folder1/folder2/folder3/'

        waterbutler_path = WaterButlerPath(path)

        url = 'http://base.url/api/v3/projects/123/repository/tree?path=folder1/folder2/folder3/'

        aiohttpretty.register_json_uri('GET', url, body={})

        with pytest.raises(exceptions.MetadataError) as exc:
            await provider.metadata(waterbutler_path)

class TestDelete:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete(self, provider):
        path = '/folder1/file.py'

        waterbutler_path = WaterButlerPath(path)

        url = 'http://base.url/api/v3/projects/123/repository/files?commit_message=File+folder1/file.py+deleted&branch_name=master&file_path=folder1/file.py'

        aiohttpretty.register_json_uri('DELETE', url)

        result = await provider.delete(waterbutler_path, branch='master')

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_folder(self, provider):

        path = '/'

        waterbutler_path = await provider.validate_path(path)

        info_url = 'http://base.url/api/v3/projects/123/repository/tree?path=/&ref_name=master'
        sub_url = 'http://base.url/api/v3/projects/123/repository/tree?path=folder1/&ref_name=master'

        url = 'http://base.url/api/v3/projects/123/repository/files?commit_message=Folder+/+deleted&file_path=folder1/file1&branch_name=master'

        aiohttpretty.register_json_uri('GET', info_url, body=gitlab_simple_project_tree())
        aiohttpretty.register_json_uri('GET', sub_url, body={})
        aiohttpretty.register_json_uri('DELETE', url)

        result = await provider.delete(waterbutler_path, branch='master')

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_complete_folder(self, provider):

        path = '/folder3/'

        waterbutler_path = await provider.validate_path(path)

        info_url = 'http://base.url/api/v3/projects/123/repository/tree?path=folder3/&ref_name=master'
        sub1_url = 'http://base.url/api/v3/projects/123/repository/tree?path=files/html/&ref_name=master'
        sub2_url = 'http://base.url/api/v3/projects/123/repository/tree?path=files/images/&ref_name=master'
        sub3_url = 'http://base.url/api/v3/projects/123/repository/tree?path=files/js/&ref_name=master'
        sub4_url = 'http://base.url/api/v3/projects/123/repository/tree?path=files/lfs/&ref_name=master'
        sub5_url = 'http://base.url/api/v3/projects/123/repository/tree?path=files/markdown/&ref_name=master'
        sub6_url = 'http://base.url/api/v3/projects/123/repository/tree?path=files/ruby/&ref_name=master'
        sub7_url = 'http://base.url/api/v3/projects/123/repository/tree?path=files/html/static/&ref_name=master'

        file1_url = 'http://base.url/api/v3/projects/123/repository/files?file_path=files/whitespace&branch_name=master&commit_message=Folder+folder3/+deleted'
        file2_url = 'http://base.url/api/v3/projects/123/repository/files?file_path=files/html/.gitkeep&branch_name=master&commit_message=Folder+folder3/+deleted'

        aiohttpretty.register_json_uri('GET', info_url, body=gitlab_example_project_tree())
        aiohttpretty.register_json_uri('GET', sub1_url, body=gitlab_example_sub_project_tree())
        aiohttpretty.register_json_uri('GET', sub2_url, body={})
        aiohttpretty.register_json_uri('GET', sub3_url, body={})
        aiohttpretty.register_json_uri('GET', sub4_url, body={})
        aiohttpretty.register_json_uri('GET', sub5_url, body={})
        aiohttpretty.register_json_uri('GET', sub6_url, body={})
        aiohttpretty.register_json_uri('GET', sub7_url, body={})
        aiohttpretty.register_json_uri('DELETE', file1_url)
        aiohttpretty.register_json_uri('DELETE', file2_url)

        result = await provider.delete(waterbutler_path, branch='master')

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_with_custom_message(self, provider):
        path = '/folder1/file.py'

        waterbutler_path = WaterButlerPath(path)

        url = 'http://base.url/api/v3/projects/123/repository/files?commit_message=custom&branch_name=master&file_path=folder1/file.py'

        aiohttpretty.register_json_uri('DELETE', url)

        result = await provider.delete(waterbutler_path, message='custom', branch='master')


class TestDownload:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_with_wrong_http_response(self, provider):
        path = '/folder1/file.py'

        waterbutler_path = WaterButlerPath(path)

        url = 'http://base.url/api/v3/projects/2123/repository/files?commit_message=File+folder1/file.py+deleted&branch_name=master&file_path=folder1/file.py'

        aiohttpretty.register_json_uri('GET', url)

        with pytest.raises(exceptions.DownloadError) as exc:
            result = await provider.download(waterbutler_path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download(self, provider):
        path = '/folder1/file.py'

        waterbutler_path = WaterButlerPath(path)

        url = 'http://base.url/api/v3/projects/123/repository/files?ref=master&file_path=folder1/file.py'

        aiohttpretty.register_json_uri('GET', url, body={
            'content': 'aGVsbG8='
        })

        result = await provider.download(waterbutler_path, branch='master')

        assert await result.read() == b'hello'

class TestUpload:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload(self, provider):
        path = '/folder1/file.py'

        waterbutler_path = WaterButlerPath(path)

        url = 'http://base.url/api/v3/projects/123/repository/files'
        aiohttpretty.register_json_uri('POST', url)

        url_put = 'http://base.url/api/v3/projects/123/repository/files'
        aiohttpretty.register_json_uri('PUT', url)

        url_metadata = 'http://base.url/api/v3/projects/123/repository/files?file_path=folder1/file.py&ref=master'
        aiohttpretty.register_json_uri('GET', url_metadata, body={
            'file_name': 'file.py',
            'file_path': path,
            'blob_id': '123',
            'size': '5',
            'commit_id': '1442422sss',
        })

        data = b'file content'
        stream = streams.StringStream(data)
        stream.name = 'foo'
        stream.content_type = 'application/octet-stream'

        result = await provider.upload(stream, waterbutler_path, 'my message', 'master')

class TestCreateFolter:
    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_create_folder_with_invalid_path(self, provider):
        path = '/folder1/file.py'

        waterbutler_path = WaterButlerPath(path)

        with pytest.raises(exceptions.CreateFolderError) as exc:
            result = await provider.create_folder(waterbutler_path, 'master', 'commit message')


    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_create_folder(self, provider):
        path = '/folder1/'

        waterbutler_path = WaterButlerPath(path)

        url = 'http://base.url/api/v3/projects/123/repository/files'
        aiohttpretty.register_json_uri('POST', url)

        url_get_metadata = 'http://base.url/api/v3/projects/123/repository/files?file_path=folder1/.gitkeep&ref=master'
        aiohttpretty.register_json_uri('GET', url_get_metadata, body={
            'file_name': '.gitkeep',
            'file_path': '/folder1/.gitkeep',
            'blob_id': '123',
            'size': '5',
            'commit_id': '1442422sss',
        })

        url_put = 'http://base.url/api/v3/projects/123/repository/files'
        aiohttpretty.register_json_uri('PUT', url)

        result = await provider.create_folder(waterbutler_path, 'master', 'commit message')

class TestOperations:
    def test_cant_duplicate_names(self, provider):
        assert provider.can_duplicate_names() == False

    def test_cant_intra_copy(self, provider, other):
        assert provider.can_intra_copy(other) == False

    def test_cant_intra_move(self, provider, other):
        assert provider.can_intra_move(other) == False

class TestValidatePath:
    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_path_v1_is_file(self, provider):
        path = '/folder1/file.py'
        url = 'http://base.url/api/v3/projects/123/repository/tree?path=folder1/file.py'
        aiohttpretty.register_json_uri('GET', url, body={})

        validated = await provider.validate_v1_path(path)

        assert validated.is_file

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_path_v1_is_dir(self, provider):
        path = '/folder1/'
        body = gitlab_simple_project_tree()
        url = 'http://base.url/api/v3/projects/123/repository/tree?path=folder1/'
        aiohttpretty.register_json_uri('GET', url, body=body)

        validated = await provider.validate_v1_path(path)

        assert validated.is_dir

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_root(self, provider):
        path = '/'
        url_get = 'http://base.url/api/v3/projects/123/repository/tree?path=/'
        body = gitlab_simple_project_tree()
        aiohttpretty.register_json_uri('GET', url_get, body=body, status=200)

        validated = await provider.validate_v1_path(path)

        assert validated.is_dir
        assert validated.is_root

    @pytest.mark.asyncio
    async def test_validate_file_path_from_gitlab(self, provider):
        path = 'folder1/file.py'

        validated = await provider.validate_v1_path(path)

        assert validated.is_file

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_folder_path_from_gitlab(self, provider):
        path = 'folder1/'
        url_get = 'http://base.url/api/v3/projects/123/repository/tree?path=folder1/'
        body = gitlab_simple_project_tree()
        aiohttpretty.register_json_uri('GET', url_get, body=body, status=200)

        validated = await provider.validate_v1_path(path)

        assert validated.is_dir

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_folder_composed_path_from_gitlab(self, provider):
        path = 'files/html/'
        url_get = 'http://base.url/api/v3/projects/123/repository/tree?path=files/html/'
        body = gitlab_example_project_tree()
        aiohttpretty.register_json_uri('GET', url_get, body=body, status=200)

        validated = await provider.validate_v1_path(path)

        assert validated.is_dir

    @pytest.mark.asyncio
    async def test_validate_path_is_file(self, provider):
        path = '/folder1/file.py'

        validated = await provider.validate_path(path)

        assert validated.is_file

    @pytest.mark.asyncio
    async def test_validate_path_is_dir(self, provider):
        path = '/folder1/folder2/'
        validated = await provider.validate_path(path)

        assert validated.is_dir

    @pytest.mark.asyncio
    async def test_validate_file_path_from_gitlab(self, provider):
        path = 'folder1/file.py'

        validated = await provider.validate_path(path)

        assert validated.is_file

    @pytest.mark.asyncio
    async def test_validate_folder_path_from_gitlab(self, provider):
        path = 'folder1/'

        validated = await provider.validate_path(path)

        assert validated.is_dir

    @pytest.mark.asyncio
    async def test_validate_folder_composed_path_from_gitlab(self, provider):
        path = 'folder1/folder2/'

        validated = await provider.validate_path(path)

        assert validated.is_dir

    @pytest.mark.asyncio
    async def test_validate_root(self, provider):
        path = '/'

        validated = await provider.validate_path(path)

        assert validated.is_dir
        assert validated.is_root
