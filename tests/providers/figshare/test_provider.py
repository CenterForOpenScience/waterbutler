import pytest

import io
import json

import aiohttpretty

from waterbutler.core import streams
from waterbutler.core import exceptions

from waterbutler.providers.figshare import metadata
from waterbutler.providers.figshare import provider
from waterbutler.providers.figshare.settings import PRIVATE_IDENTIFIER, MAX_PAGE_SIZE


@pytest.fixture
def auth():
    return {
        'name': 'cat',
        'email': 'cat@cat.com',
        'callback_url': 'http://sup.com/api/v1/project/v8s9q/waterbutler/logs/',
        'id': 'fakey',
    }


@pytest.fixture
def credentials():
    return {
        'token': 'freddie',
    }


@pytest.fixture
def project_settings():
    return {
        'container_type': 'project',
        'container_id': '13423',
    }


@pytest.fixture
def article_settings():
    return {
        'container_type': 'article',
        'container_id': '4037952',
    }


@pytest.fixture
def project_provider(auth, credentials, project_settings):
    return provider.FigshareProvider(auth, credentials, project_settings)


@pytest.fixture
def article_provider(auth, credentials, article_settings):
    return provider.FigshareProvider(auth, credentials, article_settings)


@pytest.fixture
def file_content():
    return b'sleepy'


@pytest.fixture
def file_like(file_content):
    return io.BytesIO(file_content)


@pytest.fixture
def file_stream(file_like):
    return streams.FileStreamReader(file_like)


@pytest.fixture
def list_project_articles():
    return [
        { "modified_date": "2016-10-18T12:56:27Z",
          "doi": "",
          "title": "file_article",
          "url": "https://api.figshare.com/v2/account/projects/13423/articles/4037952",
          "created_date": "2016-10-18T12:55:44Z",
          "id": 4037952,
          "published_date": None
        },
        {
          "modified_date": "2016-10-18T20:47:25Z",
          "doi": "",
          "title": "folder_article",
          "url": "https://api.figshare.com/v2/account/projects/13423/articles/4040019",
          "created_date": "2016-10-18T20:47:25Z",
          "id": 4040019,
          "published_date": None
        }
    ]


@pytest.fixture
def file_article_metadata():
    return {
        "group_resource_id": None,
        "embargo_date": None,
        "citation": "Baxter, Thomas (): file_article. figshare.\n \n Retrieved: 19 20, Oct 19, 2016 (GMT)",
        "embargo_reason": "",
        "references": [],
        "id": 4037952,
        "custom_fields": [],
        "size": 0,
        "metadata_reason": "",
        "funding": "",
        "figshare_url": "https://figshare.com/articles/_/4037952",
        "embargo_type": None,
        "title": "file_article",
        "defined_type": 3,
        "is_embargoed": False,
        "version": 0,
        "resource_doi": None,
        "confidential_reason": "",
        "files": [{
            "status": "available",
            "is_link_only": False,
            "name": "file",
            "viewer_type": "",
            "preview_state": "preview_not_supported",
            "download_url": "https://ndownloader.figshare.com/files/6530715",
            "supplied_md5": "b3e656f8b0828a31f3ed396a1c868786",
            "computed_md5": "b3e656f8b0828a31f3ed396a1c868786",
            "upload_token": "878068bf-8cdb-40c9-bcf4-5d8065ac2f7d",
            "upload_url": "",
            "id": 6530715,
            "size": 7
        }],
        "description": "",
        "tags": [],
        "created_date": "2016-10-18T12:55:44Z",
        "is_active": True,
        "authors": [{
            "url_name": "_",
            "is_active": True,
            "id": 2665435,
            "full_name": "Thomas Baxter",
            "orcid_id": ""
        }],
        "is_public": False,
        "categories": [],
        "modified_date": "2016-10-18T12:56:27Z",
        "is_confidential": False,
        "doi": "",
        "license": {
            "url": "https://creativecommons.org/licenses/by/4.0/",
            "name": "CC-BY",
            "value": 1
        },
        "has_linked_file": False,
        "url": "https://api.figshare.com/v2/account/projects/13423/articles/4037952",
        "resource_title": None,
        "status": "draft",
        "published_date": None,
        "is_metadata_record": False
    }


@pytest.fixture
def file_metadata():
    return{
        "status": "available",
        "is_link_only": False,
        "name": "file",
        "viewer_type": "",
        "preview_state": "preview_not_supported",
        "download_url": "https://ndownloader.figshare.com/files/6530715",
        "supplied_md5": "b3e656f8b0828a31f3ed396a1c868786",
        "computed_md5": "b3e656f8b0828a31f3ed396a1c868786",
        "upload_token": "878068bf-8cdb-40c9-bcf4-5d8065ac2f7d",
        "upload_url": "",
        "id": 6530715,
        "size": 7
    }


@pytest.fixture
def folder_article_metadata():
    return {
        "group_resource_id": None,
        "embargo_date": None,
        "citation": "Baxter, Thomas (): folder_article. figshare.\n \n Retrieved: 19 27, Oct 19, 2016 (GMT)",
        "embargo_reason": "",
        "references": [],
        "id": 4040019,
        "custom_fields": [],
        "size": 0,
        "metadata_reason": "",
        "funding": "",
        "figshare_url": "https://figshare.com/articles/_/4040019",
        "embargo_type": None,
        "title": "folder_article",
        "defined_type": 4,
        "is_embargoed": False,
        "version": 0,
        "resource_doi": None,
        "confidential_reason": "",
        "files": [{
            "status": "available",
            "is_link_only": False,
            "name": "folder_file.png",
            "viewer_type": "image",
            "preview_state": "preview_available",
            "download_url": "https://ndownloader.figshare.com/files/6517539",
            "supplied_md5": "",
            "computed_md5": "03dee7cf60f17a8453ccd2f51cbbbd86",
            "upload_token": "3f106f31-d62e-40e7-bac8-c6092392142d",
            "upload_url": "",
            "id": 6517539,
            "size": 15584
        }],
        "description": "",
        "tags": [],
        "created_date": "2016-10-18T20:47:25Z",
        "is_active": True,
        "authors": [{
            "url_name": "_",
            "is_active": True,
            "id": 2665435,
            "full_name": "Thomas Baxter",
            "orcid_id": ""
        }],
        "is_public": False,
        "categories": [],
        "modified_date": "2016-10-18T20:47:25Z",
        "is_confidential": False,
        "doi": "",
        "license": {
            "url": "https://creativecommons.org/licenses/by/4.0/",
            "name": "CC-BY",
            "value": 1
        },
        "has_linked_file": False,
        "url": "https://api.figshare.com/v2/account/projects/13423/articles/4040019",
        "resource_title": None,
        "status": "draft",
        "published_date": None,
        "is_metadata_record": False
    }


@pytest.fixture
def folder_file_metadata():
    return{
        "status": "available",
         "is_link_only": False,
         "name": "folder_file.png",
         "viewer_type": "image",
         "preview_state": "preview_available",
         "download_url": "https://ndownloader.figshare.com/files/6517539",
         "supplied_md5": "",
         "computed_md5": "03dee7cf60f17a8453ccd2f51cbbbd86",
         "upload_token": "3f106f31-d62e-40e7-bac8-c6092392142d",
         "upload_url": "",
         "id": 6517539,
         "size": 15584
    }

@pytest.fixture
def create_article_metadata():
    return {
        "location": "https://api.figshare.com/v2/account/projects/13423/articles/4055568"
    }

@pytest.fixture
def create_file_metadata():
    return {
        "location": "https://api.figshare.com/v2/account/articles/4055568/files/6530715"}

@pytest.fixture
def get_file_metadata():
    return {
        "status": "created",
        "is_link_only": False,
        "name": "barricade.gif",
        "viewer_type": "",
        "preview_state": "preview_not_available",
        "download_url": "https://ndownloader.figshare.com/files/6530715",
        "supplied_md5": "",
        "computed_md5": "",
        "upload_token": "c9d1a465-f3f6-402c-8106-db3493942303",
        "upload_url": "https://fup100310.figshare.com/upload/c9d1a465-f3f6-402c-8106-db3493942303",
        "id": 6530715,
        "size": 7}

@pytest.fixture
def get_upload_metadata():
    return {
        "token": "c9d1a465-f3f6-402c-8106-db3493942303",
        "md5": "",
        "size": 1071709,
        "name": "6530715/barricade.gif",
        "status": "PENDING",
        "parts": [{
            "partNo": 1,
            "startOffset": 0,
            "endOffset": 6,
            "status": "PENDING",
            "locked": False}]}

@pytest.fixture
def upload_article_metadata():
    return {
        "group_resource_id": None,
        "embargo_date": None,
        "citation": "Baxter, Thomas (): barricade.gif. figshare.\n \n Retrieved: 19 20, Oct 19, 2016 (GMT)",
        "embargo_reason": "",
        "references": [],
        "id": 4055568,
        "custom_fields": [],
        "size": 0,
        "metadata_reason": "",
        "funding": "",
        "figshare_url": "https://figshare.com/articles/_/4037952",
        "embargo_type": None,
        "title": "barricade.gif",
        "defined_type": 3,
        "is_embargoed": False,
        "version": 0,
        "resource_doi": None,
        "confidential_reason": "",
        "files": [{
            "status": "available",
            "is_link_only": False,
            "name": "barricade.gif",
            "viewer_type": "",
            "preview_state": "preview_not_supported",
            "download_url": "https://ndownloader.figshare.com/files/6530715",
            "supplied_md5": "b3e656f8b0828a31f3ed396a1c868786",
            "computed_md5": "b3e656f8b0828a31f3ed396a1c868786",
            "upload_token": "878068bf-8cdb-40c9-bcf4-5d8065ac2f7d",
            "upload_url": "",
            "id": 6530715,
            "size": 7
        }],
        "description": "",
        "tags": [],
        "created_date": "2016-10-18T12:55:44Z",
        "is_active": True,
        "authors": [{
            "url_name": "_",
            "is_active": True,
            "id": 2665435,
            "full_name": "Thomas Baxter",
            "orcid_id": ""
        }],
        "is_public": False,
        "categories": [],
        "modified_date": "2016-10-18T12:56:27Z",
        "is_confidential": False,
        "doi": "",
        "license": {
            "url": "https://creativecommons.org/licenses/by/4.0/",
            "name": "CC-BY",
            "value": 1
        },
        "has_linked_file": False,
        "url": "https://api.figshare.com/v2/account/projects/13423/articles/4037952",
        "resource_title": None,
        "status": "draft",
        "published_date": None,
        "is_metadata_record": False
    }

@pytest.fixture
def upload_folder_article_metadata():
    return {
        "group_resource_id": None,
        "embargo_date": None,
        "citation": "Baxter, Thomas (): barricade.gif. figshare.\n \n Retrieved: 19 20, Oct 19, 2016 (GMT)",
        "embargo_reason": "",
        "references": [],
        "id": 4040019,
        "custom_fields": [],
        "size": 0,
        "metadata_reason": "",
        "funding": "",
        "figshare_url": "https://figshare.com/articles/_/4040019",
        "embargo_type": None,
        "title": "barricade.gif",
        "defined_type": 4,
        "is_embargoed": False,
        "version": 0,
        "resource_doi": None,
        "confidential_reason": "",
        "files": [{
            "status": "available",
            "is_link_only": False,
            "name": "barricade.gif",
            "viewer_type": "",
            "preview_state": "preview_not_supported",
            "download_url": "https://ndownloader.figshare.com/files/6530715",
            "supplied_md5": "b3e656f8b0828a31f3ed396a1c868786",
            "computed_md5": "b3e656f8b0828a31f3ed396a1c868786",
            "upload_token": "878068bf-8cdb-40c9-bcf4-5d8065ac2f7d",
            "upload_url": "",
            "id": 6530715,
            "size": 7
        }],
        "description": "",
        "tags": [],
        "created_date": "2016-10-18T12:55:44Z",
        "is_active": True,
        "authors": [{
            "url_name": "_",
            "is_active": True,
            "id": 2665435,
            "full_name": "Thomas Baxter",
            "orcid_id": ""
        }],
        "is_public": False,
        "categories": [],
        "modified_date": "2016-10-18T12:56:27Z",
        "is_confidential": False,
        "doi": "",
        "license": {
            "url": "https://creativecommons.org/licenses/by/4.0/",
            "name": "CC-BY",
            "value": 1
        },
        "has_linked_file": False,
        "url": "https://api.figshare.com/v2/account/projects/13423/articles/4040019",
        "resource_title": None,
        "status": "draft",
        "published_date": None,
        "is_metadata_record": False
    }


class TestPolymorphism:
    # These should not be passing but are

    async def test_project_provider(self, project_settings, project_provider):
        assert isinstance(project_provider, provider.FigshareProjectProvider)
        assert project_provider.project_id == project_settings['container_id']

    async def test_article_provider(self, article_settings, article_provider):
        assert isinstance(article_provider, provider.FigshareArticleProvider)
        assert article_provider.article_id == article_settings['container_id']


class TestMetadata:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_project_contents(self, project_provider, list_project_articles,
                                    file_article_metadata, folder_article_metadata):

        root_parts = project_provider.root_path_parts
        list_articles_url = project_provider.build_url(False, *root_parts, 'articles')
        file_metadata_url = project_provider.build_url(False, *root_parts,'articles',
                                                       str(list_project_articles[0]['id']))
        folder_metadata_url = project_provider.build_url(False, *root_parts, 'articles',
                                                         str(list_project_articles[1]['id']))

        aiohttpretty.register_json_uri('GET', list_articles_url, body=list_project_articles,
                                       params={'page': '1', 'page_size': str(MAX_PAGE_SIZE)})
        aiohttpretty.register_json_uri('GET', list_articles_url, body=[],
                                       params={'page': '2', 'page_size': str(MAX_PAGE_SIZE)})
        aiohttpretty.register_json_uri('GET', file_metadata_url, body=file_article_metadata)
        aiohttpretty.register_json_uri('GET', folder_metadata_url, body=folder_article_metadata)

        path = await project_provider.validate_path('/')
        result = await project_provider.metadata(path)

        assert aiohttpretty.has_call(method='GET', uri=list_articles_url,
                                     params={'page': '1', 'page_size': str(MAX_PAGE_SIZE)})
        assert aiohttpretty.has_call(method='GET', uri=file_metadata_url)
        assert aiohttpretty.has_call(method='GET', uri=folder_metadata_url)

        assert result ==  [
            metadata.FigshareFileMetadata(file_article_metadata, file_article_metadata['files'][0]),
            metadata.FigshareFolderMetadata(folder_article_metadata)
        ]

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_project_file_article_contents(self, project_provider, list_project_articles,
                                                 file_article_metadata, file_metadata):

        root_parts = project_provider.root_path_parts
        article_id = str(file_article_metadata['id'])
        article_name = file_article_metadata['title']
        file_id = str(file_metadata['id'])
        file_name = file_metadata['name']

        list_articles_url = project_provider.build_url(False, *root_parts, 'articles')
        file_article_metadata_url = project_provider.build_url(False, *root_parts, 'articles',
                                                               article_id)
        file_metadata_url = project_provider.build_url(False, *root_parts, 'articles',
                                                       article_id, 'files', file_id)

        aiohttpretty.register_json_uri('GET', list_articles_url, body=list_project_articles,
                                       params={'page': '1', 'page_size': str(MAX_PAGE_SIZE)})
        aiohttpretty.register_json_uri('GET', list_articles_url, body=[],
                                       params={'page': '2', 'page_size': str(MAX_PAGE_SIZE)})
        aiohttpretty.register_json_uri('GET', file_article_metadata_url, body=file_article_metadata)
        aiohttpretty.register_json_uri('GET', file_metadata_url, body=file_metadata)

        path = await project_provider.validate_path('/{}/{}'.format(article_id, file_id))
        result = await project_provider.metadata(path)

        assert aiohttpretty.has_call(method='GET', uri=list_articles_url,
                                     params={'page': '1', 'page_size': str(MAX_PAGE_SIZE)})
        assert aiohttpretty.has_call(method='GET', uri=file_article_metadata_url)
        assert aiohttpretty.has_call(method='GET', uri=file_metadata_url)

        expected = metadata.FigshareFileMetadata(file_article_metadata, file_metadata)
        assert result == expected

        assert str(result.id) == file_id
        assert result.name == file_name
        assert result.path == '/{}/{}'.format(article_id, file_id)
        assert result.materialized_path == '/{}/{}'.format(article_name, file_name)
        assert str(result.article_id) == article_id
        assert result.article_name == article_name
        assert result.size == file_metadata['size']
        assert result.is_public == (PRIVATE_IDENTIFIER not in file_article_metadata['url'])

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_project_folder_article_contents(self, project_provider, list_project_articles,
                                                   folder_article_metadata, folder_file_metadata):

        root_parts = project_provider.root_path_parts
        article_id = str(folder_article_metadata['id'])
        article_name = folder_article_metadata['title']
        file_id = str(folder_file_metadata['id'])
        file_name = folder_file_metadata['name']

        list_articles_url = project_provider.build_url(False, *root_parts, 'articles')
        folder_article_metadata_url = project_provider.build_url(False, *root_parts, 'articles',
                                                                 article_id)
        file_metadata_url = project_provider.build_url(False, *root_parts, 'articles',
                                                       article_id, 'files', file_id)

        aiohttpretty.register_json_uri('GET', list_articles_url, body=list_project_articles,
                                       params={'page': '1', 'page_size': str(MAX_PAGE_SIZE)})
        aiohttpretty.register_json_uri('GET', list_articles_url, body=[],
                                       params={'page': '2', 'page_size': str(MAX_PAGE_SIZE)})
        aiohttpretty.register_json_uri('GET', folder_article_metadata_url,
                                       body=folder_article_metadata)
        aiohttpretty.register_json_uri('GET', file_metadata_url, body=folder_file_metadata)

        path = await project_provider.validate_path('/{}/{}'.format(article_id, file_id))
        result = await project_provider.metadata(path)

        assert aiohttpretty.has_call(method='GET', uri=list_articles_url,
                                     params={'page': '1', 'page_size': str(MAX_PAGE_SIZE)})
        assert aiohttpretty.has_call(method='GET', uri=folder_article_metadata_url)
        assert aiohttpretty.has_call(method='GET', uri=file_metadata_url)

        expected = metadata.FigshareFileMetadata(folder_article_metadata, folder_file_metadata)
        assert result == expected

        assert str(result.id) == file_id
        assert result.name == file_name
        assert result.path == '/{}/{}'.format(article_id, file_id)
        assert result.materialized_path == '/{}/{}'.format(article_name, file_name)
        assert str(result.article_id) == article_id
        assert result.article_name == article_name
        assert result.size == folder_file_metadata['size']
        assert result.is_public == (PRIVATE_IDENTIFIER not in folder_article_metadata['url'])

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_article_file_contents(self, article_provider, folder_article_metadata,
                                         folder_file_metadata):

        root_parts = article_provider.root_path_parts
        article_id = str(folder_article_metadata['id'])
        article_name = folder_article_metadata['title']
        file_id = str(folder_file_metadata['id'])
        file_name = folder_file_metadata['name']

        folder_article_metadata_url = article_provider.build_url(False, *root_parts)
        file_metadata_url = article_provider.build_url(False, *root_parts, 'files', file_id)
        print("%%%%%%% HERH?: {}".format(file_metadata_url))

        aiohttpretty.register_json_uri('GET', folder_article_metadata_url,
                                       body=folder_article_metadata)
        aiohttpretty.register_json_uri('GET', file_metadata_url, body=folder_file_metadata)

        path = await article_provider.validate_path('/{}'.format(file_id))
        result = await article_provider.metadata(path)

        assert aiohttpretty.has_call(method='GET', uri=folder_article_metadata_url)
        assert aiohttpretty.has_call(method='GET', uri=file_metadata_url)

        expected = metadata.FigshareFileMetadata(folder_article_metadata, folder_file_metadata)
        assert result == expected

        assert str(result.id) == file_id
        assert result.name == file_name
        assert result.path == '/{}/{}'.format(article_id, file_id)
        assert result.materialized_path == '/{}/{}'.format(article_name, file_name)
        assert result.article_name == article_name
        assert result.size == folder_file_metadata['size']
        assert result.is_public == (PRIVATE_IDENTIFIER not in folder_article_metadata['url'])


class TestCRUD:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_project_upload(self, project_provider, list_project_articles,
                                  create_article_metadata, create_file_metadata,
                                  get_file_metadata, get_upload_metadata, file_stream,
                                  upload_article_metadata):
        file_name = 'barricade.gif'

        root_parts = project_provider.root_path_parts
        list_articles_url = project_provider.build_url(False, *root_parts, 'articles')
        validate_article_url = project_provider.build_url(False, *root_parts, 'articles', file_name)

        aiohttpretty.register_json_uri('GET', list_articles_url, body=list_project_articles,
                                       params={'page': '1', 'page_size': str(MAX_PAGE_SIZE)})
        aiohttpretty.register_json_uri('GET', list_articles_url, body=[],
                                       params={'page': '2', 'page_size': str(MAX_PAGE_SIZE)})
        aiohttpretty.register_uri('GET', validate_article_url, status=404)
        path = await project_provider.validate_path('/' + file_name)

        article_id = str(upload_article_metadata['id'])
        create_article_url = project_provider.build_url(False, *root_parts, 'articles')
        create_file_url = project_provider.build_url(False, 'articles', article_id, 'files')
        file_url = project_provider.build_url(False, 'articles', article_id, 'files',
                                              str(get_file_metadata['id']))
        get_article_url = project_provider.build_url(False, *root_parts, 'articles', article_id)
        upload_url = get_file_metadata['upload_url']

        aiohttpretty.register_json_uri('POST', create_article_url, body=create_article_metadata, status=201)
        aiohttpretty.register_json_uri('POST', create_file_url, body=create_file_metadata, status=201)
        aiohttpretty.register_json_uri('GET', file_url, body=get_file_metadata)
        aiohttpretty.register_json_uri('GET', upload_url, body=get_upload_metadata)
        aiohttpretty.register_uri('PUT', '{}/1'.format(upload_url), status=200)
        aiohttpretty.register_uri('POST', file_url, status=202)
        aiohttpretty.register_json_uri('GET', get_article_url, body=upload_article_metadata)

        result, created = await project_provider.upload(file_stream, path)
        expected = metadata.FigshareFileMetadata(
            upload_article_metadata,
            upload_article_metadata['files'][0],
        )
        assert aiohttpretty.has_call(
            method='POST',
            uri=create_article_url,
            data=json.dumps({
                'title': 'barricade.gif',
            })
        )
        assert aiohttpretty.has_call(method='PUT', uri='{}/1'.format(upload_url))
        assert aiohttpretty.has_call(method='POST', uri=create_file_url)
        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_project_folder_upload(self, file_stream, project_provider, list_project_articles,
                                         folder_article_metadata, get_file_metadata,
                                         create_file_metadata, get_upload_metadata,
                                         upload_folder_article_metadata):
        file_name = 'barricade.gif'
        article_id = str(list_project_articles[1]['id'])

        root_parts = project_provider.root_path_parts
        list_articles_url = project_provider.build_url(False, *root_parts, 'articles')
        validate_folder_url = project_provider.build_url(False, *root_parts, 'articles', article_id)
        validate_file_url = project_provider.build_url(False, *root_parts, 'articles', article_id,
                                                       'files', file_name)

        aiohttpretty.register_json_uri('GET', list_articles_url, body=list_project_articles,
                                       params={'page': '1', 'page_size': str(MAX_PAGE_SIZE)})
        aiohttpretty.register_json_uri('GET', list_articles_url, body=[],
                                       params={'page': '2', 'page_size': str(MAX_PAGE_SIZE)})
        aiohttpretty.register_json_uri('GET', validate_folder_url, body=folder_article_metadata)
        aiohttpretty.register_uri('GET', validate_file_url, status=404)
        path = await project_provider.validate_path('/{}/{}'.format(article_id, file_name))

        create_file_url = project_provider.build_url(False, 'articles', article_id, 'files')
        file_url = project_provider.build_url(False, 'articles', article_id, 'files',
                                              str(get_file_metadata['id']))
        get_article_url = project_provider.build_url(False, *root_parts, 'articles', article_id)
        upload_url = get_file_metadata['upload_url']

        aiohttpretty.register_json_uri('POST', create_file_url, body=create_file_metadata,
                                       status=201)
        aiohttpretty.register_json_uri('GET', file_url, body=get_file_metadata)
        aiohttpretty.register_json_uri('GET', upload_url, body=get_upload_metadata)
        aiohttpretty.register_uri('PUT', '{}/1'.format(upload_url), status=200)
        aiohttpretty.register_uri('POST', file_url, status=202)
        aiohttpretty.register_json_uri('GET', get_article_url, body=upload_folder_article_metadata)

        result, created = await project_provider.upload(file_stream, path)
        expected = metadata.FigshareFileMetadata(
            upload_folder_article_metadata,
            upload_folder_article_metadata['files'][0],
        )
        assert aiohttpretty.has_call(method='PUT', uri='{}/1'.format(upload_url))
        assert aiohttpretty.has_call(method='POST', uri=create_file_url)
        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_article_upload(self, file_stream, article_provider, folder_article_metadata,
                                  get_file_metadata, create_file_metadata, get_upload_metadata,
                                  upload_folder_article_metadata):
        file_name = 'barricade.gif'
        file_id = str(get_file_metadata['id'])
        root_parts = article_provider.root_path_parts

        validate_file_url = article_provider.build_url(False, *root_parts, 'files', file_name)

        aiohttpretty.register_uri('GET', validate_file_url, status=404)
        path = await article_provider.validate_path('/' + file_name)

        create_file_url = article_provider.build_url(False, *root_parts, 'files')
        file_url = article_provider.build_url(False, *root_parts, 'files', file_id)
        get_article_url = article_provider.build_url(False, *root_parts)
        upload_url = get_file_metadata['upload_url']

        aiohttpretty.register_json_uri('POST', create_file_url, body=create_file_metadata, status=201)
        aiohttpretty.register_json_uri('GET', file_url, body=get_file_metadata)
        aiohttpretty.register_json_uri('GET', get_file_metadata['upload_url'], body=get_upload_metadata)
        aiohttpretty.register_uri('PUT', '{}/1'.format(upload_url), status=200)
        aiohttpretty.register_uri('POST', file_url, status=202)
        aiohttpretty.register_json_uri('GET', get_article_url, body=upload_folder_article_metadata)

        result, created = await article_provider.upload(file_stream, path)
        expected = metadata.FigshareFileMetadata(
            upload_folder_article_metadata,
            upload_folder_article_metadata['files'][0],
        )
        assert aiohttpretty.has_call(method='PUT', uri='{}/1'.format(upload_url))
        assert aiohttpretty.has_call(method='POST', uri=create_file_url)
        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_project_article_download(self, project_provider, file_article_metadata,
                                            list_project_articles, file_metadata):
        article_id = str(list_project_articles[0]['id'])
        file_id = str(file_article_metadata['files'][0]['id'])
        body = b'castle on a cloud'
        root_parts = project_provider.root_path_parts

        list_articles_url = project_provider.build_url(False, *root_parts, 'articles')
        file_metadata_url = project_provider.build_url(False, *root_parts, 'articles', article_id,
                                                       'files', file_id)
        article_metadata_url = project_provider.build_url(False, *root_parts, 'articles',
                                                          article_id)
        download_url = file_metadata['download_url']

        aiohttpretty.register_json_uri('GET', list_articles_url, body=list_project_articles,
                                       params={'page': '1', 'page_size': str(MAX_PAGE_SIZE)})
        aiohttpretty.register_json_uri('GET', list_articles_url, body=[],
                                       params={'page': '2', 'page_size': str(MAX_PAGE_SIZE)})
        aiohttpretty.register_json_uri('GET', file_metadata_url, body=file_metadata)
        aiohttpretty.register_json_uri('GET', article_metadata_url, body=file_article_metadata)
        aiohttpretty.register_uri('GET', download_url, params={'token': project_provider.token},
                                  body=body, auto_length=True)

        path = await project_provider.validate_path('/{}/{}'.format(article_id, file_id))
        result = await project_provider.download(path)
        content = await result.read()

        assert content == body

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_article_download(self, article_provider, file_article_metadata, file_metadata):
        body = b'castle on a cloud'
        file_id = str(file_metadata['id'])
        root_parts = article_provider.root_path_parts

        file_metadata_url = article_provider.build_url(False, *root_parts, 'files', file_id)
        article_metadata_url = article_provider.build_url(False, *root_parts)
        download_url = file_metadata['download_url']

        aiohttpretty.register_json_uri('GET', file_metadata_url, body=file_metadata)
        aiohttpretty.register_json_uri('GET', article_metadata_url, body=file_article_metadata)
        aiohttpretty.register_uri('GET', download_url, params={'token': article_provider.token},
                                   body=body, auto_length=True)

        path = await article_provider.validate_path('/{}'.format(file_id))
        result = await article_provider.download(path)
        content = await result.read()
        assert content == body

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_project_file_delete(self, project_provider, list_project_articles,
                                       file_article_metadata, file_metadata):
        file_id = str(file_metadata['id'])
        article_id = str(list_project_articles[0]['id'])
        root_parts = project_provider.root_path_parts

        list_articles_url = project_provider.build_url(False, *root_parts, 'articles')
        file_url = project_provider.build_url(False, *root_parts, 'articles', article_id, 'files',
                                              file_id)
        file_article_url = project_provider.build_url(False, *root_parts, 'articles', article_id)

        aiohttpretty.register_json_uri('GET', list_articles_url, body=list_project_articles,
                                       params={'page': '1', 'page_size': str(MAX_PAGE_SIZE)})
        aiohttpretty.register_json_uri('GET', list_articles_url, body=[],
                                       params={'page': '2', 'page_size': str(MAX_PAGE_SIZE)})
        aiohttpretty.register_json_uri('GET', file_url, body=file_metadata)
        aiohttpretty.register_json_uri('GET', file_article_url, body=file_article_metadata)
        aiohttpretty.register_uri('DELETE', file_article_url, status=204)

        path = await project_provider.validate_path('/{}/{}'.format(article_id, file_id))
        result = await project_provider.delete(path)

        assert result is None
        assert aiohttpretty.has_call(method='DELETE', uri=file_article_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_project_folder_delete(self, project_provider, list_project_articles,
                                         folder_article_metadata):
        article_id = str(list_project_articles[1]['id'])
        root_parts = project_provider.root_path_parts

        list_articles_url = project_provider.build_url(False, *root_parts, 'articles')
        folder_article_url = project_provider.build_url(False, *root_parts,'articles', article_id)

        aiohttpretty.register_json_uri('GET', list_articles_url, body=list_project_articles,
                                       params={'page': '1', 'page_size': str(MAX_PAGE_SIZE)})
        aiohttpretty.register_json_uri('GET', list_articles_url, body=[],
                                       params={'page': '2', 'page_size': str(MAX_PAGE_SIZE)})
        aiohttpretty.register_json_uri('GET', folder_article_url, body=folder_article_metadata)
        aiohttpretty.register_uri('DELETE', folder_article_url, status=204)

        path = await project_provider.validate_path('/{}'.format(article_id))
        result = await project_provider.delete(path)

        assert result is None
        assert aiohttpretty.has_call(method='DELETE', uri=folder_article_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_article_file_delete(self, article_provider, file_metadata):
        file_id = str(file_metadata['id'])

        file_url = article_provider.build_url(False, *article_provider.root_path_parts, 'files',
                                              file_id)

        aiohttpretty.register_json_uri('GET', file_url, body=file_metadata)
        aiohttpretty.register_uri('DELETE', file_url, status=204)

        path = await article_provider.validate_path('/{}'.format(file_id))
        result = await article_provider.delete(path)

        assert result is None
        assert aiohttpretty.has_call(method='DELETE', uri=file_url)


class TestRevalidatePath:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_revalidate_path(self, project_provider, list_project_articles,
                                   file_article_metadata, folder_article_metadata):
        file_article_id = str(list_project_articles[0]['id'])
        folder_article_id = str(list_project_articles[1]['id'])

        root_parts = project_provider.root_path_parts
        list_articles_url = project_provider.build_url(False, *root_parts, 'articles')
        file_article_url = project_provider.build_url(False, *root_parts, 'articles',
                                                      file_article_id)
        folder_article_url = project_provider.build_url(False, *root_parts, 'articles',
                                                        folder_article_id)
        print("%%%%%% list_articles_url: {}".format(list_articles_url))
        print("%%%%%% file_article_url: {}".format(file_article_url))
        print("%%%%%% folder_article_url: {}".format(folder_article_url))

        aiohttpretty.register_json_uri('GET', list_articles_url, body=list_project_articles,
                                       params={'page': '1', 'page_size': str(MAX_PAGE_SIZE)})
        aiohttpretty.register_json_uri('GET', list_articles_url, body=[],
                                       params={'page': '2', 'page_size': str(MAX_PAGE_SIZE)})
        aiohttpretty.register_json_uri('GET', file_article_url, body=file_article_metadata)
        aiohttpretty.register_json_uri('GET', folder_article_url, body=folder_article_metadata)

        path = await project_provider.validate_path('/')

        result = await project_provider.revalidate_path(path, '{}'.format('file'), folder=False)

        assert result.is_dir is False
        assert result.name == 'file'
        assert result.identifier == str(file_article_metadata['files'][0]['id'])
