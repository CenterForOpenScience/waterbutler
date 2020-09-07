import io
import json

import pytest
import aiohttpretty

from waterbutler.core import streams
from waterbutler.core import exceptions
from waterbutler.providers.figshare import metadata
from waterbutler.providers.figshare import provider
from waterbutler.providers.figshare.path import FigsharePath
from waterbutler.providers.figshare.settings import MAX_PAGE_SIZE

from tests.providers.figshare.fixtures import (crud_fixtures,
                                               error_fixtures,
                                               project_list_articles,
                                               root_provider_fixtures,
                                               project_article_type_1_metadata,
                                               project_article_type_3_metadata,
                                               project_article_type_1_file_metadata,
                                               project_article_type_3_file_metadata)


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
def project_settings_2():
    return {
        'container_type': 'project',
        'container_id': '64916',
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
def project_provider_2(auth, credentials, project_settings_2):
    return provider.FigshareProvider(auth, credentials, project_settings_2)


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


class TestPolymorphism:

    def test_project_provider(self, project_settings, project_provider):
        assert isinstance(project_provider, provider.FigshareProjectProvider)
        assert project_provider.container_id == project_settings['container_id']

    def test_article_provider(self, article_settings, article_provider):
        assert isinstance(article_provider, provider.FigshareArticleProvider)
        assert article_provider.container_id == article_settings['container_id']


class TestProjectV1ValidatePath:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_folder_article(self, project_provider, root_provider_fixtures):
        item = root_provider_fixtures['folder_article_metadata']
        file_id = str(item['id'])
        path = '/{}/'.format(file_id)
        article_list_url = project_provider.build_url(False, *project_provider.root_path_parts,
                                                      'articles')
        article_segments = (*project_provider.root_path_parts, 'articles', str(item['id']))
        article_url = project_provider.build_url(False, *article_segments)

        aiohttpretty.register_json_uri('GET', article_list_url,
                                       body=root_provider_fixtures['list_project_articles'],
                                       params={'page': '1', 'page_size': str(MAX_PAGE_SIZE)})
        aiohttpretty.register_json_uri('GET', article_list_url, body=[],
                                       params={'page': '2', 'page_size': str(MAX_PAGE_SIZE)})
        aiohttpretty.register_json_uri('GET', article_url, body=item)

        result = await project_provider.validate_v1_path(path)
        expected = FigsharePath('/{}/'.format(item['title']),
                                _ids=(project_provider.container_id, file_id),
                                folder=True,
                                is_public=False)

        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_folder_article_bad_path(self, project_provider,
                                                            root_provider_fixtures):
        item = root_provider_fixtures['folder_article_metadata']
        file_id = str(item['id'])
        path = '/{}'.format(file_id)
        article_list_url = project_provider.build_url(False, *project_provider.root_path_parts,
                                                      'articles')
        article_segments = (*project_provider.root_path_parts, 'articles', str(item['id']))
        article_url = project_provider.build_url(False, *article_segments)

        aiohttpretty.register_json_uri('GET', article_list_url,
                                       body=root_provider_fixtures['list_project_articles'],
                                       params={'page': '1', 'page_size': str(MAX_PAGE_SIZE)})
        aiohttpretty.register_json_uri('GET', article_list_url, body=[],
                                       params={'page': '2', 'page_size': str(MAX_PAGE_SIZE)})
        aiohttpretty.register_json_uri('GET', article_url, body=item)

        with pytest.raises(exceptions.NotFoundError) as e:
            await project_provider.validate_v1_path(path)

        assert e.value.code == 404
        assert aiohttpretty.has_call(method='GET', uri=article_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_folder_article_bad_type(self, project_provider,
                                                            root_provider_fixtures):
        item = root_provider_fixtures['folder_article_metadata']
        file_id = str(item['id'])
        path = '/{}/'.format(file_id)
        item['defined_type'] = 5
        article_list_url = project_provider.build_url(False, *project_provider.root_path_parts,
                                                      'articles')
        article_segments = (*project_provider.root_path_parts, 'articles', str(item['id']))
        article_url = project_provider.build_url(False, *article_segments)

        aiohttpretty.register_json_uri('GET', article_list_url,
                                       body=root_provider_fixtures['list_project_articles'],
                                       params={'page': '1', 'page_size': str(MAX_PAGE_SIZE)})

        aiohttpretty.register_json_uri('GET', article_list_url, body=[],
                                       params={'page': '2', 'page_size': str(MAX_PAGE_SIZE)})

        aiohttpretty.register_json_uri('GET', article_url, body=item)

        with pytest.raises(exceptions.NotFoundError) as e:
            await project_provider.validate_v1_path(path)

        assert e.value.code == 404
        assert aiohttpretty.has_call(method='GET', uri=article_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_project_validate_v1_path_root(self, project_provider):
        path = '/'

        result = await project_provider.validate_v1_path(path)
        expected = FigsharePath(path, _ids=('', ), folder=True, is_public=False)

        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_article_validate_v1_path_invalid_path(self, article_provider):
        with pytest.raises(exceptions.InvalidPathError) as e:
            await article_provider.validate_v1_path('/this/is/an/invalid/path')

        assert e.value.code == 400

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_invalid_path(self, project_provider):
        path = 'whatever'

        with pytest.raises(exceptions.InvalidPathError) as e:
            await project_provider.validate_v1_path(path)

        assert e.value.code == 400

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_file_article(self, project_provider, root_provider_fixtures):
        file_item = root_provider_fixtures['file_metadata']
        item = root_provider_fixtures['file_article_metadata']
        file_id = str(item['files'][0]['id'])
        article_id = str(item['id'])
        path = '/{}/{}'.format(article_id, file_id)
        article_list_url = project_provider.build_url(False, *project_provider.root_path_parts,
                                                      'articles')
        article_segments = (*project_provider.root_path_parts, 'articles', str(item['id']))
        article_url = project_provider.build_url(False, *article_segments, 'files', file_id)

        aiohttpretty.register_json_uri('GET', article_list_url,
                                       body=root_provider_fixtures['list_project_articles'],
                                       params={'page': '1', 'page_size': str(MAX_PAGE_SIZE)})

        aiohttpretty.register_json_uri('GET', article_list_url, body=[],
                                       params={'page': '2', 'page_size': str(MAX_PAGE_SIZE)})

        aiohttpretty.register_json_uri('GET', article_url, body=file_item)

        result = await project_provider.validate_v1_path(path)
        expected = FigsharePath('/{}/{}'.format(item['title'], file_item['name']),
                                _ids=(project_provider.container_id, file_id),
                                folder=False, is_public=False)

        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_file_article_public(self, project_provider,
                                                        root_provider_fixtures):
        file_item = root_provider_fixtures['file_metadata_public']
        item = root_provider_fixtures['file_article_metadata']
        file_id = str(file_item['id'])
        article_id = str(item['id'])
        path = '/{}/{}'.format(article_id, file_id)
        article_list_url = project_provider.build_url(False, *project_provider.root_path_parts,
                                                      'articles')
        article_segments = (*project_provider.root_path_parts, 'articles', str(item['id']))
        article_url = project_provider.build_url(True, *article_segments, 'files', file_id)

        aiohttpretty.register_json_uri('GET', article_list_url,
                                       body=root_provider_fixtures['public_list_project_articles'],
                                       params={'page': '1', 'page_size': str(MAX_PAGE_SIZE)})

        aiohttpretty.register_json_uri('GET', article_list_url, body=[],
                                       params={'page': '2', 'page_size': str(MAX_PAGE_SIZE)})

        aiohttpretty.register_json_uri('GET', article_url, body=file_item)

        result = await project_provider.validate_v1_path(path)
        expected = FigsharePath('/{}/{}'.format(item['title'], file_item['name']),
                                _ids=(project_provider.container_id, file_id),
                                folder=False, is_public=False)

        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_file_article_bad_path(self, project_provider,
                                                          root_provider_fixtures):
        file_item = root_provider_fixtures['file_metadata']
        item = root_provider_fixtures['file_article_metadata']
        file_id = str(item['files'][0]['id'])
        article_id = str(item['id'])
        path = '/{}/{}/'.format(article_id, file_id)
        article_list_url = project_provider.build_url(False,
                                    *project_provider.root_path_parts, 'articles')
        article_segments = (*project_provider.root_path_parts, 'articles', str(item['id']))
        article_url = project_provider.build_url(False, *article_segments, 'files', file_id)

        aiohttpretty.register_json_uri('GET', article_list_url,
                                        body=root_provider_fixtures['list_project_articles'],
                                        params={'page': '1', 'page_size': str(MAX_PAGE_SIZE)})

        aiohttpretty.register_json_uri('GET', article_list_url, body=[],
                                       params={'page': '2', 'page_size': str(MAX_PAGE_SIZE)})

        aiohttpretty.register_json_uri('GET', article_url, body=file_item)

        with pytest.raises(exceptions.NotFoundError) as e:
            await project_provider.validate_v1_path(path)

        assert e.value.code == 404


class TestArticleV1ValidatePath:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_article_validate_v1_path_root(self, article_provider):
        path = '/'

        result = await article_provider.validate_v1_path(path)
        expected = FigsharePath(path, _ids=('', ), folder=True, is_public=False)

        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_article_validate_v1_path(self, article_provider, root_provider_fixtures):
        item = root_provider_fixtures['file_metadata']
        file_id = item['id']
        path = '/' + str(file_id)
        url = article_provider.build_url(False, *article_provider.root_path_parts, 'files',
                                         str(file_id))

        aiohttpretty.register_json_uri('GET', url, body=item)

        result = await article_provider.validate_v1_path(path)
        expected = FigsharePath('/' + item['name'], _ids=('', file_id), folder=False,
                                is_public=False)

        assert result == expected


class TestProjectV0ValidatePath:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v0_path_folder_article(self, project_provider, root_provider_fixtures):
        item = root_provider_fixtures['folder_article_metadata']
        file_id = str(item['id'])
        path = '/{}/'.format(file_id)
        article_list_url = project_provider.build_url(False, *project_provider.root_path_parts,
                                                      'articles')
        article_segments = (*project_provider.root_path_parts, 'articles', str(item['id']))
        article_url = project_provider.build_url(False, *article_segments)

        aiohttpretty.register_json_uri('GET', article_list_url,
                                       body=root_provider_fixtures['list_project_articles'],
                                       params={'page': '1', 'page_size': str(MAX_PAGE_SIZE)})
        aiohttpretty.register_json_uri('GET', article_list_url, body=[],
                                       params={'page': '2', 'page_size': str(MAX_PAGE_SIZE)})
        aiohttpretty.register_json_uri('GET', article_url, body=item)

        result = await project_provider.validate_path(path)
        expected = FigsharePath('/{}/'.format(item['title']),
                                _ids=(project_provider.container_id, file_id),
                                folder=True,
                                is_public=False)

        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v0_path_folder_article_bad_path(self, project_provider,
                                                            root_provider_fixtures):
        bad_article_id = '000000000'
        path = '/{}'.format(bad_article_id)

        article_list_url = project_provider.build_url(False, *project_provider.root_path_parts,
                                                      'articles')
        article_segments = (*project_provider.root_path_parts, 'articles', bad_article_id)
        article_url = project_provider.build_url(False, *article_segments)

        aiohttpretty.register_json_uri('GET', article_list_url,
                                       body=root_provider_fixtures['list_project_articles'],
                                       params={'page': '1', 'page_size': str(MAX_PAGE_SIZE)})
        aiohttpretty.register_json_uri('GET', article_list_url, body=[],
                                       params={'page': '2', 'page_size': str(MAX_PAGE_SIZE)})
        aiohttpretty.register_json_uri('GET', article_url, status=404)

        result = await project_provider.validate_path(path)
        expected = FigsharePath(path, _ids=('', ''), folder=True, is_public=False)
        assert result == expected
        assert aiohttpretty.has_call(method='GET', uri=article_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_project_validate_v0_path_root(self, project_provider):
        path = '/'

        result = await project_provider.validate_path(path)
        expected = FigsharePath(path, _ids=('', ), folder=True, is_public=False)

        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_article_validate_v0_path_invalid_path(self, article_provider):
        with pytest.raises(exceptions.InvalidPathError) as e:
            await article_provider.validate_path('/this/is/an/invalid/path')

        assert e.value.code == 400

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v0_path_invalid_path(self, project_provider):
        path = 'whatever'

        with pytest.raises(exceptions.InvalidPathError) as e:
            await project_provider.validate_path(path)

        assert e.value.code == 400

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v0_path_file_article(self, project_provider, root_provider_fixtures):
        file_item = root_provider_fixtures['file_metadata']
        item = root_provider_fixtures['file_article_metadata']
        file_id = str(item['files'][0]['id'])
        article_id = str(item['id'])
        path = '/{}/{}'.format(article_id, file_id)
        article_list_url = project_provider.build_url(False, *project_provider.root_path_parts,
                                                      'articles')
        article_segments = (*project_provider.root_path_parts, 'articles', str(item['id']))
        article_url = project_provider.build_url(False, *article_segments, 'files', file_id)

        aiohttpretty.register_json_uri('GET', article_list_url,
                                       body=root_provider_fixtures['list_project_articles'],
                                       params={'page': '1', 'page_size': str(MAX_PAGE_SIZE)})

        aiohttpretty.register_json_uri('GET', article_list_url, body=[],
                                       params={'page': '2', 'page_size': str(MAX_PAGE_SIZE)})

        aiohttpretty.register_json_uri('GET', article_url, body=file_item)

        result = await project_provider.validate_path(path)
        expected = FigsharePath('/{}/{}'.format(item['title'], file_item['name']),
                                _ids=(project_provider.container_id, file_id),
                                folder=False, is_public=False)

        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v0_path_file_article_public(self, project_provider,
                                                        root_provider_fixtures):
        file_item = root_provider_fixtures['file_metadata_public']
        item = root_provider_fixtures['file_article_metadata']
        file_id = str(file_item['id'])
        article_id = str(item['id'])
        path = '/{}/{}'.format(article_id, file_id)
        article_list_url = project_provider.build_url(False, *project_provider.root_path_parts,
                                                      'articles')
        article_segments = (*project_provider.root_path_parts, 'articles', str(item['id']))
        article_url = project_provider.build_url(True, *article_segments, 'files', file_id)

        aiohttpretty.register_json_uri('GET', article_list_url,
                                       body=root_provider_fixtures['public_list_project_articles'],
                                       params={'page': '1', 'page_size': str(MAX_PAGE_SIZE)})

        aiohttpretty.register_json_uri('GET', article_list_url, body=[],
                                       params={'page': '2', 'page_size': str(MAX_PAGE_SIZE)})

        aiohttpretty.register_json_uri('GET', article_url, body=file_item)

        result = await project_provider.validate_path(path)
        expected = FigsharePath('/{}/{}'.format(item['title'], file_item['name']),
                                _ids=(project_provider.container_id, file_id),
                                folder=False, is_public=False)

        assert result == expected


class TestArticleV0ValidatePath:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_article_validate_v0_path_root(self, article_provider):
        path = '/'

        result = await article_provider.validate_path(path)
        expected = FigsharePath(path, _ids=('', ), folder=True, is_public=False)

        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_article_validate_v0_path(self, article_provider, root_provider_fixtures):
        item = root_provider_fixtures['file_metadata']
        file_id = item['id']
        path = '/' + str(file_id)
        url = article_provider.build_url(False, *article_provider.root_path_parts, 'files',
                                         str(file_id))

        aiohttpretty.register_json_uri('GET', url, body=item)

        result = await article_provider.validate_path(path)
        expected = FigsharePath('/' + item['name'], _ids=('', file_id), folder=False,
                                is_public=False)

        assert result == expected


class TestProjectMetadata:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_project_contents(self,
                                    project_provider_2,
                                    project_list_articles,
                                    project_article_type_1_metadata,
                                    project_article_type_3_metadata):
        """Test content listings for a project root.
        """

        root_parts = project_provider_2.root_path_parts

        # Register the requests that retrieve the article list of a project.
        list_articles_url = project_provider_2.build_url(False, *root_parts, 'articles')
        aiohttpretty.register_json_uri('GET', list_articles_url,
                                       body=project_list_articles['page1'],
                                       params={'page': '1', 'page_size': str(MAX_PAGE_SIZE)})
        aiohttpretty.register_json_uri('GET', list_articles_url,
                                       body=project_list_articles['page2'],
                                       params={'page': '2', 'page_size': str(MAX_PAGE_SIZE)})
        aiohttpretty.register_json_uri('GET', list_articles_url,
                                       body=[],
                                       params={'page': '3', 'page_size': str(MAX_PAGE_SIZE)})

        # Register the requests that retrieve the metadata for each item in the article list.
        article_id_1 = str(project_list_articles['page1'][0]['id'])
        article_url_1 = project_provider_2.build_url(False, *root_parts, 'articles', article_id_1)
        article_meta_1 = project_article_type_1_metadata['private']
        aiohttpretty.register_json_uri('GET', article_url_1, body=article_meta_1)

        article_id_2 = str(project_list_articles['page1'][1]['id'])
        article_url_2 = project_provider_2.build_url(False, *root_parts, 'articles', article_id_2)
        article_meta_2 = project_article_type_1_metadata['public']
        aiohttpretty.register_json_uri('GET', article_url_2, body=article_meta_2)

        article_id_3 = str(project_list_articles['page2'][0]['id'])
        article_url_3 = project_provider_2.build_url(False, *root_parts, 'articles', article_id_3)
        article_meta_3 = project_article_type_3_metadata['private']
        aiohttpretty.register_json_uri('GET', article_url_3, body=article_meta_3)

        article_id_4 = str(project_list_articles['page2'][1]['id'])
        article_url_4 = project_provider_2.build_url(False, *root_parts, 'articles', article_id_4)
        article_meta_4 = project_article_type_3_metadata['public']
        aiohttpretty.register_json_uri('GET', article_url_4, body=article_meta_4)

        # The ``metadata()`` call to test
        path = FigsharePath('/', _ids=(''), folder=True)
        metadata_list = (await project_provider_2.metadata(path)).sort(key=lambda x: x.path)

        assert aiohttpretty.has_call(method='GET', uri=list_articles_url,
                                     params={'page': '1', 'page_size': str(MAX_PAGE_SIZE)})
        assert aiohttpretty.has_call(method='GET', uri=list_articles_url,
                                     params={'page': '2', 'page_size': str(MAX_PAGE_SIZE)})
        assert aiohttpretty.has_call(method='GET', uri=list_articles_url,
                                     params={'page': '3', 'page_size': str(MAX_PAGE_SIZE)})
        assert aiohttpretty.has_call(method='GET', uri=article_url_1)
        assert aiohttpretty.has_call(method='GET', uri=article_url_2)
        assert aiohttpretty.has_call(method='GET', uri=article_url_3)
        assert aiohttpretty.has_call(method='GET', uri=article_url_4)

        expected = ([
            metadata.FigshareFileMetadata(
                project_article_type_1_metadata['public'],
                raw_file=project_article_type_1_metadata['public']['files'][0]
            ),
            metadata.FigshareFileMetadata(
                project_article_type_1_metadata['private'],
                raw_file=project_article_type_1_metadata['private']['files'][0]
            ),
            metadata.FigshareFolderMetadata(project_article_type_3_metadata['public']),
            metadata.FigshareFolderMetadata(project_article_type_3_metadata['private'])
        ]).sort(key=lambda x: x.path)
        assert metadata_list == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_project_metadata_invalid_figshare_path(self, project_provider_2):
        """Test that figshare path can at most have three levels (including root itself).
        """
        path = FigsharePath('/folder_lvl_1/folder_lvl_2/file_lvl_3.txt',
                            _ids=('1', '2', '3', '4', ), folder=False, is_public=False)
        with pytest.raises(exceptions.NotFoundError) as e:
            await project_provider_2.metadata(path)
        assert e.value.code == 404

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_project_metadata_bad_article_response(self, project_provider_2):
        """Test handling 404 response for figshare article request.
        """
        root_parts = project_provider_2.root_path_parts
        path = FigsharePath('/article_name/file_name',
                            _ids=('1', '2', '3'), folder=False, is_public=False)
        article_url = project_provider_2.build_url(path.is_public, *root_parts,
                                                 'articles', path.parts[1].identifier)
        aiohttpretty.register_json_uri('GET', article_url, status=404)

        with pytest.raises(exceptions.NotFoundError) as e:
            await project_provider_2.metadata(path)

        assert aiohttpretty.has_call(method='GET', uri=article_url)
        assert e.value.code == 404

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_project_folder_article_file(self,
                                               project_provider_2,
                                               project_article_type_1_metadata,
                                               project_article_type_1_file_metadata):
        """Test metadata for a file that belongs to an article of file type.
        """
        root_parts = project_provider_2.root_path_parts

        article_meta_json = project_article_type_1_metadata['private']
        file_meta_json = project_article_type_1_file_metadata['private']
        article_id = str(article_meta_json['id'])
        article_name = article_meta_json['title']
        file_id = str(file_meta_json['id'])
        file_name = file_meta_json['name']

        article_meta_url = project_provider_2.build_url(False, *root_parts, 'articles', article_id)
        aiohttpretty.register_json_uri('GET', article_meta_url, body=article_meta_json)

        path = FigsharePath('/{}/{}'.format(article_name, file_name),
                            _ids=('', article_id, file_id), folder=False, is_public=False)
        result = await project_provider_2.metadata(path)
        expected = metadata.FigshareFileMetadata(article_meta_json, file_meta_json)

        assert aiohttpretty.has_call(method='GET', uri=article_meta_url)
        assert result == expected
        assert str(result.id) == file_id
        assert result.name == file_name
        assert result.path == '/{}/{}'.format(article_id, file_id)
        assert result.materialized_path == '/{}/{}'.format(article_name, file_name)
        assert str(result.article_id) == article_id
        assert result.article_name == article_name
        assert result.size == file_meta_json['size']
        assert result.is_public == (article_meta_json['published_date'] is not None)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_project_folder_article_file_not_found(self,
                                                         project_provider_2,
                                                         project_article_type_3_metadata,
                                                         project_article_type_3_file_metadata):
        """Test the error case where the file is not found in the article's file list.
        """
        root_parts = project_provider_2.root_path_parts

        article_meta_json = project_article_type_3_metadata['private']
        article_meta_json['files'] = []
        article_id = str(article_meta_json['id'])
        article_name = article_meta_json['title']
        file_id = str(project_article_type_3_file_metadata['private']['id'])
        file_name = project_article_type_3_file_metadata['private']['name']

        article_meta_url = project_provider_2.build_url(False, *root_parts, 'articles', article_id)
        aiohttpretty.register_json_uri('GET', article_meta_url, body=article_meta_json)

        path = FigsharePath('/{}/{}'.format(article_name, file_name),
                            _ids=('', article_id, file_id), folder=False, is_public=False)
        with pytest.raises(exceptions.NotFoundError) as e:
            await project_provider_2.metadata(path)

        assert aiohttpretty.has_call(method='GET', uri=article_meta_url)
        assert e.value.code == 404

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_project_folder_article_type_error(self,
                                                     project_provider,
                                                     project_article_type_3_metadata):
        """Test the error case where the folder article is of a wrong type.
        """
        root_parts = project_provider.root_path_parts

        article_meta_json = project_article_type_3_metadata['private']
        article_meta_json['defined_type'] = 15
        article_id = str(article_meta_json['id'])
        article_name = article_meta_json['title']

        article_meta_url = project_provider.build_url(False, *root_parts, 'articles', article_id)
        aiohttpretty.register_json_uri('GET', article_meta_url, body=article_meta_json)

        path = FigsharePath('/{}'.format(article_name),
                            _ids=('', article_id), folder=True, is_public=False)
        with pytest.raises(exceptions.NotFoundError) as e:
            await project_provider.metadata(path)

        assert aiohttpretty.has_call(method='GET', uri=article_meta_url)
        assert e.value.code == 404

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_project_folder_article_contents(self,
                                                   project_provider_2,
                                                   project_article_type_3_metadata):
        """Test content listing for an article of folder type.
        """
        root_parts = project_provider_2.root_path_parts

        article_meta_json = project_article_type_3_metadata['private']
        article_id = str(article_meta_json['id'])
        article_name = article_meta_json['title']

        article_meta_url = project_provider_2.build_url(False, *root_parts, 'articles', article_id)
        aiohttpretty.register_json_uri('GET', article_meta_url, body=article_meta_json)

        path = FigsharePath('/{}'.format(article_name), _ids=('', article_id), folder=True,
                            is_public=False)
        result = (await project_provider_2.metadata(path)).sort(key=lambda x: x.path)
        expected = ([
            metadata.FigshareFileMetadata(article_meta_json,
                                          raw_file=article_meta_json['files'][0]),
            metadata.FigshareFileMetadata(article_meta_json,
                                          raw_file=article_meta_json['files'][1])
        ]).sort(key=lambda x: x.path)

        assert aiohttpretty.has_call(method='GET', uri=article_meta_url)
        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_project_folder_article_file(self,
                                               project_provider_2,
                                               project_article_type_3_metadata,
                                               project_article_type_3_file_metadata):
        """Test metadata for a file that belongs to an article of folder type.
        """
        root_parts = project_provider_2.root_path_parts

        article_meta_json = project_article_type_3_metadata['private']
        file_meta_json = project_article_type_3_file_metadata['private']
        article_id = str(article_meta_json['id'])
        article_name = article_meta_json['title']
        file_id = str(file_meta_json['id'])
        file_name = file_meta_json['name']

        article_meta_url = project_provider_2.build_url(False, *root_parts, 'articles', article_id)
        aiohttpretty.register_json_uri('GET', article_meta_url, body=article_meta_json)

        path = FigsharePath('/{}/{}'.format(article_name, file_name),
                            _ids=('', article_id, file_id), folder=False, is_public=False)
        result = await project_provider_2.metadata(path)

        assert aiohttpretty.has_call(method='GET', uri=article_meta_url)
        expected = metadata.FigshareFileMetadata(article_meta_json, file_meta_json)
        assert result == expected
        assert str(result.id) == file_id
        assert result.name == file_name
        assert result.path == '/{}/{}'.format(article_id, file_id)
        assert result.materialized_path == '/{}/{}'.format(article_name, file_name)
        assert str(result.article_id) == article_id
        assert result.article_name == article_name
        assert result.size == file_meta_json['size']
        assert result.is_public == (article_meta_json['published_date'] is not None)
        assert result.extra['hashes']['md5'] == '68c3a15be1ddc27893c17eaab61f2d3d'


class TestArticleMetadata:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_article_file_contents(self, article_provider, root_provider_fixtures):

        article_metadata = root_provider_fixtures['folder_article_metadata']
        file_metadata = root_provider_fixtures['folder_file_metadata']

        root_parts = article_provider.root_path_parts
        article_id = str(article_metadata['id'])
        article_name = article_metadata['title']
        file_id = str(file_metadata['id'])
        file_name = file_metadata['name']

        folder_article_metadata_url = article_provider.build_url(False, *root_parts)

        aiohttpretty.register_json_uri('GET', folder_article_metadata_url, body=article_metadata)

        path = FigsharePath('/{}'.format(file_name), _ids=('', file_id), folder=False,
                            is_public=False)

        result = await article_provider.metadata(path)

        assert aiohttpretty.has_call(method='GET', uri=folder_article_metadata_url)

        expected = metadata.FigshareFileMetadata(article_metadata, file_metadata)
        assert result == expected

        assert str(result.id) == file_id
        assert result.name == file_name
        assert result.path == '/{}/{}'.format(article_id, file_id)
        assert result.materialized_path == '/{}/{}'.format(article_name, file_name)
        assert result.article_name == article_name
        assert result.size == file_metadata['size']
        assert result.is_public is False

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_article_root_contents(self, article_provider, root_provider_fixtures):

        article_metadata = root_provider_fixtures['folder_article_metadata']
        file_metadata = root_provider_fixtures['folder_file_metadata']

        root_parts = article_provider.root_path_parts
        file_id = str(file_metadata['id'])

        folder_article_metadata_url = article_provider.build_url(False, *root_parts)
        file_metadata_url = article_provider.build_url(False, *root_parts, 'files', file_id)

        aiohttpretty.register_json_uri('GET', folder_article_metadata_url, body=article_metadata)
        aiohttpretty.register_json_uri('GET', file_metadata_url, body=file_metadata)

        path = FigsharePath('/', _ids=(file_id, ), folder=True, is_public=False)
        result = await article_provider.metadata(path)

        expected = [metadata.FigshareFileMetadata(article_metadata, file_metadata)]
        assert result == expected


class TestProjectCRUD:
    """Due to a bug in aiohttpretty, the file stream is not being read from on file upload for the
    Figshare provider.  Because the file stream isn't read, the stream hash calculator never gets
    any data, and the computed md5sum is always that of the empty string.  To work around this, the
    fixtures currently include the empty md5 in the metadata.  Once aiohttpretty is fixed, the
    metadata can be reverted to deliver the actual content hash."""

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_project_upload(self, file_stream, project_provider,
                                root_provider_fixtures, crud_fixtures):
        file_name = 'barricade.gif'
        path = FigsharePath('/' + file_name, _ids=('', ''), folder=False, is_public=False)

        root_parts = project_provider.root_path_parts
        article_id = str(crud_fixtures['upload_article_metadata']['id'])
        file_metadata = root_provider_fixtures['get_file_metadata']
        create_article_url = project_provider.build_url(False, *root_parts, 'articles')
        create_file_url = project_provider.build_url(False, 'articles', article_id, 'files')
        file_url = project_provider.build_url(False, 'articles', article_id, 'files',
                                              str(file_metadata['id']))
        get_article_url = project_provider.build_url(False, *root_parts, 'articles', article_id)
        upload_url = file_metadata['upload_url']

        aiohttpretty.register_json_uri('POST', create_article_url,
                                       body=crud_fixtures['create_article_metadata'], status=201)
        aiohttpretty.register_json_uri('POST', create_file_url,
                                       body=crud_fixtures['create_file_metadata'], status=201)
        aiohttpretty.register_json_uri('GET', file_url,
                                       body=file_metadata)
        aiohttpretty.register_json_uri('GET', upload_url,
                                       body=root_provider_fixtures['get_upload_metadata'])
        aiohttpretty.register_uri('PUT', '{}/1'.format(upload_url), status=200)
        aiohttpretty.register_uri('POST', file_url, status=202)
        aiohttpretty.register_json_uri('GET', get_article_url,
                                       body=crud_fixtures['upload_article_metadata'])

        # md5 hash calculation is being hacked around.  see test class docstring
        result, created = await project_provider.upload(file_stream, path)
        expected = metadata.FigshareFileMetadata(
            crud_fixtures['upload_article_metadata'],
            crud_fixtures['upload_article_metadata']['files'][0],
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
    async def test_project_upload_checksum_mismatch(self, project_provider,
                                                    root_provider_fixtures,
                                                    crud_fixtures, file_stream):
        file_name = 'barricade.gif'
        item = root_provider_fixtures['get_file_metadata']
        root_parts = project_provider.root_path_parts

        path = FigsharePath('/' + file_name, _ids=('', ''), folder=False, is_public=False)

        article_id = str(crud_fixtures['checksum_mismatch_article_metadata']['id'])
        create_article_url = project_provider.build_url(False, *root_parts, 'articles')
        create_file_url = project_provider.build_url(False, 'articles', article_id, 'files')
        file_url = project_provider.build_url(False, 'articles', article_id, 'files',
                                              str(item['id']))
        get_article_url = project_provider.build_url(False, *root_parts, 'articles', article_id)
        upload_url = item['upload_url']

        aiohttpretty.register_json_uri('POST', create_article_url,
                                       body=crud_fixtures['create_article_metadata'], status=201)
        aiohttpretty.register_json_uri('POST', create_file_url,
                                       body=crud_fixtures['create_file_metadata'], status=201)
        aiohttpretty.register_json_uri('GET', file_url, body=item)
        aiohttpretty.register_json_uri('GET', upload_url,
                                       body=root_provider_fixtures['get_upload_metadata'])
        aiohttpretty.register_uri('PUT', '{}/1'.format(upload_url), status=200)
        aiohttpretty.register_uri('POST', file_url, status=202)
        aiohttpretty.register_json_uri('GET', get_article_url,
                                       body=crud_fixtures['checksum_mismatch_article_metadata'])

        with pytest.raises(exceptions.UploadChecksumMismatchError):
            await project_provider.upload(file_stream, path)

        assert aiohttpretty.has_call(
            method='POST',
            uri=create_article_url,
            data=json.dumps({
                'title': 'barricade.gif',
            })
        )
        assert aiohttpretty.has_call(method='POST', uri=create_file_url)
        assert aiohttpretty.has_call(method='GET', uri=file_url)
        assert aiohttpretty.has_call(method='GET', uri=upload_url)
        assert aiohttpretty.has_call(method='PUT', uri='{}/1'.format(upload_url))
        assert aiohttpretty.has_call(method='POST', uri=file_url)
        assert aiohttpretty.has_call(method='GET', uri=get_article_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_project_folder_upload(self, file_stream,
                                            project_provider,
                                            root_provider_fixtures,
                                            crud_fixtures):
        file_name = 'barricade.gif'
        article_id = str(root_provider_fixtures['list_project_articles'][1]['id'])
        article_name = root_provider_fixtures['list_project_articles'][1]['title']
        root_parts = project_provider.root_path_parts

        path = FigsharePath('/{}/{}'.format(article_name, file_name),
                            _ids=('', article_id, ''), folder=False, is_public=False)

        file_metadata = root_provider_fixtures['get_file_metadata']
        create_file_url = project_provider.build_url(False, 'articles', article_id, 'files')
        file_url = project_provider.build_url(False, 'articles', article_id, 'files',
                                              str(file_metadata['id']))
        get_article_url = project_provider.build_url(False, *root_parts, 'articles', article_id)
        upload_url = file_metadata['upload_url']

        aiohttpretty.register_json_uri('POST', create_file_url,
                                       body=crud_fixtures['create_file_metadata'], status=201)
        aiohttpretty.register_json_uri('GET', file_url, body=file_metadata)
        aiohttpretty.register_json_uri('GET', upload_url,
                                       body=root_provider_fixtures['get_upload_metadata'])
        aiohttpretty.register_uri('PUT', '{}/1'.format(upload_url), status=200)
        aiohttpretty.register_uri('POST', file_url, status=202)
        aiohttpretty.register_json_uri('GET', get_article_url,
                                       body=crud_fixtures['upload_folder_article_metadata'])

        # md5 hash calculation is being hacked around.  see test class docstring
        result, created = await project_provider.upload(file_stream, path)
        expected = metadata.FigshareFileMetadata(
            crud_fixtures['upload_folder_article_metadata'],
            crud_fixtures['upload_folder_article_metadata']['files'][0],
        )
        assert aiohttpretty.has_call(method='PUT', uri='{}/1'.format(upload_url))
        assert aiohttpretty.has_call(method='POST', uri=create_file_url)
        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_project_folder_upload_undefined_type(self, file_stream,
                                                        project_provider,
                                                        root_provider_fixtures,
                                                        crud_fixtures):
        file_name = 'barricade.gif'
        article_id = str(root_provider_fixtures['list_project_articles'][1]['id'])
        article_name = root_provider_fixtures['list_project_articles'][1]['title']
        changed_metadata = crud_fixtures['upload_folder_article_metadata']
        changed_metadata['defined_type'] = 5
        root_parts = project_provider.root_path_parts

        list_articles_url = project_provider.build_url(False, *root_parts, 'articles')

        aiohttpretty.register_json_uri('POST', list_articles_url, status=201,
                                       body=crud_fixtures['create_upload_article_metadata'])

        path = FigsharePath('/{}/{}'.format(article_name, file_name), _ids=('', article_id, ''),
                            folder=False, is_public=False)

        file_metadata = root_provider_fixtures['get_file_metadata']
        create_file_url = project_provider.build_url(False, 'articles', article_id, 'files')
        file_url = project_provider.build_url(False, 'articles', article_id, 'files',
                                              str(file_metadata['id']))
        get_article_url = project_provider.build_url(False, *root_parts, 'articles', article_id)
        upload_url = file_metadata['upload_url']

        aiohttpretty.register_json_uri('POST', create_file_url,
                                       body=crud_fixtures['create_file_metadata'], status=201)
        aiohttpretty.register_json_uri('GET', file_url, body=file_metadata)
        aiohttpretty.register_json_uri('GET', upload_url,
                                       body=root_provider_fixtures['get_upload_metadata'])
        aiohttpretty.register_uri('PUT', '{}/1'.format(upload_url), status=200)
        aiohttpretty.register_uri('POST', file_url, status=202)
        aiohttpretty.register_json_uri('GET', get_article_url, body=changed_metadata)

        result, created = await project_provider.upload(file_stream, path)
        expected = metadata.FigshareFileMetadata(
            crud_fixtures['upload_folder_article_metadata'],
            crud_fixtures['upload_folder_article_metadata']['files'][0],
        )
        assert aiohttpretty.has_call(method='PUT', uri='{}/1'.format(upload_url))
        assert aiohttpretty.has_call(method='POST', uri=create_file_url)
        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_project_upload_update_error(self, file_stream, project_provider):
        path = FigsharePath('/testfolder/whatever.txt',
                            _ids=('512415', '123325', '8890481'),
                            folder=False, is_public=False)

        with pytest.raises(exceptions.UnsupportedOperationError) as e:
            await project_provider.upload(file_stream, path)

        assert e.value.code == 403

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_project_article_download(self, project_provider, root_provider_fixtures):
        article_id = str(root_provider_fixtures['list_project_articles'][0]['id'])
        file_id = str(root_provider_fixtures['file_article_metadata']['files'][0]['id'])
        article_name = str(root_provider_fixtures['list_project_articles'][0]['title'])
        file_name = str(root_provider_fixtures['file_article_metadata']['files'][0]['name'])
        body = b'castle on a cloud'
        root_parts = project_provider.root_path_parts

        file_metadata_url = project_provider.build_url(False, *root_parts, 'articles', article_id,
                                                       'files', file_id)
        article_metadata_url = project_provider.build_url(False, *root_parts, 'articles',
                                                          article_id)
        download_url = root_provider_fixtures['file_metadata']['download_url']

        aiohttpretty.register_json_uri('GET', file_metadata_url,
                                       body=root_provider_fixtures['file_metadata'])
        aiohttpretty.register_json_uri('GET', article_metadata_url,
                                       body=root_provider_fixtures['file_article_metadata'])
        aiohttpretty.register_uri('GET', download_url,
                                  params={'token': project_provider.token},
                                  body=body, auto_length=True)

        path = FigsharePath('/{}/{}'.format(article_name, file_name),
                            _ids=('', article_id, file_id),
                            folder=False, is_public=False)

        result = await project_provider.download(path)
        content = await result.read()

        assert content == body

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_project_file_delete(self, project_provider, root_provider_fixtures):
        file_id = str(root_provider_fixtures['file_metadata']['id'])
        file_name = root_provider_fixtures['file_metadata']['name']
        article_id = str(root_provider_fixtures['list_project_articles'][0]['id'])
        article_name = str(root_provider_fixtures['list_project_articles'][0]['title'])
        root_parts = project_provider.root_path_parts

        file_url = project_provider.build_url(False, *root_parts,
                                              'articles', article_id, 'files', file_id)
        file_article_url = project_provider.build_url(False, *root_parts, 'articles', article_id)

        aiohttpretty.register_json_uri('GET', file_url,
                                       body=root_provider_fixtures['file_metadata'])
        aiohttpretty.register_json_uri('GET', file_article_url,
                                       body=root_provider_fixtures['file_article_metadata'])
        aiohttpretty.register_uri('DELETE', file_article_url, status=204)

        path = FigsharePath('/{}/{}'.format(article_name, file_name),
                            _ids=('', article_id, file_id),
                            folder=False, is_public=False)

        result = await project_provider.delete(path)

        assert result is None
        assert aiohttpretty.has_call(method='DELETE', uri=file_article_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_project_file_delete_folder_type(self, project_provider, root_provider_fixtures):
        item = root_provider_fixtures['file_article_metadata']
        item['defined_type'] = 4
        file_id = str(root_provider_fixtures['file_metadata']['id'])
        article_id = str(root_provider_fixtures['list_project_articles'][0]['id'])
        root_parts = project_provider.root_path_parts
        path = FigsharePath('/{}/{}'.format(article_id, file_id),
                            _ids=('', article_id, file_id), folder=False)

        list_articles_url = project_provider.build_url(False, *root_parts, 'articles')
        file_article_url = project_provider.build_url(False, 'articles', path.parts[1]._id,
                                                      'files', path.parts[2]._id)
        get_file_article_url = project_provider.build_url(False,
                                                          *root_parts, 'articles', article_id)

        aiohttpretty.register_json_uri('GET', list_articles_url,
                                       body=root_provider_fixtures['list_project_articles'],
                                       params={'page': '1', 'page_size': str(MAX_PAGE_SIZE)})
        aiohttpretty.register_json_uri('GET', list_articles_url, body=[],
                                       params={'page': '2', 'page_size': str(MAX_PAGE_SIZE)})
        aiohttpretty.register_json_uri('GET', get_file_article_url, body=item)
        aiohttpretty.register_uri('DELETE', file_article_url, status=204)

        result = await project_provider.delete(path)

        assert result is None
        assert aiohttpretty.has_call(method='DELETE', uri=file_article_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_project_file_delete_bad_path(self, project_provider, root_provider_fixtures):
        file_name = str(root_provider_fixtures['file_metadata']['name'])
        article_name = str(root_provider_fixtures['list_project_articles'][0]['title'])
        path = FigsharePath('/{}/{}'.format(article_name, file_name), _ids=('',), folder=False)

        with pytest.raises(exceptions.NotFoundError) as e:
            await project_provider.delete(path)

        assert e.value.code == 404

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_project_folder_delete(self, project_provider, root_provider_fixtures):
        article_id = str(root_provider_fixtures['list_project_articles'][1]['id'])
        root_parts = project_provider.root_path_parts

        list_articles_url = project_provider.build_url(False, *root_parts, 'articles')
        folder_article_url = project_provider.build_url(False, *root_parts, 'articles', article_id)

        aiohttpretty.register_json_uri('GET', list_articles_url,
                                       body=root_provider_fixtures['list_project_articles'],
                                       params={'page': '1', 'page_size': str(MAX_PAGE_SIZE)})
        aiohttpretty.register_json_uri('GET', list_articles_url, body=[],
                                       params={'page': '2', 'page_size': str(MAX_PAGE_SIZE)})
        aiohttpretty.register_json_uri('GET', folder_article_url,
                                       body=root_provider_fixtures['folder_article_metadata'])
        aiohttpretty.register_uri('DELETE', folder_article_url, status=204)

        path = FigsharePath('/{}'.format(article_id), _ids=('', article_id), folder=True)

        result = await project_provider.delete(path)

        assert result is None
        assert aiohttpretty.has_call(method='DELETE', uri=folder_article_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_project_delete_root_confirm_error(self, project_provider):
        path = FigsharePath('/', _ids=('11241213', ), folder=True, is_public=False)

        with pytest.raises(exceptions.DeleteError) as e:
            await project_provider.delete(path)

        assert e.value.code == 400

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_project_delete_root(self, project_provider, root_provider_fixtures):
        path = FigsharePath('/', _ids=('11241213', ), folder=True, is_public=False)
        item = root_provider_fixtures['list_project_articles']
        list_articles_url = project_provider.build_url(False,
                                                       *project_provider.root_path_parts,
                                                       'articles')
        delete_url_1 = project_provider.build_url(False, *project_provider.root_path_parts,
                                                  'articles', str(item[0]['id']))
        delete_url_2 = project_provider.build_url(False, *project_provider.root_path_parts,
                                                  'articles', str(item[1]['id']))

        aiohttpretty.register_json_uri('DELETE', delete_url_1, status=204)
        aiohttpretty.register_json_uri('DELETE', delete_url_2, status=204)

        aiohttpretty.register_json_uri('GET', list_articles_url,
                                       body=root_provider_fixtures['list_project_articles'],
                                       params={'page': '1', 'page_size': str(MAX_PAGE_SIZE)})

        aiohttpretty.register_json_uri('GET', list_articles_url, body=[],
                                       params={'page': '2', 'page_size': str(MAX_PAGE_SIZE)})

        await project_provider.delete(path, 1)
        assert aiohttpretty.has_call(method='DELETE', uri=delete_url_2)
        assert aiohttpretty.has_call(method='DELETE', uri=delete_url_1)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_project_delete_errors(self, project_provider):
        path = FigsharePath('/test.txt', _ids=('11241213', '123123'), folder=False, is_public=False)

        with pytest.raises(exceptions.NotFoundError) as e:
            await project_provider.delete(path)

        assert e.value.code == 404

        path = FigsharePath('/test/test.txt', _ids=('11241213', '123123', '123123'),
                            folder=True, is_public=False)

        with pytest.raises(exceptions.NotFoundError) as e:
            await project_provider.delete(path)

        assert e.value.code == 404

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_create_folder(self, project_provider, root_provider_fixtures, crud_fixtures):
        item = root_provider_fixtures['file_metadata']
        file_id = str(item['id'])
        path = FigsharePath('/folder2/', _ids=('', file_id), folder=True)

        create_url = project_provider.build_url(False,
                                        *project_provider.root_path_parts, 'articles')
        metadata_url = crud_fixtures['create_article_metadata']['location']

        aiohttpretty.register_json_uri('POST', create_url,
                                       body=crud_fixtures['create_article_metadata'], status=201)
        aiohttpretty.register_json_uri('GET', metadata_url,
                                       body=root_provider_fixtures['folder_article_metadata'])

        result = await project_provider.create_folder(path)
        assert result is not None

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_create_folder_invalid_path(self, project_provider, root_provider_fixtures):
        item = root_provider_fixtures['file_metadata']
        file_id = str(item['id'])
        path = FigsharePath('/folder2/folder3/folder4/folder5',
                            _ids=('', file_id, file_id, file_id), folder=True)

        with pytest.raises(exceptions.CreateFolderError) as e:
            await project_provider.create_folder(path)

        assert e.value.code == 400


class TestArticleCRUD:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_article_upload(self, file_stream, article_provider,
                                root_provider_fixtures, crud_fixtures):
        file_name = 'barricade.gif'
        file_id = str(root_provider_fixtures['get_file_metadata']['id'])
        root_parts = article_provider.root_path_parts

        path = FigsharePath('/' + file_name, _ids=('', ''), folder=False, is_public=False)

        create_file_url = article_provider.build_url(False, *root_parts, 'files')
        file_url = article_provider.build_url(False, *root_parts, 'files', file_id)
        get_article_url = article_provider.build_url(False, *root_parts)
        upload_url = root_provider_fixtures['get_file_metadata']['upload_url']

        aiohttpretty.register_json_uri('POST', create_file_url,
                                       body=crud_fixtures['create_file_metadata'], status=201)
        aiohttpretty.register_json_uri('GET', file_url,
                                       body=root_provider_fixtures['get_file_metadata'])
        aiohttpretty.register_json_uri('GET',
                                       root_provider_fixtures['get_file_metadata']['upload_url'],
                                       body=root_provider_fixtures['get_upload_metadata'])
        aiohttpretty.register_uri('PUT', '{}/1'.format(upload_url), status=200)
        aiohttpretty.register_uri('POST', file_url, status=202)
        aiohttpretty.register_json_uri('GET', get_article_url,
                                       body=crud_fixtures['upload_folder_article_metadata'])

        # md5 hash calculation is being hacked around.  see test class docstring
        result, created = await article_provider.upload(file_stream, path)
        expected = metadata.FigshareFileMetadata(
            crud_fixtures['upload_folder_article_metadata'],
            crud_fixtures['upload_folder_article_metadata']['files'][0],
        )
        assert aiohttpretty.has_call(method='PUT', uri='{}/1'.format(upload_url))
        assert aiohttpretty.has_call(method='POST', uri=create_file_url)
        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_article_download(self, article_provider, root_provider_fixtures):
        item = root_provider_fixtures['file_metadata']
        file_id = str(item['id'])
        file_name = str(item['name'])
        body = b'castle on a cloud'
        root_parts = article_provider.root_path_parts

        article_metadata_url = article_provider.build_url(False, *root_parts)
        download_url = item['download_url']

        aiohttpretty.register_json_uri('GET', article_metadata_url,
                                       body=root_provider_fixtures['file_article_metadata'])
        aiohttpretty.register_uri('GET', download_url, params={'token': article_provider.token},
                                  body=body, auto_length=True)

        path = FigsharePath('/{}'.format(file_name), _ids=('', file_id),
                            folder=False, is_public=False)

        result = await article_provider.download(path)
        content = await result.read()
        assert content == body

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_article_download_range(self, article_provider, root_provider_fixtures):
        item = root_provider_fixtures['file_metadata']
        file_id = str(item['id'])
        file_name = str(item['name'])
        body = b'castle on a cloud'
        root_parts = article_provider.root_path_parts

        article_metadata_url = article_provider.build_url(False, *root_parts)
        download_url = item['download_url']

        aiohttpretty.register_json_uri('GET', article_metadata_url,
                                       body=root_provider_fixtures['file_article_metadata'])
        aiohttpretty.register_uri('GET', download_url, params={'token': article_provider.token},
                                  body=body[0:2], auto_length=True, status=206)

        path = FigsharePath('/{}'.format(file_name), _ids=('', file_id),
                            folder=False, is_public=False)

        result = await article_provider.download(path, range=(0, 1))
        assert result.partial
        content = await result.read()
        assert content == b'ca'

        assert aiohttpretty.has_call(method='GET', uri=download_url,
                                     headers={'Range': 'bytes=0-1',
                                              'Authorization': 'token freddie'},
                                     params={'token': 'freddie'})

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_article_download_path_not_file(self, article_provider, root_provider_fixtures):
        path = FigsharePath('/testfolder/', _ids=('', ), folder=True, is_public=False)

        with pytest.raises(exceptions.NotFoundError) as e:
            await article_provider.download(path)
        assert e.value.code == 404
        assert e.value.message == 'Could not retrieve file or directory /{}'.format(path.path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_article_download_no_downloadurl(self, article_provider, error_fixtures):
        item = error_fixtures['file_metadata_missing_download_url']
        file_id = str(item['id'])
        path = FigsharePath('/{}'.format(file_id), _ids=('', file_id), folder=False)
        root_parts = article_provider.root_path_parts

        file_metadata_url = article_provider.build_url(False, *root_parts, 'files', file_id)
        article_metadata_url = article_provider.build_url(False, *root_parts)

        missing_download_url = error_fixtures['file_article_metadata_missing_download_url']
        aiohttpretty.register_json_uri('GET', file_metadata_url, body=item)
        aiohttpretty.register_json_uri('GET', article_metadata_url, body=missing_download_url)

        with pytest.raises(exceptions.DownloadError) as e:
            await article_provider.download(path)
        assert e.value.code == 403
        assert e.value.message == 'Download not available'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_article_upload_checksum_mismatch(self, file_stream, article_provider,
                                                    root_provider_fixtures, crud_fixtures):

        file_name = 'barricade.gif'
        item = root_provider_fixtures['get_file_metadata']
        file_id = str(item['id'])
        root_parts = article_provider.root_path_parts

        validate_file_url = article_provider.build_url(False, *root_parts, 'files', file_name)

        aiohttpretty.register_uri('GET', validate_file_url, status=404)
        path = FigsharePath('/' + file_name, _ids=('', file_id), folder=False, is_public=False)

        create_file_url = article_provider.build_url(False, *root_parts, 'files')
        file_url = article_provider.build_url(False, *root_parts, 'files', file_id)
        get_article_url = article_provider.build_url(False, *root_parts)
        upload_url = item['upload_url']

        aiohttpretty.register_json_uri('POST', create_file_url,
                                       body=crud_fixtures['create_file_metadata'], status=201)
        aiohttpretty.register_json_uri('GET', file_url, body=item)
        aiohttpretty.register_json_uri('GET', item['upload_url'],
                                       body=root_provider_fixtures['get_upload_metadata'])
        aiohttpretty.register_uri('PUT', '{}/1'.format(upload_url), status=200)
        aiohttpretty.register_uri('POST', file_url, status=202)
        aiohttpretty.register_json_uri('GET', get_article_url,
                                       body=crud_fixtures['checksum_mismatch_folder_article_metadata'])

        with pytest.raises(exceptions.UploadChecksumMismatchError):
            await article_provider.upload(file_stream, path)

        assert aiohttpretty.has_call(method='POST', uri=create_file_url)
        assert aiohttpretty.has_call(method='GET', uri=file_url)
        assert aiohttpretty.has_call(method='GET', uri=item['upload_url'])
        assert aiohttpretty.has_call(method='PUT', uri='{}/1'.format(upload_url))
        assert aiohttpretty.has_call(method='POST', uri=file_url)
        assert aiohttpretty.has_call(method='GET', uri=get_article_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_article_file_delete_root_no_confirm(self, article_provider):
        path = FigsharePath('/', _ids=('11241213', ), folder=True, is_public=False)

        with pytest.raises(exceptions.DeleteError) as e:
            await article_provider.delete(path)
        assert e.value.code == 400

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_article_file_delete_root(self, article_provider, root_provider_fixtures):
        path = FigsharePath('/', _ids=('11241213', ), folder=True, is_public=False)
        item = root_provider_fixtures['file_article_metadata']

        list_articles_url = article_provider.build_url(False, *article_provider.root_path_parts)
        delete_url = article_provider.build_url(False, *article_provider.root_path_parts,
                                                'files', str(item['files'][0]['id']))

        aiohttpretty.register_json_uri('DELETE', delete_url, status=204)
        aiohttpretty.register_json_uri('GET', list_articles_url, body=item)

        await article_provider.delete(path, 1)

        assert aiohttpretty.has_call(method='DELETE', uri=delete_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_article_create_folder(self, article_provider):
        path = '/'
        with pytest.raises(exceptions.CreateFolderError) as e:
            await article_provider.create_folder(path)

        assert e.value.code == 400

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_article_file_delete(self, article_provider, root_provider_fixtures):
        file_id = str(root_provider_fixtures['file_metadata']['id'])
        file_name = root_provider_fixtures['file_metadata']['name']

        file_url = article_provider.build_url(False, *article_provider.root_path_parts, 'files',
                                              file_id)

        aiohttpretty.register_uri('DELETE', file_url, status=204)

        path = FigsharePath('/{}'.format(file_name), _ids=('', file_id), folder=False)

        result = await article_provider.delete(path)

        assert result is None
        assert aiohttpretty.has_call(method='DELETE', uri=file_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_article_download_404(self, article_provider, root_provider_fixtures):
        item = root_provider_fixtures['file_metadata']
        file_id = str(item['id'])
        path = FigsharePath('/{}'.format(file_id), _ids=('', file_id), folder=False)

        root_parts = article_provider.root_path_parts

        file_metadata_url = article_provider.build_url(False, *root_parts, 'files', file_id)
        article_metadata_url = article_provider.build_url(False, *root_parts)
        download_url = item['download_url']

        aiohttpretty.register_json_uri('GET', file_metadata_url, body=item)
        aiohttpretty.register_json_uri('GET', article_metadata_url,
                                       body=root_provider_fixtures['file_article_metadata'])
        aiohttpretty.register_uri('GET', download_url, params={'token': article_provider.token},
                                  status=404, auto_length=True)

        with pytest.raises(exceptions.DownloadError) as e:
            await article_provider.download(path)

        assert e.value.code == 404
        assert e.value.message == 'Download not available'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_article_upload_root(self, file_stream, article_provider,
                                    root_provider_fixtures, crud_fixtures):
        file_name = 'barricade.gif'
        file_id = str(root_provider_fixtures['get_file_metadata']['id'])
        root_parts = article_provider.root_path_parts
        item = crud_fixtures["upload_folder_article_metadata"]
        item['defined_type'] = 5
        validate_file_url = article_provider.build_url(False, *root_parts, 'files', file_name)

        aiohttpretty.register_uri('GET', validate_file_url, status=404)
        path = FigsharePath('/1234/94813', _ids=('1234', '94813'), folder=False, is_public=False)

        create_file_url = article_provider.build_url(False, *root_parts, 'files')
        file_url = article_provider.build_url(False, *root_parts, 'files', file_id)
        get_article_url = article_provider.build_url(False, *root_parts)
        upload_url = root_provider_fixtures['get_file_metadata']['upload_url']
        parent_url = article_provider.build_url(False, *root_parts,
                                            'articles', path.parent.identifier)

        aiohttpretty.register_json_uri('GET', parent_url, body=item)
        aiohttpretty.register_json_uri('POST', create_file_url,
                                       body=crud_fixtures['create_file_metadata'], status=201)
        aiohttpretty.register_json_uri('GET', file_url,
                                       body=root_provider_fixtures['get_file_metadata'])
        aiohttpretty.register_json_uri('GET',
                                       root_provider_fixtures['get_file_metadata']['upload_url'],
                                       body=root_provider_fixtures['get_upload_metadata'])
        aiohttpretty.register_uri('PUT', '{}/1'.format(upload_url), status=200)
        aiohttpretty.register_uri('POST', file_url, status=202)
        aiohttpretty.register_json_uri('GET', get_article_url,
                                       body=crud_fixtures['upload_folder_article_metadata'])

        result, created = await article_provider.upload(file_stream, path)
        expected = metadata.FigshareFileMetadata(
            crud_fixtures['upload_folder_article_metadata'],
            crud_fixtures['upload_folder_article_metadata']['files'][0],
        )
        assert aiohttpretty.has_call(method='PUT', uri='{}/1'.format(upload_url))
        assert aiohttpretty.has_call(method='POST', uri=create_file_url)
        assert result == expected


class TestRevalidatePath:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_project_revalidate_path(self, project_provider, root_provider_fixtures):
        file_article_id = str(root_provider_fixtures['list_project_articles'][0]['id'])
        folder_article_id = str(root_provider_fixtures['list_project_articles'][1]['id'])

        root_parts = project_provider.root_path_parts
        list_articles_url = project_provider.build_url(False, *root_parts, 'articles')
        file_article_url = project_provider.build_url(False, *root_parts, 'articles',
                                                      file_article_id)
        folder_article_url = project_provider.build_url(False, *root_parts, 'articles',
                                                        folder_article_id)

        aiohttpretty.register_json_uri('GET', list_articles_url,
                                       body=root_provider_fixtures['list_project_articles'],
                                       params={'page': '1', 'page_size': str(MAX_PAGE_SIZE)})
        aiohttpretty.register_json_uri('GET', list_articles_url, body=[],
                                       params={'page': '2', 'page_size': str(MAX_PAGE_SIZE)})
        aiohttpretty.register_json_uri('GET', file_article_url,
                                       body=root_provider_fixtures['file_article_metadata'])
        aiohttpretty.register_json_uri('GET', folder_article_url,
                                       body=root_provider_fixtures['folder_article_metadata'])

        path = FigsharePath('/', _ids=(''), folder=True)

        result = await project_provider.revalidate_path(path, '{}'.format('file'), folder=False)

        assert result.is_dir is False
        assert result.name == 'file'
        assert result.identifier == str(
            root_provider_fixtures['file_article_metadata']['files'][0]['id'])

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_project_revalidate_path_duplicate_folder(self, project_provider,
                                                            root_provider_fixtures):
        file_article_id = str(root_provider_fixtures['list_project_articles'][0]['id'])
        folder_article_id = str(root_provider_fixtures['list_project_articles'][1]['id'])
        folder_article_name = root_provider_fixtures['list_project_articles'][1]['title']

        root_parts = project_provider.root_path_parts
        list_articles_url = project_provider.build_url(False, *root_parts, 'articles')
        file_article_url = project_provider.build_url(False, *root_parts, 'articles',
                                                      file_article_id)
        folder_article_url = project_provider.build_url(False, *root_parts, 'articles',
                                                        folder_article_id)

        aiohttpretty.register_json_uri('GET', list_articles_url,
                                       body=root_provider_fixtures['list_project_articles'],
                                       params={'page': '1', 'page_size': str(MAX_PAGE_SIZE)})
        aiohttpretty.register_json_uri('GET', list_articles_url, body=[],
                                       params={'page': '2', 'page_size': str(MAX_PAGE_SIZE)})
        aiohttpretty.register_json_uri('GET', file_article_url,
                                       body=root_provider_fixtures['file_article_metadata'])
        aiohttpretty.register_json_uri('GET', folder_article_url,
                                       body=root_provider_fixtures['folder_article_metadata'])

        path = FigsharePath('/', _ids=(''), folder=True)

        result = await project_provider.revalidate_path(path, folder_article_name, folder=True)

        assert result.is_dir is True
        assert result.name == 'folder_article'
        assert result.identifier == folder_article_id

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_project_revalidate_path_not_root(self, project_provider, root_provider_fixtures):
        file_article_id = str(root_provider_fixtures['list_project_articles'][0]['id'])
        path = FigsharePath('/folder1/', _ids=('', file_article_id), folder=True)
        root_parts = project_provider.root_path_parts
        file_article_url = project_provider.build_url(False, *root_parts, 'articles',
                                                      file_article_id)

        aiohttpretty.register_json_uri('GET', file_article_url,
                                       body=root_provider_fixtures['file_article_metadata'])

        result = await project_provider.revalidate_path(path, '{}'.format('file'), folder=False)
        assert result.is_dir is False
        assert result.name == 'file'
        assert result.identifier == str(
            root_provider_fixtures['file_article_metadata']['files'][0]['id'])

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_article_revalidate_path_bad_path(self, article_provider, root_provider_fixtures):
        item = root_provider_fixtures['file_metadata']
        file_id = str(item['id'])
        path = FigsharePath('/fodler1/folder2/', _ids=('', '', file_id), folder=True)

        with pytest.raises(exceptions.NotFoundError) as e:
            await article_provider.revalidate_path(path, 'childname', folder=True)

        assert e.value.code == 404

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_article_revalidate_path_file(self, article_provider, crud_fixtures):
        item = crud_fixtures["upload_folder_article_metadata"]
        file_id = str(item['files'][0]['id'])
        file_name = item['files'][0]['name']
        path = FigsharePath('/' + str(file_name), _ids=('', file_id), folder=True)

        urn_parts = (*article_provider.root_path_parts, (path.identifier))

        url = article_provider.build_url(False, *urn_parts)
        aiohttpretty.register_json_uri('GET', url, body=item)

        result = await article_provider.revalidate_path(path, item['files'][0]['name'],
                                                        folder=False)

        expected = path.child(item['files'][0]['name'], _id=file_id, folder=False,
                              parent_is_folder=False)

        assert result == expected


class TestMisc:

    def test_path_from_metadata_file(self, project_provider, root_provider_fixtures):
        file_article_metadata = root_provider_fixtures['file_article_metadata']
        fig_metadata = metadata.FigshareFileMetadata(file_article_metadata)

        path = FigsharePath('/', _ids=(''), folder=True)

        expected = FigsharePath('/file_article/file', _ids=('', '4037952', '6530715'), folder=False)

        result = project_provider.path_from_metadata(path, fig_metadata)
        assert result == expected

    def test_path_from_metadata_folder(self, project_provider, root_provider_fixtures):
        folder_article_metadata = root_provider_fixtures['folder_article_metadata']
        fig_metadata = metadata.FigshareFolderMetadata(folder_article_metadata)

        path = FigsharePath('/', _ids=(''), folder=True)

        expected = FigsharePath('/folder_article/', _ids=('', '4040019'), folder=True)

        result = project_provider.path_from_metadata(path, fig_metadata)
        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test__get_article_metadata_returns_none(self, project_provider,
                                                    root_provider_fixtures):
        file_id = root_provider_fixtures['file_article_metadata']['id']
        item = {'defined_type': 5, 'files': None, 'id': file_id}

        url = project_provider.build_url(False, *project_provider.root_path_parts,
                                         'articles', str(file_id))

        aiohttpretty.register_json_uri('GET', url, body=item)

        result = await project_provider._get_article_metadata(str(file_id), False)
        assert result is None

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test__get_file_upload_url_error(self, project_provider):
        article_id = '12345'
        file_id = '871947'
        url = project_provider.build_url(False, 'articles', article_id, 'files', file_id)
        aiohttpretty.register_json_uri('GET', url, status=404)

        with pytest.raises(exceptions.ProviderError) as e:
            await project_provider._get_file_upload_url(article_id, file_id)

        assert e.value.code == 500

    @pytest.mark.asyncio
    async def test_revisions(self, project_provider):
        result = await project_provider.revisions('/')
        expected = [metadata.FigshareFileRevisionMetadata()]
        assert result == expected

    def test_can_duplicate_names(self, project_provider):
        assert project_provider.can_duplicate_names() is False

    def test_base_figshare_provider_fileset(self, auth, credentials):
        settings = {
            'container_type': 'fileset',
            'container_id': '13423',
        }

        test_provider = provider.FigshareArticleProvider(auth, credentials, settings)

        assert test_provider.container_type == 'article'

    def test_base_figshare_provider_invalid_setting(self, auth, credentials):
        bad_settings = {
            'container_type': 'not_a_project',
            'container_id': '13423',
        }

        with pytest.raises(exceptions.ProviderError) as e:
            provider.FigshareProjectProvider(auth, credentials, bad_settings)

        assert e.value.message == '{} is not a valid container type.'.format(
            bad_settings['container_type'])

    def test_figshare_provider_invalid_setting(self, auth, credentials):
        bad_settings = {
            'container_type': 'not_a_project',
            'container_id': '13423',
        }

        with pytest.raises(exceptions.ProviderError) as e:
            provider.FigshareProvider(auth, credentials, bad_settings)

        assert e.value.message == 'Invalid "container_type" {0}'.format(
            bad_settings['container_type'])
