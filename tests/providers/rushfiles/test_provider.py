import io
import os
import copy
import json
from http import client
from urllib import parse

import pytest
import aiohttpretty

from waterbutler.core import streams
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.rushfiles import settings as ds
from waterbutler.providers.rushfiles import RushFilesProvider
# from waterbutler.providers.rushfiles import utils as drive_utils
from waterbutler.providers.rushfiles.provider import RushFilesPath
from waterbutler.providers.rushfiles.metadata import (RushFilesRevision,
                                                        RushFilesFileMetadata,
                                                        RushFilesFolderMetadata,
                                                        RushFilesFileRevisionMetadata)

from tests.providers.rushfiles.fixtures import(root_provider_fixtures)

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
        'share':{
            'name': 'rush',
            'id': '123',
        },
        'folder': '1234567890'
}

@pytest.fixture
def search_for_file_response():
    return {
        'Data':[{
            'InternalName': '0f04f33f715a4d5890307f114bf24e9c',
            'IsFile': True,
            'PublicName': 'files.txt'
        }]
    }

@pytest.fixture
def file_metadata_response():
    return {
        'Data': {
            'ShareId': '0f04f33f715a4d5890307f114bf24e9c',
            'IsFile': True
        }
    }

@pytest.fixture
def search_for_folder_response():
    return {
        'Data':[{
            'InternalName': '088e80f914f74290b15ef9cf5d63e06a',
            'IsFile': False,
            'PublicName': 'fooFolder'
        }]
    }

@pytest.fixture
def folder_metadata_response():
    return {
        'Data': {
            'ShareId': '088e80f914f74290b15ef9cf5d63e06a',
            'IsFile': False
        }
    }



@pytest.fixture
def provider(auth, credentials, settings):
    return RushFilesProvider(auth, credentials, settings)

class TestValidatePath:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_file(self, provider, search_for_file_response, file_metadata_response):
        file_name = 'files.txt'
        file_inter_id = '0f04f33f715a4d5890307f114bf24e9c' # Tasks.xlsx

        children_of_root_url = provider.build_url(
            str(provider.share['id']), 'virtualfiles', str(provider.share['id']), 'children')
        good_url = provider.build_url(
            str(provider.share['id']), 'virtualfiles', file_inter_id)
        bad_url = provider.build_url(
            str(provider.share['id']), 'virtualfiles', file_inter_id, 'children')

        aiohttpretty.register_json_uri('GET', children_of_root_url, body=search_for_file_response, status=200)
        aiohttpretty.register_json_uri('GET', good_url, body=file_metadata_response, status=200)
        aiohttpretty.register_json_uri('GET', bad_url, status=404)

        try:
            wb_path_v1 = await provider.validate_v1_path('/' + file_name)
        except Exception as exc:
            pytest.fail(str(exc))

        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider.validate_v1_path('/' + file_name + '/')

        assert exc.value.code == client.NOT_FOUND

        wb_path_v0 = await provider.validate_path('/' + file_name)

        assert wb_path_v1 == wb_path_v0

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_folder(self, provider, search_for_folder_response, folder_metadata_response):
        folder_name = 'fooFolder'
        folder_inter_id = '088e80f914f74290b15ef9cf5d63e06a'

        children_of_root_url = provider.build_url(
            str(provider.share['id']), 'virtualfiles', str(provider.share['id']), 'children')
        good_url = provider.build_url(
            str(provider.share['id']), 'virtualfiles', folder_inter_id)
        bad_url = provider.build_url(
            str(provider.share['id']), 'virtualfiles', folder_inter_id, 'children')

        aiohttpretty.register_json_uri('GET', children_of_root_url, body=search_for_folder_response, status=200)
        aiohttpretty.register_json_uri('GET', good_url, body=folder_metadata_response, status=200)
        aiohttpretty.register_json_uri('GET', bad_url, status=404)

        try:
            wb_path_v1 = await provider.validate_v1_path('/' + folder_name + '/')
        except Exception as exc:
            pytest.fail(str(exc))

        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider.validate_v1_path('/' + folder_name)

        assert exc.value.code == client.NOT_FOUND

        wb_path_v0 = await provider.validate_path('/' + folder_name + '/')

        assert wb_path_v1 == wb_path_v0

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_root(self, provider):
        path = '/'

        result = await provider.validate_v1_path(path)
        expected = RushFilesPath(path, folder=True)

        assert result == expected

    # @pytest.mark.asyncio
    # @pytest.mark.aiohttpretty
    # async def test_revalidate_path_file(self, provider, root_provider_fixtures):
    #     file_name = '/Gear1.stl'
    #     revalidate_path_metadata = root_provider_fixtures['revalidate_path_file_metadata_1']
    #     file_id = revalidate_path_metadata['items'][0]['id']
    #     path = RushFilesPath(file_name, _ids=['0', file_id])

    #     parts = [[parse.unquote(x), True] for x in file_name.strip('/').split('/')]
    #     parts[-1][1] = False

    #     current_part = parts.pop(0)
    #     part_name, part_is_folder = current_part[0], current_part[1]
    #     name, ext = os.path.splitext(part_name)
    #     query = _build_title_search_query(provider, file_name.strip('/'), False)

    #     url = provider.build_url('files', file_id, 'children', q=query, fields='items(id)')
    #     aiohttpretty.register_json_uri('GET', url, body=revalidate_path_metadata)

    #     url = provider.build_url('files', file_id, fields='id,title,mimeType')
    #     aiohttpretty.register_json_uri('GET', url,
    #                                    body=root_provider_fixtures['revalidate_path_file_metadata_2'])

    #     result = await provider.revalidate_path(path, file_name)

    #     assert result.name in path.name

    # @pytest.mark.asyncio
    # @pytest.mark.aiohttpretty
    # async def test_revalidate_path_file_gdoc(self, provider, root_provider_fixtures):
    #     file_name = '/Gear1.gdoc'
    #     file_id = root_provider_fixtures['revalidate_path_file_metadata_1']['items'][0]['id']
    #     path = RushFilesPath(file_name, _ids=['0', file_id])

    #     parts = [[parse.unquote(x), True] for x in file_name.strip('/').split('/')]
    #     parts[-1][1] = False

    #     current_part = parts.pop(0)
    #     part_name, part_is_folder = current_part[0], current_part[1]
    #     name, ext = os.path.splitext(part_name)
    #     gd_ext = drive_utils.get_mimetype_from_ext(ext)
    #     query = "title = '{}' " \
    #             "and trashed = false " \
    #             "and mimeType = '{}'".format(clean_query(name), gd_ext)

    #     url = provider.build_url('files', file_id, 'children', q=query, fields='items(id)')
    #     aiohttpretty.register_json_uri('GET', url,
    #                                    body=root_provider_fixtures['revalidate_path_file_metadata_1'])

    #     url = provider.build_url('files', file_id, fields='id,title,mimeType')
    #     aiohttpretty.register_json_uri('GET', url,
    #                                    body=root_provider_fixtures['revalidate_path_gdoc_file_metadata'])

    #     result = await provider.revalidate_path(path, file_name)

    #     assert result.name in path.name

    # @pytest.mark.asyncio
    # @pytest.mark.aiohttpretty
    # async def test_revalidate_path_folder(self, provider, root_provider_fixtures):
    #     file_name = "/inception folder yo/"
    #     file_id = root_provider_fixtures['revalidate_path_folder_metadata_1']['items'][0]['id']
    #     path = RushFilesPath(file_name, _ids=['0', file_id])

    #     parts = [[parse.unquote(x), True] for x in file_name.strip('/').split('/')]
    #     parts[-1][1] = False

    #     current_part = parts.pop(0)
    #     part_name, part_is_folder = current_part[0], current_part[1]
    #     name, ext = os.path.splitext(part_name)
    #     query = _build_title_search_query(provider, file_name.strip('/') + '/', True)

    #     folder_one_url = provider.build_url('files', file_id, 'children', q=query, fields='items(id)')
    #     aiohttpretty.register_json_uri('GET', folder_one_url,
    #                                    body=root_provider_fixtures['revalidate_path_folder_metadata_1'])

    #     folder_two_url = provider.build_url('files', file_id, fields='id,title,mimeType')
    #     aiohttpretty.register_json_uri('GET', folder_two_url,
    #                                    body=root_provider_fixtures['revalidate_path_folder_metadata_2'])

    #     result = await provider.revalidate_path(path, file_name, True)
    #     assert result.name in path.name

class TestCreateFolder:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_already_exists(self, provider):
        path = WaterButlerPath('/hoge/', _ids=('doesnt', 'exist'))

        with pytest.raises(exceptions.FolderNamingConflict) as e:
            await provider.create_folder(path)

        assert e.value.code == 409
        assert e.value.message == ('Cannot create folder "hoge", because a file or folder '
                                   'already exists with that name')

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_returns_metadata(self, provider, root_provider_fixtures):
        path = WaterButlerPath('/hogeTest/', _ids=('38960c447d9643e395334f46aeeb4188', None))

        aiohttpretty.register_json_uri('POST', provider._build_filecache_url(str(provider.share['id']), 'files'),
                                       body=root_provider_fixtures['create_folder_metadata'])

        resp = await provider.create_folder(path)

        assert resp.kind == 'folder'
        assert resp.name == 'hogeTest'
        assert resp.path == '/hogeTest/'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_raises_non_404(self, provider):
        path = WaterButlerPath('/hoge/huga/bar/', _ids=('38960c447d9643e395334f46aeeb4188',
                                                        'something', 'something', None))

        url = provider._build_filecache_url(str(provider.share['id']), 'files')
        aiohttpretty.register_json_uri('POST', url, status=418)

        with pytest.raises(exceptions.CreateFolderError) as e:
            await provider.create_folder(path)

        assert e.value.code == 418

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_must_be_folder(self, provider, monkeypatch):
        with pytest.raises(exceptions.CreateFolderError) as e:
            await provider.create_folder(WaterButlerPath('/hoge.foo', _ids=('this', 'file')))
    
class TestOperationsOrMisc:

    def test_path_from_metadata(self, provider, root_provider_fixtures):
        item = root_provider_fixtures['file_metadata']
        src_path = RushFilesPath('/Tasks.xlsx', _ids=(provider.share['id'], item['InternalName']))

        metadata = RushFilesFileMetadata(item, src_path)
        child_path = provider.path_from_metadata(src_path.parent, metadata)

        assert child_path.full_path == src_path.full_path
        assert child_path == src_path
