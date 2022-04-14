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

from waterbutler.providers.rushfiles.provider import (RushFilesPath,
                                                        Attributes,
                                                        ClientJournalEventType)
from waterbutler.providers.rushfiles.metadata import (RushFilesRevision,
                                                        RushFilesFileMetadata,
                                                        RushFilesFolderMetadata)

from tests.providers.rushfiles.fixtures import(root_provider_fixtures,
                                                intra_fixtures,
                                                revision_fixtures)

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
            'domain': 'rushfiles.com',
            'id': 'd0c475011bd24b6dae8a6f890f6b4a93',
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
            "ShareId": "d0c475011bd24b6dae8a6f890f6b4a93",
            "InternalName": "0f04f33f715a4d5890307f114bf24e9c",
            "UploadName": "3897409d06204fefbdd6da1a70581654",
            "Tick": 5,
            "ParrentId": "d0c475011bd24b6dae8a6f890f6b4a93",
            "EndOfFile": 5,
            "CreationTime": "2021-11-18T15:44:36.4329227Z",
            "LastAccessTime": "2021-11-14T02:34:18.575Z",
            "LastWriteTime": "2021-11-18T15:44:36.4329227Z",
            "PublicName": "hoge.txt",
            "IsFile": True,
            "Attributes": Attributes.ARCHIVE
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
            "ShareId": "d0c475011bd24b6dae8a6f890f6b4a93",
            "InternalName": "088e80f914f74290b15ef9cf5d63e06a",
            "Tick": 5,
            "ParrentId": "d0c475011bd24b6dae8a6f890f6b4a93",
            "EndOfFile": 0,
            "CreationTime": "2021-11-18T15:44:36.4329227Z",
            "LastAccessTime": "2021-11-14T02:34:18.575Z",
            "LastWriteTime": "2021-11-18T15:44:36.4329227Z",
            "PublicName": "foo",
            "IsFile": False,
            "Attributes": Attributes.DIRECTORY
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

        children_of_root_url = provider._build_clientgateway_url(
            str(provider.share['id']), 'virtualfiles', str(provider.share['id']), 'children')
        good_url = provider._build_clientgateway_url(
            str(provider.share['id']), 'virtualfiles', file_inter_id)
        bad_url = provider._build_clientgateway_url(
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

        children_of_root_url = provider._build_clientgateway_url(
            str(provider.share['id']), 'virtualfiles', str(provider.share['id']), 'children')
        good_url = provider._build_clientgateway_url(
            str(provider.share['id']), 'virtualfiles', folder_inter_id)
        bad_url = provider._build_clientgateway_url(
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
        
    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_revalidate_path_file(self, provider, root_provider_fixtures):
        file_name = 'Tasks.xlsx'
        revalidate_path_metadata = root_provider_fixtures['file_metadata']
        file_id = revalidate_path_metadata['InternalName']
        root_id = str(provider.share['id'])

        parent_path = RushFilesPath('/', _ids=[root_id])
        expected_path = RushFilesPath('/{}'.format(file_name), _ids=[root_id, file_id])

        parent_url = provider._build_clientgateway_url(root_id, 'virtualfiles', root_id, 'children')
        aiohttpretty.register_json_uri('GET', parent_url,
                                       body=root_provider_fixtures['children_metadata'], status=200)

        actual_path = await provider.revalidate_path(parent_path, file_name, False)
        assert actual_path == expected_path

        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider.revalidate_path(parent_path, file_name, True)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_revalidate_path_folder(self, provider, root_provider_fixtures):
        folder_name = 'Test'
        revalidate_path_metadata = root_provider_fixtures['folder_metadata']
        folder_id = revalidate_path_metadata['InternalName']
        root_id = str(provider.share['id'])

        parent_path = RushFilesPath('/', _ids=[root_id])
        expected_path = RushFilesPath('/{}/'.format(folder_name), _ids=[root_id, folder_id])

        parent_url = provider._build_clientgateway_url(root_id, 'virtualfiles', root_id, 'children')
        aiohttpretty.register_json_uri('GET', parent_url,
                                       body=root_provider_fixtures['children_metadata'], status=200)

        actual_path = await provider.revalidate_path(parent_path, folder_name, True)
        assert actual_path == expected_path

        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider.revalidate_path(parent_path, folder_name, False)
    
    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_revalidate_path_subfile(self, provider, root_provider_fixtures):
        root_id = str(provider.share['id'])
        parent_id = root_provider_fixtures['folder_metadata']['InternalName']
        subfile_id = root_provider_fixtures['file_metadata']['InternalName']

        parent_name = 'Test'
        subfile_name = 'Tasks.xlsx'

        parent_path = RushFilesPath('/{}/'.format(parent_name), _ids=[root_id, parent_id])
        expected_path = RushFilesPath('/{}/{}'.format(parent_name, subfile_name),
                                     _ids=[root_id, parent_id, subfile_id])

        parent_url = provider._build_clientgateway_url(root_id, 'virtualfiles', parent_id, 'children')
        aiohttpretty.register_json_uri('GET', parent_url,
                                       body=root_provider_fixtures['children_metadata'], status=200)

        actual_path = await provider.revalidate_path(parent_path, subfile_name, False)
        assert actual_path == expected_path

        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider.revalidate_path(parent_path, subfile_name, True)
    
class TestDelete:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_file(self, provider, root_provider_fixtures):
        item = root_provider_fixtures['file_metadata_delete_ok']
        path = RushFilesPath('/Tasks.xlsx', _ids=(provider.share['id'], item['InternalName']))
        url = provider._build_filecache_url(str(provider.share['id']), 'files', item['InternalName'])
        url_body = json.dumps({
                        "Data":{
                            "ClientJournalEvent": {
                                "RfVirtualFile": {
                                    "Deleted": True
                                }
                            }
                        }   
                    })

        aiohttpretty.register_uri('DELETE', url, body=url_body, status=200)

        await provider.delete(path)

        assert aiohttpretty.has_call(method='DELETE', uri=url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_folder_ok(self, provider, root_provider_fixtures):
        item = root_provider_fixtures['folder_metadata_delete_ok']
        path = RushFilesPath('/GakuNin RDM/', _ids=(provider.share['id'], item['InternalName']))
        url = provider._build_filecache_url(str(provider.share['id']), 'files', item['InternalName'])
        url_body = json.dumps({
                        "Data":{
                            "ClientJournalEvent": {
                                'TransmitId': provider._generate_uuid(),
                                'ClientJournalEventType': ClientJournalEventType.DELETE,
                                "RfVirtualFile": {
                                    "FileLock": {
                                        'DeviceId': 'waterbutler'
                                    }
                                }
                            }
                        }   
                    })

        aiohttpretty.register_uri('DELETE', url, body=url_body, status=200)

        await provider.delete(path)

        assert aiohttpretty.has_call(method='DELETE', uri=url)

    @pytest.mark.asyncio
    async def test_delete_path_does_not_exist(self, provider):
        path = RushFilesPath('/Gone')

        with pytest.raises(exceptions.NotFoundError) as e:
            await provider.delete(path)

        assert e.value.code == 404
        assert str(path) in e.value.message

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_root(self, provider):
        path = RushFilesPath('/', _ids=[provider.share['id']], folder=True)

        with pytest.raises(exceptions.DeleteError) as e:
            await provider.delete(path)

        assert e.value.message == 'root cannot be deleted'
        assert e.value.code == 400

class TestDownload:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download(self, provider, root_provider_fixtures):
        body = b'dearly-beloved'
        path = WaterButlerPath('/Tasks.xlsx',_ids=(provider.share['id'],'1a8fe2de6a4144a6b1088ffc03fef4c1'))
        metadata = root_provider_fixtures['file_metadata_resp']
        
        metadata_url = provider._build_clientgateway_url(str(provider.share['id']), 'virtualfiles', path.identifier)
        aiohttpretty.register_uri('GET', metadata_url, body=json.dumps(metadata))

        url = provider._build_filecache_url(str(provider.share['id']), 'files', metadata['Data']['UploadName'])
        aiohttpretty.register_uri('GET', url, body=body)

        result = await provider.download(path)
        content = await result.read()

        assert content == body
    
    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_revision(self, provider, revision_fixtures):
        revision_metadata = revision_fixtures['revisions_list_metadata']
        revision = str(revision_metadata['Data'][1]['File']['Tick'])
        body = b'dearly-beloved'
        path = WaterButlerPath('/Tasks.xlsx',_ids=(provider.share['id'],'1a8fe2de6a4144a6b1088ffc03fef4c1'))
        
        metadata_url = provider._build_clientgateway_url(str(provider.share['id']), 'virtualfiles', path.identifier, 'history')
        aiohttpretty.register_uri('GET', metadata_url, body=json.dumps(revision_metadata))

        url = provider._build_filecache_url(str(provider.share['id']), 'files', revision_metadata['Data'][1]['File']['UploadName'])
        aiohttpretty.register_uri('GET', url, body=body)

        result = await provider.download(path, revision)
        content = await result.read()

        assert content == body

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_path_is_dir(self, provider):
        path = WaterButlerPath('/lets-go-dir/')
        with pytest.raises(exceptions.DownloadError):
            await provider.download(path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_id_not_found(self, provider):
        path = WaterButlerPath('/lets-go-crazy')
        with pytest.raises(exceptions.DownloadError):
            await provider.download(path)

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

class TestIntraMove:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_move_file(self, provider, root_provider_fixtures, intra_fixtures, file_metadata_response):
        item = file_metadata_response['Data']
        src_path = WaterButlerPath('/hoge.txt', _ids=(provider, item['InternalName']))
        dest_path = WaterButlerPath('/super/hoge.txt', _ids=(provider, item['InternalName'], item['InternalName']))

        metadata_url = provider._build_clientgateway_url(str(provider.share['id']), 'virtualfiles', item['InternalName'])
        intra_move_url = provider._build_filecache_url(str(provider.share['id']), 'files', src_path.identifier)
        del_url = provider._build_filecache_url(str(provider.share['id']), 'files', dest_path.identifier)
        aiohttpretty.register_json_uri('GET', metadata_url, body=file_metadata_response)
        aiohttpretty.register_json_uri('PUT', intra_move_url, body=intra_fixtures['intra_move_file_resp_metadata'])
        aiohttpretty.register_json_uri('DELETE', del_url)

        result, created = await provider.intra_move(provider, src_path, dest_path)
        expected= RushFilesFileMetadata(item, dest_path)

        assert result == expected
    
    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_move_folder(self, provider, root_provider_fixtures, intra_fixtures, folder_metadata_response):
        item = folder_metadata_response['Data']
        src_path = WaterButlerPath('/foo/', _ids=(provider, item['InternalName']))
        dest_path = WaterButlerPath('/super/foo/', _ids=(provider, item['InternalName'], item['InternalName']))

        metadata_url = provider._build_clientgateway_url(str(provider.share['id']), 'virtualfiles', item['InternalName'])
        children_url = provider._build_clientgateway_url(str(provider.share['id']), 'virtualfiles', item['InternalName'], 'children')
        intra_move_url = provider._build_filecache_url(str(provider.share['id']), 'files', src_path.identifier)
        del_url = provider._build_filecache_url(str(provider.share['id']), 'files', dest_path.identifier)
        aiohttpretty.register_json_uri('GET', metadata_url, body=folder_metadata_response)
        aiohttpretty.register_json_uri('GET', children_url, body=intra_fixtures['intra_folder_children_metadata'])
        aiohttpretty.register_json_uri('PUT', intra_move_url, body=intra_fixtures['intra_move_folder_resp_metadata'])
        aiohttpretty.register_json_uri('DELETE', del_url)

        expected = RushFilesFolderMetadata(item, dest_path)
        expected.children = await provider._folder_metadata(dest_path)

        result, created = await provider.intra_move(provider, src_path, dest_path)

        assert result == expected
    
    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_move_duplicated_file(self, provider, root_provider_fixtures, intra_fixtures, file_metadata_response):
        item = file_metadata_response['Data']
        src_path = WaterButlerPath('/hoge.txt', _ids=(provider, item['InternalName']))
        dest_path = WaterButlerPath('/super/hoge.txt', _ids=(provider, item['InternalName'], item['InternalName']))

        metadata_url = provider._build_clientgateway_url(str(provider.share['id']), 'virtualfiles', item['InternalName'])
        intra_move_url = provider._build_filecache_url(str(provider.share['id']), 'files', src_path.identifier)
        del_url = provider._build_filecache_url(str(provider.share['id']), 'files', dest_path.identifier)
        aiohttpretty.register_json_uri('GET', metadata_url, body=file_metadata_response)
        aiohttpretty.register_json_uri('PUT', intra_move_url, body=intra_fixtures['intra_duplicated_file_resp_metadata'])
        aiohttpretty.register_json_uri('DELETE', del_url)

        result, created = await provider.intra_move(provider, src_path, dest_path)

        duplicated_path =  WaterButlerPath('/super/hoge(duplicated 2021-11-18T15:44:36.4329227Z).txt', 
                                            _ids=(provider, item['InternalName']))
        duplicated_item = intra_fixtures['intra_duplicated_file_resp_metadata']['Data']['ClientJournalEvent']['RfVirtualFile']
        expected= RushFilesFileMetadata(duplicated_item, duplicated_path)

        assert result == expected
    
    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_move_duplicated_folder(self, provider, root_provider_fixtures, intra_fixtures, folder_metadata_response):
        item = folder_metadata_response['Data']
        src_path = WaterButlerPath('/foo/', _ids=(provider, item['InternalName']))
        dest_path = WaterButlerPath('/super/foo/', _ids=(provider, item['InternalName'], item['InternalName']))

        metadata_url = provider._build_clientgateway_url(str(provider.share['id']), 'virtualfiles', item['InternalName'])
        children_url = provider._build_clientgateway_url(str(provider.share['id']), 'virtualfiles', item['InternalName'], 'children')
        intra_move_url = provider._build_filecache_url(str(provider.share['id']), 'files', src_path.identifier)
        del_url = provider._build_filecache_url(str(provider.share['id']), 'files', dest_path.identifier)
        aiohttpretty.register_json_uri('GET', metadata_url, body=folder_metadata_response)
        aiohttpretty.register_json_uri('GET', children_url, body=intra_fixtures['intra_folder_children_metadata'])
        aiohttpretty.register_json_uri('PUT', intra_move_url, body=intra_fixtures['intra_duplicated_folder_resp_metadata'])
        aiohttpretty.register_json_uri('DELETE', del_url)

        duplicated_path =  WaterButlerPath('/super/foo(duplicated 2021-11-18T15:44:36.4329227Z)/', 
                                            _ids=(provider, item['InternalName'], item['InternalName']))
        duplicated_item = intra_fixtures['intra_duplicated_folder_resp_metadata']['Data']['ClientJournalEvent']['RfVirtualFile']

        expected = RushFilesFolderMetadata(duplicated_item, duplicated_path)
        expected.children = await provider._folder_metadata(duplicated_path)

        result, created = await provider.intra_move(provider, src_path, dest_path)

        assert result == expected

class TestIntraCopy:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_copy_file(self, provider, root_provider_fixtures, intra_fixtures, file_metadata_response):
        item = file_metadata_response['Data']
        src_path = WaterButlerPath('/hoge.txt', _ids=(provider, item['InternalName']))
        dest_path = WaterButlerPath('/super/hoge.txt', _ids=(provider, item['InternalName']))

        metadata_url = provider._build_clientgateway_url(str(provider.share['id']), 'virtualfiles', item['InternalName'])
        intra_copy_url = provider._build_filecache_url(str(provider.share['id']), 'files', src_path.identifier, 'clone')
        intra_move_url = provider._build_filecache_url(str(provider.share['id']), 'files', src_path.identifier)
        aiohttpretty.register_json_uri('GET', metadata_url, body=file_metadata_response)
        aiohttpretty.register_json_uri('POST', intra_copy_url, body=intra_fixtures['intra_copy_file_resp_metadata'], status=201)
        aiohttpretty.register_json_uri('PUT', intra_move_url, body=intra_fixtures['intra_move_file_resp_metadata'])

        result, created = await provider.intra_copy(provider, src_path, dest_path)
        expected = RushFilesFileMetadata(item, dest_path)

        assert result == expected
    
    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_copy_duplicated_file(self, provider, root_provider_fixtures, intra_fixtures, file_metadata_response):
        item = file_metadata_response['Data']
        src_path = WaterButlerPath('/hoge.txt', _ids=(provider, item['InternalName']))
        dest_path = WaterButlerPath('/super/hoge.txt', _ids=(provider, item['InternalName']))

        metadata_url = provider._build_clientgateway_url(str(provider.share['id']), 'virtualfiles', item['InternalName'])
        intra_copy_url = provider._build_filecache_url(str(provider.share['id']), 'files', src_path.identifier, 'clone')
        intra_move_url = provider._build_filecache_url(str(provider.share['id']), 'files', src_path.identifier)
        aiohttpretty.register_json_uri('GET', metadata_url, body=file_metadata_response)
        aiohttpretty.register_json_uri('POST', intra_copy_url, body=intra_fixtures['intra_duplicated_file_resp_metadata'], status=201)
        aiohttpretty.register_json_uri('PUT', intra_move_url, body=intra_fixtures['intra_duplicated_file_resp_metadata'])

        result, created = await provider.intra_copy(provider, src_path, dest_path)

        duplicated_path =  WaterButlerPath('/super/hoge(duplicated 2021-11-18T15:44:36.4329227Z).txt', 
                                            _ids=(provider, intra_fixtures['intra_duplicated_file_resp_metadata']['Data']['ClientJournalEvent']['RfVirtualFile']['InternalName']))
        duplicated_item = intra_fixtures['intra_duplicated_file_resp_metadata']['Data']['ClientJournalEvent']['RfVirtualFile']
        expected = RushFilesFileMetadata(duplicated_item, duplicated_path)

        assert result == expected

class TestRevision:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_get_revisions(self, provider, root_provider_fixtures, revision_fixtures):
        item = root_provider_fixtures['file_metadata']
        revisions_list = revision_fixtures['revisions_list_metadata']
        path = WaterButlerPath('/Tasks.xlsx', _ids=(provider, item['InternalName']))

        revisions_url = provider._build_clientgateway_url(str(provider.share['id']), 'virtualfiles', path.identifier, 'history')
        aiohttpretty.register_json_uri('GET', revisions_url, body=revisions_list)

        result = await provider.revisions(path)

        expected = [
            RushFilesRevision(each['File'])
            for each in revisions_list['Data']
        ]

        assert result == expected
        assert aiohttpretty.has_call(method='GET', uri=revisions_url)
    
    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_revision_metadata(self, provider, root_provider_fixtures, revision_fixtures):
        list_metadata = revision_fixtures['revisions_list_metadata']
        item = list_metadata['Data'][0]['File']

        path = WaterButlerPath('/Tasks.xlsx', _ids=(provider.share['id'], item['InternalName']))
        url = provider._build_clientgateway_url(str(provider.share['id']), 'virtualfiles', path.identifier, 'history')

        aiohttpretty.register_json_uri('GET', url, body=list_metadata)

        result = await provider.metadata(path, revision=str(item['Tick']))
        expected = RushFilesFileMetadata(item, path)

        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_revision_metadata_error(self, provider, root_provider_fixtures,
                                           revision_fixtures):
        list_metadata = revision_fixtures['revisions_list_metadata']
        item = list_metadata['Data'][0]['File']

        path = WaterButlerPath('/Tasks.xlsx', _ids=(provider.share['id'], item['InternalName']))
        url = provider._build_clientgateway_url(str(provider.share['id']), 'virtualfiles', path.identifier, 'history')

        aiohttpretty.register_json_uri('GET', url, body=list_metadata)

        with pytest.raises(exceptions.NotFoundError) as e:
            await provider.metadata(path, revision='this is a bad revision id')

        assert e.value.code == 404
        assert str(path) in e.value.message