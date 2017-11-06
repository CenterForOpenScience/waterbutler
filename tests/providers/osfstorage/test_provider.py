import os
import json
from http import client
from unittest import mock

import pytest
import aiohttpretty

from waterbutler.core import metadata
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath
from waterbutler.providers.osfstorage.settings import FILE_PATH_COMPLETE
from waterbutler.providers.osfstorage.metadata import (OsfStorageFileMetadata,
                                                       OsfStorageFolderMetadata,
                                                       OsfStorageRevisionMetadata)

from tests import utils
from tests.providers.osfstorage.fixtures import (auth,
                                                 credentials,
                                                 settings,
                                                 provider,
                                                 provider_and_mock,
                                                 provider_and_mock2,
                                                 file_stream,
                                                 file_like,
                                                 file_content,
                                                 file_lineage,
                                                 file_metadata,
                                                 file_metadata_object,
                                                 file_path,
                                                 folder_lineage,
                                                 folder_metadata,
                                                 folder_children_metadata,
                                                 folder_path,
                                                 revisions_metadata,
                                                 revision_metadata_object,
                                                 download_response,
                                                 download_path,
                                                 upload_response,
                                                 upload_path,
                                                 root_path,
                                                 mock_time)


def build_signed_url_without_auth(provider, method, *segments, **params):
    data = params.pop('data', None)
    base_url = provider.build_url(*segments, **params)
    url, _, params = provider.build_signed_url(method, base_url, data=data)
    return url, params


def build_signed_url_with_auth(provider, method, *segments, **params):
    data = params.pop('data', None)
    base_url = provider.build_url(*segments, **params)
    url, _, params = provider.build_signed_url(method,
                                               base_url,
                                               data=data,
                                               params={'user': provider.auth['id']})
    return url, params


class TestCreateFolder:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_create_folder(self, folder_path, provider, folder_metadata, mock_time):

        data = json.dumps(folder_metadata)
        url, params = build_signed_url_without_auth(provider, 'POST',
                                                    folder_path.parent.identifier,
                                                    'children', data=data)

        aiohttpretty.register_json_uri('POST', url, body=folder_metadata, status=201, params=params)

        resp = await provider.create_folder(folder_path)

        assert isinstance(resp, OsfStorageFolderMetadata)


class TestDownload:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_with_auth(self, provider_and_mock, download_response, download_path,
                                      mock_time):

        provider, inner_provider = provider_and_mock

        uri, params = build_signed_url_with_auth(provider, 'GET', download_path.identifier,
                                                 'download', version=None, mode=None)

        aiohttpretty.register_json_uri('GET', uri,  body=download_response, params=params)

        await provider.download(download_path)

        assert provider.make_provider.called
        assert inner_provider.download.called

        assert aiohttpretty.has_call(method='GET', uri=uri, params=params)
        provider.make_provider.assert_called_once_with(download_response['settings'])
        expected_path = WaterButlerPath('/' + download_response['data']['path'])
        expected_display_name = download_response['data']['name']
        inner_provider.download.assert_called_once_with(path=expected_path,
                                                        displayName=expected_display_name)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_without_auth(self, provider_and_mock, download_response, download_path,
                                         mock_time):
        provider, inner_provider = provider_and_mock

        provider.auth = {}
        url, params = build_signed_url_without_auth(provider, 'GET', download_path.identifier,
                                                    'download', version=None, mode=None)
        aiohttpretty.register_json_uri('GET', url, params=params, body=download_response)

        await provider.download(download_path)

        assert provider.make_provider.called
        assert inner_provider.download.called
        assert aiohttpretty.has_call(method='GET', uri=url, params=params)
        provider.make_provider.assert_called_once_with(download_response['settings'])

        expected_path = WaterButlerPath('/' + download_response['data']['path'])
        expected_display_name = download_response['data']['name']
        inner_provider.download.assert_called_once_with(path=expected_path,
                                                        displayName=expected_display_name)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_without_id(self, provider, download_response, file_path, mock_time):

        url, params = build_signed_url_without_auth(provider, 'GET', file_path.identifier,
                                                    'download', version=None, mode=None)
        aiohttpretty.register_json_uri('GET', url, params=params, body=download_response)
        file_path._parts[-1]._id = None

        with pytest.raises(exceptions.NotFoundError):
            await provider.download(file_path)


class TestDelete:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete(self, provider, file_path, mock_time):

        url,  params = build_signed_url_with_auth(provider, 'DELETE', file_path.identifier)
        aiohttpretty.register_uri('DELETE', url, status_code=200, params=params)

        await provider.delete(file_path)

        assert aiohttpretty.has_call(method='DELETE', uri=url, check_params=False)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_without_id(self, provider, file_path, mock_time):

        url, params = build_signed_url_without_auth(provider, 'DELETE', file_path.identifier)
        aiohttpretty.register_uri('DELETE', url, status_code=200)

        file_path._parts[-1]._id = None

        with pytest.raises(exceptions.NotFoundError):
            await provider.delete(file_path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_root(self, provider, root_path, mock_time):

        provider._delete_folder_contents = utils.MockCoroutine()

        url, params = build_signed_url_without_auth(provider, 'DELETE', root_path.identifier)
        aiohttpretty.register_uri('DELETE', url,  status_code=200)

        with pytest.raises(exceptions.DeleteError):
            await provider.delete(root_path)

        provider._delete_folder_contents.assert_not_called()

        await provider.delete(root_path, confirm_delete=1)
        provider._delete_folder_contents.assert_called_once_with(root_path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_folder_contents(self, provider, file_path, folder_path,
                                          folder_children_metadata, mock_time):

        provider.validate_path = utils.MockCoroutine(return_value=file_path)
        provider.delete = utils.MockCoroutine()

        children_url, params = build_signed_url_without_auth(provider, 'GET',
                                                             folder_path.identifier, 'children')
        aiohttpretty.register_json_uri('GET', children_url, params=params, status=200,
                                       body=folder_children_metadata)

        await provider._delete_folder_contents(folder_path)

        provider.delete.assert_called_with(file_path)
        assert provider.delete.call_count == 4


class TestMetadata:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_provider_metadata_empty(self, provider, folder_path, mock_time):

        url, params = build_signed_url_without_auth(provider, 'GET', folder_path.identifier,
                                                    'children')
        aiohttpretty.register_json_uri('GET', url, params=params, status_code=200, body=[])

        res = await provider.metadata(folder_path)

        assert res == []
        assert aiohttpretty.has_call(method='GET', uri=url, params=params)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_provider_metadata_folder(self, provider, folder_path, folder_children_metadata,
                                            mock_time):

        url, params = build_signed_url_without_auth(provider, 'GET', folder_path.identifier,
                                                    'children')
        aiohttpretty.register_json_uri('GET', url, params=params, status=200,
                                       body=folder_children_metadata)

        res = await provider.metadata(folder_path)

        assert isinstance(res, list)
        for item in res:
            assert isinstance(item, metadata.BaseMetadata)
            assert item.name is not None
            assert item.path is not None
            assert item.provider == 'osfstorage'

        assert aiohttpretty.has_call(method='GET', uri=url, params=params)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_provider_metadata_file(self, provider, file_path, file_metadata, mock_time):

        url, params = build_signed_url_without_auth(provider, 'GET', file_path.identifier)
        aiohttpretty.register_json_uri('GET', url, params=params, status=200, body=file_metadata)

        res = await provider.metadata(file_path)

        assert isinstance(res, OsfStorageFileMetadata)
        assert res.name is not None
        assert res.path is not None
        assert res.provider == 'osfstorage'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_provider_metadata_without_id(self, provider, folder_path,
                                                folder_children_metadata, mock_time):

        url, params = build_signed_url_without_auth(provider, 'GET', folder_path.identifier,
                                                    'children')
        aiohttpretty.register_json_uri('GET', url, params=params, status=200,
                                       body=folder_children_metadata)
        folder_path._parts[-1]._id = None

        with pytest.raises(exceptions.MetadataError):
            await provider.metadata(folder_path)


class TestRevisions:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_revisions(self, provider, file_path, revisions_metadata, mock_time):
        url, params = build_signed_url_without_auth(provider, 'GET', file_path.identifier,
                                                    'revisions')
        aiohttpretty.register_json_uri('GET', url, params=params, status=200,
                                       body=revisions_metadata)

        response = await provider.revisions(file_path)

        assert isinstance(response, list)
        for index, revision in enumerate(response):
            assert isinstance(revision, OsfStorageRevisionMetadata)
            assert revision.raw == revisions_metadata['revisions'][index]

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_revisions_without_id(self, provider, file_path, revisions_metadata, mock_time):

        url, params = build_signed_url_without_auth(provider, 'GET', file_path.identifier,
                                                    'revisions')
        aiohttpretty.register_json_uri('GET', url, params=params, status=200,
                                       body=revisions_metadata)
        file_path._parts[-1]._id = None

        with pytest.raises(exceptions.MetadataError):
            await provider.revisions(file_path)


class TestIntraCopy:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_copy_folder(self, provider_and_mock, provider_and_mock2,
                                     folder_children_metadata, mock_time):
        src_provider, src_mock = provider_and_mock
        src_mock.intra_copy = src_provider.intra_copy

        dest_provider, dest_mock = provider_and_mock2
        dest_mock.nid = 'abcde'
        dest_mock._children_metadata = utils.MockCoroutine(return_value=folder_children_metadata)
        dest_mock.validate_v1_path = utils.MockCoroutine(
            return_value=WaterButlerPath('/folder1/', _ids=('rootId', 'folder1'))
        )

        src_path = WaterButlerPath('/folder1/', _ids=['RootId', 'folder1'], folder=True)
        dest_path = WaterButlerPath('/folder1/', folder=True)

        data=json.dumps({
            'user': src_provider.auth['id'],
            'source': src_path.identifier,
            'destination': {
                'name': dest_path.name,
                'node': dest_provider.nid,
                'parent': dest_path.parent.identifier
            }
        })
        url, _, params = src_provider.build_signed_url('POST',
                                                       'https://waterbutler.io/hooks/copy/',
                                                       data=data)

        body = {'path': '/folder1/', 'id': 'folder1', 'kind': 'folder', 'name': 'folder1'}
        aiohttpretty.register_json_uri('POST', url, params=params, status=201, body=body)

        folder_meta, created = await src_mock.intra_copy(dest_mock, src_path, dest_path)
        assert created
        assert isinstance(folder_meta, OsfStorageFolderMetadata)
        assert len(folder_meta.children) == 4
        dest_mock._children_metadata.assert_called_once_with(WaterButlerPath('/folder1/'))
        assert dest_mock.validate_v1_path.call_count == 1

        src_mock._children_metadata.assert_not_called()
        src_mock.validate_v1_path.assert_not_called()


    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_copy_file(self, provider_and_mock, provider_and_mock2,
                                   file_metadata, mock_time):
        src_provider, src_mock = provider_and_mock
        src_mock.intra_copy = src_provider.intra_copy

        dest_provider, dest_mock = provider_and_mock2
        dest_mock.nid = 'abcde'

        src_path = WaterButlerPath('/test_file', _ids=['RootId', 'fileId'], folder=False)
        dest_path = WaterButlerPath('/folder1/', folder=True)

        data=json.dumps({
            'user': src_provider.auth['id'],
            'source': src_path.identifier,
            'destination': {
                'name': dest_path.name,
                'node': dest_provider.nid,
                'parent': dest_path.parent.identifier
            }
        })

        url, params = build_signed_url_without_auth(src_provider, 'POST', 'hooks', 'copy',data=data)
        aiohttpretty.register_json_uri('POST', url, params=params, status=201, body=file_metadata)

        file_meta, created = await src_mock.intra_copy(dest_mock, src_path, dest_path)
        assert created == True
        assert isinstance(file_meta, OsfStorageFileMetadata)
        assert file_meta.name == 'doc.rst'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_copy_file_overwrite(self, provider_and_mock, provider_and_mock2,
                                             mock_time):
        src_provider, src_mock = provider_and_mock
        src_mock.intra_copy = src_provider.intra_copy

        dest_provider, dest_mock = provider_and_mock2
        dest_mock.nid = 'abcde'
        dest_mock.validate_v1_path = utils.MockCoroutine(
            return_value=WaterButlerPath('/file', _ids=('rootId', 'fileId'))
        )

        src_path = WaterButlerPath('/folder1/', _ids=['RootId', 'folder1'], folder=True)
        dest_path = WaterButlerPath('/folder1/',_ids=['RootId', 'folder1'], folder=True)

        data=json.dumps({
            'user': src_provider.auth['id'],
            'source': src_path.identifier,
            'destination': {
                'name': dest_path.name,
                'node': dest_provider.nid,
                'parent': dest_path.parent.identifier
            }
        })
        url, _, params = src_provider.build_signed_url('POST',
                                                       'https://waterbutler.io/hooks/copy/',
                                                       data=data)

        body = {'path': '/file', 'id': 'fileId', 'kind': 'file', 'name': 'file'}
        aiohttpretty.register_json_uri('POST', url, params=params, status=201, body=body)

        file_meta, created = await src_mock.intra_copy(dest_mock, src_path, dest_path)
        assert not created
        assert isinstance(file_meta, OsfStorageFileMetadata)
        assert file_meta.name == 'file'


class TestIntraMove:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_move_folder(self, provider_and_mock, provider_and_mock2,
                                     folder_children_metadata, mock_time):
        src_provider, src_mock = provider_and_mock
        assert src_provider.can_duplicate_names()

        src_mock.intra_move = src_provider.intra_move

        dest_provider, dest_mock = provider_and_mock2
        dest_mock.nid = 'abcde'
        dest_mock._children_metadata = utils.MockCoroutine(return_value=folder_children_metadata)
        dest_mock.validate_v1_path = utils.MockCoroutine(
            return_value=WaterButlerPath('/folder1/', _ids=('rootId', 'folder1'))
        )

        src_path = WaterButlerPath('/folder1/', _ids=['RootId', 'folder1'], folder=True)
        dest_path = WaterButlerPath('/folder1/', _ids=['RootId', 'folder1'], folder=True)

        data=json.dumps({
            'user': src_provider.auth['id'],
            'source': src_path.identifier,
            'destination': {
                'name': dest_path.name,
                'node': dest_provider.nid,
                'parent': dest_path.parent.identifier
            }
        })
        url, _, params = src_provider.build_signed_url('POST', 'https://waterbutler.io/hooks/move/',
                                                       data=data)

        body = {'path': '/folder1/', 'id': 'folder1', 'kind': 'folder', 'name': 'folder1'}
        aiohttpretty.register_json_uri('POST', url, params=params, status=201, body=body)

        folder_meta, created = await src_mock.intra_move(dest_mock, src_path, dest_path)
        assert created == False
        assert isinstance(folder_meta, OsfStorageFolderMetadata)
        assert len(folder_meta.children) == 4
        dest_mock._children_metadata.assert_called_once_with(WaterButlerPath('/folder1/'))
        assert dest_mock.validate_v1_path.call_count == 1

        src_mock._children_metadata.assert_not_called()
        src_mock.validate_v1_path.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_move_file(self, provider_and_mock, provider_and_mock2, mock_time):
        src_provider, src_mock = provider_and_mock
        src_mock.intra_move = src_provider.intra_move

        dest_provider, dest_mock = provider_and_mock2
        dest_mock.nid = 'abcde'

        src_path = WaterButlerPath('/file', _ids=['RootId', 'fileId'], folder=False)
        dest_path = WaterButlerPath('/folder1/', folder=True)

        data = json.dumps({
            'user': src_provider.auth['id'],
            'source': src_path.identifier,
            'destination': {
                'name': dest_path.name,
                'node': dest_provider.nid,
                'parent': dest_path.parent.identifier
            }
        })
        url, _, params = src_provider.build_signed_url(
            'POST',
            'https://waterbutler.io/hooks/move/',
            data=data
        )

        body = {'path': '/file', 'id': 'fileId', 'kind': 'file', 'name': 'file'}
        aiohttpretty.register_json_uri('POST', url, params=params, status=201, body=body)

        file_meta, created = await src_mock.intra_move(dest_mock, src_path, dest_path)
        assert created == True
        assert isinstance(file_meta, OsfStorageFileMetadata)
        assert file_meta.name == 'file'


class TestUtils:

    def test_intra_move_copy_utils(self, provider):
        assert provider.can_duplicate_names()

        assert provider.can_intra_copy(provider)
        assert provider.can_intra_move(provider)

        assert not provider.can_intra_copy(str())
        assert not provider.can_intra_move(str())

    def test_make_provider(self, provider):
        pass


class TestValidatePath:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_path_root(self, provider, root_path, mock_time):
        assert root_path == await provider.validate_path('/')
        assert root_path == await provider.validate_v1_path('/')

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_path_file(self, provider, file_lineage, mock_time):
        file_id = file_lineage['data'][0]['id']

        url, params = build_signed_url_without_auth(provider, 'GET', file_id, 'lineage')
        aiohttpretty.register_json_uri('GET', url, params=params, status=200, body=file_lineage)

        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider.validate_v1_path('/' + file_id + '/')

        assert exc.value.code == client.NOT_FOUND

        wb_path_v0 = await provider.validate_path('/' + file_id)
        wb_path_v1 = await provider.validate_v1_path('/' + file_id)

        expected = WaterButlerPath('/doc.rst')
        assert wb_path_v0 == expected
        assert wb_path_v1 == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_path_folder(self, provider, folder_lineage, mock_time):
        folder_id = folder_lineage['data'][0]['id']

        url, params = build_signed_url_without_auth(provider, 'GET', folder_id, 'lineage')
        aiohttpretty.register_json_uri('GET', url, params=params, status=200, body=folder_lineage)

        with pytest.raises(exceptions.NotFoundError):
            await provider.validate_v1_path('/' + folder_id)

        wb_path_v0 = await provider.validate_path('/' + folder_id)
        wb_path_v1 = await provider.validate_v1_path('/' + folder_id + '/')

        expected = WaterButlerPath('/New Folder/')
        assert wb_path_v0 == expected
        assert wb_path_v1 == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_path_404s(self, provider, file_lineage, mock_time):
        file_id = file_lineage['data'][0]['id']

        url, params = build_signed_url_without_auth(provider, 'GET', file_id, 'lineage')
        aiohttpretty.register_json_uri('GET', url, params=params, status=404, body=file_lineage)

        with pytest.raises(exceptions.UnhandledProviderError):
            await provider.validate_v1_path('/' + file_id)

        wb_path_v0 = await provider.validate_path('/' + file_id)

        assert wb_path_v0 == WaterButlerPath(file_lineage['data'][0]['path'], prepend=None)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_revalidate_path_new(self, provider, folder_path, folder_children_metadata,
                                       mock_time):
        url, params = build_signed_url_without_auth(provider, 'GET', folder_path.identifier,
                                                    'children')
        aiohttpretty.register_json_uri('GET', url, params=params, status=200,
                                       body=folder_children_metadata)

        revalidated_path = await provider.revalidate_path(folder_path, 'new_file', folder=False)

        assert revalidated_path.name == 'new_file'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_revalidate_path_existing(self, provider, folder_path, folder_children_metadata,
                                            mock_time):
        url, params = build_signed_url_without_auth(provider, 'GET', folder_path.identifier,
                                                    'children')
        aiohttpretty.register_json_uri('GET', url, params=params, status=200,
                                       body=folder_children_metadata)

        revalidated_path = await provider.revalidate_path(folder_path,
                                                          folder_children_metadata[1]['name'],
                                                          folder=False)

        assert revalidated_path.name == 'one'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_path_nested(self, provider, file_lineage, folder_lineage, mock_time):
        file_id = file_lineage['data'][0]['id']

        url, params = build_signed_url_without_auth(provider, 'GET', file_id, 'lineage')
        aiohttpretty.register_json_uri('GET', url, params=params, status=200, body=file_lineage)

        url, params = build_signed_url_without_auth(provider, 'GET', 'New%20Folder', 'lineage')
        aiohttpretty.register_json_uri('GET', url, params=params, status=200, body=folder_lineage)

        wb_path_v0 = await provider.validate_path('New Folder/' + file_id)

        assert len(wb_path_v0._parts) == 3
        assert wb_path_v0.name == '59a9b628b7d1c903ab5a8f52'

class TestUploads:

    def patch_tasks(self, monkeypatch):
        basepath = 'waterbutler.providers.osfstorage.provider.{}'
        monkeypatch.setattr(basepath.format('os.rename'), lambda *_: None)
        monkeypatch.setattr(basepath.format('settings.RUN_TASKS'), False)
        monkeypatch.setattr(basepath.format('uuid.uuid4'), lambda: 'patched_path')

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_new(self, monkeypatch, provider_and_mock, file_stream,
                              upload_response, upload_path, mock_time):
        self.patch_tasks(monkeypatch)

        url = 'https://waterbutler.io/{}/children/'.format(upload_path.parent.identifier)
        aiohttpretty.register_json_uri('POST', url, status=201, body=upload_response)

        provider, inner_provider = provider_and_mock
        inner_provider.metadata = utils.MockCoroutine(return_value=utils.MockFileMetadata())

        res, created = await provider.upload(file_stream, upload_path)

        assert created is True
        assert res.name == '[TEST]'
        assert res.extra['version'] == 8
        assert res.provider == 'osfstorage'
        assert res.extra['downloads'] == 0
        assert res.extra['checkout'] is None
        assert upload_path.identifier_path == res.path

        inner_provider.delete.assert_called_once_with(WaterButlerPath('/patched_path'))
        expected_path = WaterButlerPath('/' + file_stream.writers['sha256'].hexdigest)
        inner_provider.metadata.assert_called_once_with(expected_path)
        inner_provider.upload.assert_called_once_with(file_stream, WaterButlerPath('/patched_path'),
                                                      check_created=False, fetch_metadata=False)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_existing(self, monkeypatch, provider_and_mock, file_stream, upload_path,
                                   upload_response, mock_time):
        self.patch_tasks(monkeypatch)
        provider, inner_provider = provider_and_mock

        url = 'https://waterbutler.io/{}/children/'.format(upload_path.parent.identifier)

        inner_provider.move.return_value = (utils.MockFileMetadata(), True)
        inner_provider.metadata.side_effect = exceptions.MetadataError('Boom!', code=404)

        aiohttpretty.register_json_uri('POST', url, status=200, body=upload_response)

        res, created = await provider.upload(file_stream, upload_path)

        assert created is False
        assert res.name == '[TEST]'
        assert res.extra['version'] == 8
        assert res.provider == 'osfstorage'
        assert res.extra['downloads'] == 0
        assert res.extra['checkout'] is None
        assert upload_path.identifier_path == res.path

        expected_path = WaterButlerPath('/' + file_stream.writers['sha256'].hexdigest)
        inner_provider.metadata.assert_called_once_with(expected_path)
        inner_provider.upload.assert_called_once_with(file_stream,
                                                      WaterButlerPath('/patched_path'),
                                                      check_created=False,
                                                      fetch_metadata=False)
        inner_provider.move.assert_called_once_with(inner_provider,
                                                    WaterButlerPath('/patched_path'),
                                                    expected_path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_catch_non_404_errors(self, monkeypatch, provider_and_mock, file_stream,
                                               upload_path, mock_time):
        self.patch_tasks(monkeypatch)
        provider, inner_provider = provider_and_mock

        url = 'https://waterbutler.io/{}/children/'.format(upload_path.parent.identifier)

        inner_provider.metadata.side_effect = exceptions.MetadataError('Boom!', code=500)
        aiohttpretty.register_json_uri('POST', url, status=500)

        with pytest.raises(exceptions.MetadataError):
            await provider.upload(file_stream, upload_path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_and_tasks(self, monkeypatch, provider_and_mock, file_stream,
                                    upload_response, credentials, settings, mock_time):
        provider, inner_provider = provider_and_mock
        basepath = 'waterbutler.providers.osfstorage.provider.{}'
        path = WaterButlerPath('/' + upload_response['data']['name'],
                               _ids=('Test', upload_response['data']['id']))
        url = 'https://waterbutler.io/{}/children/'.format(path.parent.identifier)

        mock_parity = mock.Mock()
        mock_backup = mock.Mock()
        inner_provider.move.return_value = (utils.MockFileMetadata(), True)
        inner_provider.metadata.side_effect = exceptions.MetadataError('Boom!', code=404)

        aiohttpretty.register_json_uri('POST', url, status=201, body=upload_response)
        monkeypatch.setattr(basepath.format('backup.main'), mock_backup)
        monkeypatch.setattr(basepath.format('parity.main'), mock_parity)
        monkeypatch.setattr(basepath.format('settings.RUN_TASKS'), True)
        monkeypatch.setattr(basepath.format('os.rename'), lambda *_: None)
        monkeypatch.setattr(basepath.format('uuid.uuid4'), lambda: 'uniquepath')

        res, created = await provider.upload(file_stream, path)

        assert created is True
        assert res.name == '[TEST]'
        assert res.extra['version'] == 8
        assert res.provider == 'osfstorage'
        assert res.extra['downloads'] == 0
        assert res.extra['checkout'] is None

        inner_provider.upload.assert_called_once_with(file_stream, WaterButlerPath('/uniquepath'),
                                                      check_created=False, fetch_metadata=False)
        complete_path = os.path.join(FILE_PATH_COMPLETE, file_stream.writers['sha256'].hexdigest)
        mock_parity.assert_called_once_with(complete_path, credentials['parity'],
                                            settings['parity'])
        mock_backup.assert_called_once_with(complete_path, upload_response['version'],
                                            'https://waterbutler.io/hooks/metadata/',
                                            credentials['archive'], settings['parity'])
        expected_path = WaterButlerPath('/' + file_stream.writers['sha256'].hexdigest)
        inner_provider.metadata.assert_called_once_with(expected_path)
        inner_provider.move.assert_called_once_with(inner_provider, WaterButlerPath('/uniquepath'),
                                                    expected_path)
