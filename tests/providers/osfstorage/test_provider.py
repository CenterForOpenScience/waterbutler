import os
import json
from http import client
from unittest import mock

import pytest
import aiohttpretty

from waterbutler.core import metadata
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath
from waterbutler.providers.osfstorage.provider import OSFStorageProvider
from waterbutler.providers.osfstorage.metadata import (OsfStorageFileMetadata,
                                                       OsfStorageFolderMetadata,
                                                       OsfStorageRevisionMetadata)
from waterbutler.providers.osfstorage.exceptions import OsfStorageQuotaExceededError

from tests import utils
from tests.providers.osfstorage.fixtures import (auth, credentials, settings,
                                                 settings_region_one, settings_region_two,
                                                 provider_one, provider_two,
                                                 provider_and_mock_one, provider_and_mock_two,
                                                 file_stream, file_like, file_content,
                                                 file_lineage, file_metadata,
                                                 file_metadata_object, file_path,
                                                 folder_lineage, folder_metadata,
                                                 folder_children_metadata, folder_path,
                                                 revisions_metadata, revision_metadata_object,
                                                 download_response, download_path,
                                                 upload_response, upload_path, root_path,
                                                 mock_time, mock_inner_provider,)


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
    async def test_create_folder(self, folder_path, provider_one, folder_metadata, mock_time):

        data = json.dumps(folder_metadata)
        url, params = build_signed_url_without_auth(provider_one, 'POST',
                                                    folder_path.parent.identifier,
                                                    'children', data=data)

        aiohttpretty.register_json_uri('POST', url, body=folder_metadata, status=201, params=params)

        resp = await provider_one.create_folder(folder_path)

        assert isinstance(resp, OsfStorageFolderMetadata)


class TestDownload:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_with_auth(self, provider_and_mock_one, download_response,
                                      download_path, mock_time):

        provider, inner_provider = provider_and_mock_one

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
                                                        display_name=expected_display_name)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_without_auth(self, provider_and_mock_one, download_response,
                                         download_path, mock_time):
        provider, inner_provider = provider_and_mock_one

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
                                                        display_name=expected_display_name)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_without_id(self, provider_one, download_response, file_path,
                                       mock_time):

        url, params = build_signed_url_without_auth(provider_one, 'GET', file_path.identifier,
                                                    'download', version=None, mode=None)
        aiohttpretty.register_json_uri('GET', url, params=params, body=download_response)
        file_path._parts[-1]._id = None

        with pytest.raises(exceptions.NotFoundError):
            await provider_one.download(file_path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize("display_name_arg,expected_name", [
        ('meow.txt', 'meow.txt'),
        ('',         'doc.rst'),
        (None,       'doc.rst'),
    ])
    async def test_download_with_display_name(self, provider_and_mock_one, download_response,
                                              download_path, mock_time, display_name_arg,
                                              expected_name):

        provider, inner_provider = provider_and_mock_one

        uri, params = build_signed_url_with_auth(provider, 'GET', download_path.identifier,
                                                 'download', version=None, mode=None)

        aiohttpretty.register_json_uri('GET', uri,  body=download_response, params=params)

        await provider.download(download_path, display_name=display_name_arg)

        assert provider.make_provider.called
        assert inner_provider.download.called

        assert aiohttpretty.has_call(method='GET', uri=uri, params=params)
        provider.make_provider.assert_called_once_with(download_response['settings'])
        expected_path = WaterButlerPath('/' + download_response['data']['path'])
        inner_provider.download.assert_called_once_with(path=expected_path,
                                                        display_name=expected_name)


class TestDelete:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete(self, provider_one, file_path, mock_time):

        url,  params = build_signed_url_with_auth(provider_one, 'DELETE', file_path.identifier)
        aiohttpretty.register_uri('DELETE', url, status_code=200, params=params)

        await provider_one.delete(file_path)

        assert aiohttpretty.has_call(method='DELETE', uri=url, check_params=False)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_without_id(self, provider_one, file_path, mock_time):

        url, params = build_signed_url_without_auth(provider_one, 'DELETE', file_path.identifier)
        aiohttpretty.register_uri('DELETE', url, status_code=200)

        file_path._parts[-1]._id = None

        with pytest.raises(exceptions.NotFoundError):
            await provider_one.delete(file_path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_root(self, provider_one, root_path, mock_time):

        provider_one._delete_folder_contents = utils.MockCoroutine()

        url, params = build_signed_url_without_auth(provider_one, 'DELETE', root_path.identifier)
        aiohttpretty.register_uri('DELETE', url,  status_code=200)

        with pytest.raises(exceptions.DeleteError):
            await provider_one.delete(root_path)

        provider_one._delete_folder_contents.assert_not_called()

        await provider_one.delete(root_path, confirm_delete=1)
        provider_one._delete_folder_contents.assert_called_once_with(root_path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_folder_contents(self, provider_one, file_path, folder_path,
                                          folder_children_metadata, mock_time):

        provider_one.validate_path = utils.MockCoroutine(return_value=file_path)
        provider_one.delete = utils.MockCoroutine()

        children_url, params = build_signed_url_without_auth(provider_one, 'GET',
                                                             folder_path.identifier, 'children',
                                                             user_id=provider_one.auth['id'])
        aiohttpretty.register_json_uri('GET', children_url, params=params, status=200,
                                       body=folder_children_metadata)

        await provider_one._delete_folder_contents(folder_path)

        provider_one.delete.assert_called_with(file_path)
        assert provider_one.delete.call_count == 4


class TestMetadata:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_provider_metadata_empty(self, provider_one, folder_path, mock_time):

        url, params = build_signed_url_without_auth(provider_one, 'GET', folder_path.identifier,
                                                    'children', user_id=provider_one.auth['id'])
        aiohttpretty.register_json_uri('GET', url, params=params, status_code=200, body=[])

        res = await provider_one.metadata(folder_path)

        assert res == []
        assert aiohttpretty.has_call(method='GET', uri=url, params=params)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_provider_metadata_folder(self, provider_one, folder_path,
                                            folder_children_metadata, mock_time):

        url, params = build_signed_url_without_auth(provider_one, 'GET', folder_path.identifier,
                                                    'children', user_id=provider_one.auth['id'])
        aiohttpretty.register_json_uri('GET', url, params=params, status=200,
                                       body=folder_children_metadata)

        res = await provider_one.metadata(folder_path)

        assert isinstance(res, list)
        for item in res:
            assert isinstance(item, metadata.BaseMetadata)
            assert item.name is not None
            assert item.path is not None
            assert item.provider == 'osfstorage'

        assert aiohttpretty.has_call(method='GET', uri=url, params=params)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_provider_metadata_file(self, provider_one, file_path, file_metadata, mock_time):

        url, params = build_signed_url_without_auth(provider_one, 'GET', file_path.identifier)
        aiohttpretty.register_json_uri('GET', url, params=params, status=200, body=file_metadata)

        res = await provider_one.metadata(file_path)

        assert isinstance(res, OsfStorageFileMetadata)
        assert res.name is not None
        assert res.path is not None
        assert res.provider == 'osfstorage'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_provider_metadata_without_id(self, provider_one, folder_path,
                                                folder_children_metadata, mock_time):

        url, params = build_signed_url_without_auth(provider_one, 'GET', folder_path.identifier,
                                                    'children')
        aiohttpretty.register_json_uri('GET', url, params=params, status=200,
                                       body=folder_children_metadata)
        folder_path._parts[-1]._id = None

        with pytest.raises(exceptions.MetadataError):
            await provider_one.metadata(folder_path)


class TestRevisions:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_revisions(self, provider_one, file_path, revisions_metadata, mock_time):

        url, params = build_signed_url_without_auth(provider_one, 'GET', file_path.identifier,
                                                    'revisions')
        aiohttpretty.register_json_uri('GET', url, params=params, status=200,
                                       body=revisions_metadata)

        response = await provider_one.revisions(file_path)

        assert isinstance(response, list)
        for index, revision in enumerate(response):
            assert isinstance(revision, OsfStorageRevisionMetadata)
            assert revision.raw == revisions_metadata['revisions'][index]

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_revisions_without_id(self, provider_one, file_path, revisions_metadata,
                                        mock_time):

        url, params = build_signed_url_without_auth(provider_one, 'GET', file_path.identifier,
                                                    'revisions')
        aiohttpretty.register_json_uri('GET', url, params=params, status=200,
                                       body=revisions_metadata)
        file_path._parts[-1]._id = None

        with pytest.raises(exceptions.MetadataError):
            await provider_one.revisions(file_path)


class TestIntraMoveCopy:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('action, method_name', [
        ('move', 'intra_move'),
        ('copy', 'intra_copy'),
    ])
    async def test_intra_foo_folder(self, provider_one, provider_two, folder_children_metadata,
                                    mock_time, action, method_name):

        src_provider = provider_one
        src_provider.delete = utils.MockCoroutine()
        src_provider.validate_v1_path = utils.MockCoroutine()
        src_provider._children_metadata = utils.MockCoroutine()

        dest_provider = provider_two
        dest_provider.delete = utils.MockCoroutine()
        dest_provider.validate_v1_path = utils.MockCoroutine(
            return_value=WaterButlerPath('/folder1/', _ids=('RootId', 'folder1'))
        )
        dest_provider._children_metadata = utils.MockCoroutine(
            return_value=folder_children_metadata
        )

        src_path = WaterButlerPath('/folder1/', _ids=['RootId', 'folder1'], folder=True)
        dest_path = WaterButlerPath('/folder1/', _ids=['RootId'], folder=True)

        data = json.dumps({
            'user': src_provider.auth['id'],
            'source': src_path.identifier,
            'destination': {
                'name': dest_path.name,
                'node': dest_provider.nid,
                'parent': dest_path.parent.identifier
            }
        })
        url, params = build_signed_url_without_auth(src_provider, 'POST', 'hooks', action,
                                                    data=data)
        body = {'path': '/folder1/', 'id': 'folder1', 'kind': 'folder', 'name': 'folder1'}
        aiohttpretty.register_json_uri('POST', url, params=params, status=201, body=body)

        method = getattr(src_provider, method_name)
        folder_meta, created = await method(dest_provider, src_path, dest_path)

        assert created
        assert isinstance(folder_meta, OsfStorageFolderMetadata)
        assert len(folder_meta.children) == 4

        # these should be called on dest_provider (if at all), not src_provider
        src_provider.delete.assert_not_called()
        src_provider.validate_v1_path.assert_not_called()
        src_provider._children_metadata.assert_not_called()

        # delete isn't called, b/c dest_path doesn't already exist
        dest_provider.delete.assert_not_called()
        dest_provider.validate_v1_path.assert_called_once_with('/folder1/')
        dest_provider._children_metadata.assert_called_once_with(WaterButlerPath('/folder1/'))

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('action, method_name', [
        ('move', 'intra_move'),
        ('copy', 'intra_copy'),
    ])
    async def test_intra_foo_file(self, provider_one, provider_two, file_metadata, mock_time,
                                  action, method_name):

        src_provider = provider_one
        src_provider.delete = utils.MockCoroutine()
        src_provider.validate_v1_path = utils.MockCoroutine()
        src_provider._children_metadata = utils.MockCoroutine()

        dest_provider = provider_two
        dest_provider.delete = utils.MockCoroutine()
        dest_provider.validate_v1_path = utils.MockCoroutine()
        dest_provider._children_metadata = utils.MockCoroutine()

        src_path = WaterButlerPath('/test_file', _ids=['RootId', 'fileId'], folder=False)
        dest_path = WaterButlerPath('/folder1/test_file', _ids=['RootId', 'folderId'],
                                    folder=False)

        data = json.dumps({
            'user': src_provider.auth['id'],
            'source': src_path.identifier,
            'destination': {
                'name': dest_path.name,
                'node': dest_provider.nid,
                'parent': dest_path.parent.identifier
            }
        })

        url, params = build_signed_url_without_auth(src_provider, 'POST', 'hooks', action,
                                                    data=data)
        aiohttpretty.register_json_uri('POST', url, params=params, status=201, body=file_metadata)

        method = getattr(src_provider, method_name)
        file_meta, created = await method(dest_provider, src_path, dest_path)

        assert created == True
        assert isinstance(file_meta, OsfStorageFileMetadata)
        assert file_meta.name == 'doc.rst'

        # these should be called on dest_provider (if at all), not src_provider
        src_provider.delete.assert_not_called()
        src_provider.validate_v1_path.assert_not_called()
        src_provider._children_metadata.assert_not_called()

        # delete isn't called, b/c dest_path doesn't already exist
        # others aren't called b/c copied entity isa file
        dest_provider.delete.assert_not_called()
        dest_provider.validate_v1_path.assert_not_called()
        dest_provider._children_metadata.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('action, method_name', [
        ('move', 'intra_move'),
        ('copy', 'intra_copy'),
    ])
    async def test_intra_foo_folder_overwrite(self, provider_one, provider_two,
                                              folder_children_metadata, mock_time, action,
                                              method_name):

        src_provider = provider_one
        src_provider.delete = utils.MockCoroutine()
        src_provider.validate_v1_path = utils.MockCoroutine()
        src_provider._children_metadata = utils.MockCoroutine()

        dest_provider = provider_two
        dest_provider.delete = utils.MockCoroutine()
        dest_provider.validate_v1_path = utils.MockCoroutine(
            return_value=WaterButlerPath('/folder1/', _ids=('RootId', 'folder1'))
        )
        dest_provider._children_metadata = utils.MockCoroutine(
            return_value=folder_children_metadata
        )

        src_path = WaterButlerPath('/folder1/', _ids=['RootId', 'folder1'], folder=True)
        dest_path = WaterButlerPath('/folder1/', _ids=['RootId', 'doomedFolder'], folder=True)

        data = json.dumps({
            'user': src_provider.auth['id'],
            'source': src_path.identifier,
            'destination': {
                'name': dest_path.name,
                'node': dest_provider.nid,
                'parent': dest_path.parent.identifier
            }
        })
        url, params = build_signed_url_without_auth(src_provider, 'POST', 'hooks', action,
                                                    data=data)
        body = {'path': '/folder1/', 'id': 'folder1', 'kind': 'folder', 'name': 'folder1'}
        aiohttpretty.register_json_uri('POST', url, params=params, status=201, body=body)

        method = getattr(src_provider, method_name)
        folder_meta, created = await method(dest_provider, src_path, dest_path)

        assert not created
        assert isinstance(folder_meta, OsfStorageFolderMetadata)
        assert len(folder_meta.children) == 4

        # these should be called on dest_provider (if at all), not src_provider
        src_provider.delete.assert_not_called()
        src_provider.validate_v1_path.assert_not_called()
        src_provider._children_metadata.assert_not_called()

        dest_provider.delete.assert_called_once_with(WaterButlerPath('/folder1/'))
        dest_provider.validate_v1_path.assert_called_once_with('/folder1/')
        dest_provider._children_metadata.assert_called_once_with(WaterButlerPath('/folder1/'))

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('action, method_name', [
        ('move', 'intra_move'),
        ('copy', 'intra_copy'),
    ])
    async def test_intra_foo_file_overwrite(self, provider_one, provider_two,
                                            file_metadata, mock_time, action, method_name):

        src_provider = provider_one
        src_provider.delete = utils.MockCoroutine()
        src_provider.validate_v1_path = utils.MockCoroutine()
        src_provider._children_metadata = utils.MockCoroutine()

        dest_provider = provider_two
        dest_provider.delete = utils.MockCoroutine()
        dest_provider.validate_v1_path = utils.MockCoroutine()
        dest_provider._children_metadata = utils.MockCoroutine()

        src_path = WaterButlerPath('/test_file', _ids=['RootId', 'fileId'], folder=False)
        dest_path = WaterButlerPath('/folder1/test_file',
                                    _ids=['RootId', 'folder1Id', 'doomedFile'],
                                    folder=False)

        data = json.dumps({
            'user': src_provider.auth['id'],
            'source': src_path.identifier,
            'destination': {
                'name': dest_path.name,
                'node': dest_provider.nid,
                'parent': dest_path.parent.identifier
            }
        })

        url, params = build_signed_url_without_auth(src_provider, 'POST', 'hooks', action,
                                                    data=data)
        aiohttpretty.register_json_uri('POST', url, params=params, status=201, body=file_metadata)

        method = getattr(src_provider, method_name)
        file_meta, created = await method(dest_provider, src_path, dest_path)

        assert not created
        assert isinstance(file_meta, OsfStorageFileMetadata)
        assert file_meta.name == 'doc.rst'

        # these should be called on dest_provider (if at all), not src_provider
        src_provider.delete.assert_not_called()
        src_provider.validate_v1_path.assert_not_called()
        src_provider._children_metadata.assert_not_called()

        # vvp & _cm aren't called b/c copied entity isa file
        dest_provider.delete.assert_called_once_with(WaterButlerPath('/folder1/test_file'))
        dest_provider.validate_v1_path.assert_not_called()
        dest_provider._children_metadata.assert_not_called()


class TestUtils:

    def test_is_same_region_true(self, provider_one):
        assert provider_one.is_same_region(provider_one)

    def test_is_same_region_false(self, provider_one, provider_two):
        assert not provider_one.is_same_region(provider_two)

    def test_is_same_region_error(self, provider_one):

        with pytest.raises(AssertionError) as exc:
            provider_one.is_same_region(str())
        assert str(exc.value) == 'Cannot compare region for providers of different provider ' \
                                 'classes.'

    def test_can_intra_move_copy_true(self, provider_one):
        assert provider_one.can_intra_copy(provider_one)
        assert provider_one.can_intra_move(provider_one)

    def test_can_intra_move_copy_false_region_mismatch(self, provider_one, provider_two):
        assert not provider_one.can_intra_copy(provider_two)
        assert not provider_one.can_intra_move(provider_two)

    def test_can_intra_move_copy_false_class_mismatch(self, provider_one):
        assert not provider_one.can_intra_copy(str())
        assert not provider_one.can_intra_move(str())

    def test_can_duplicate_names(self, provider_one):
        assert provider_one.can_duplicate_names()

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test__check_resource_quota_retries_zero(self, provider_one, monkeypatch, mock_time):

        monkeypatch.setattr('waterbutler.providers.osfstorage.settings.QUOTA_RETRIES', 2)
        monkeypatch.setattr('waterbutler.providers.osfstorage.settings.QUOTA_RETRIES_DELAY', 1)

        responses = [
            {
                'body': json.dumps({'over_quota': True}),
                'status': 200,
                'headers': {'Content-Type': 'application/json'},
            },
        ]

        quota_url, quota_params = build_signed_url_without_auth(provider_one, 'GET', 'quota_status')
        aiohttpretty.register_json_uri('GET', quota_url, params=quota_params, responses=responses)

        resp = await provider_one._check_resource_quota()
        assert resp == {'over_quota': True}

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test__check_resource_quota_retries_one(self, provider_one, monkeypatch, mock_time):

        monkeypatch.setattr('waterbutler.providers.osfstorage.settings.QUOTA_RETRIES', 2)
        monkeypatch.setattr('waterbutler.providers.osfstorage.settings.QUOTA_RETRIES_DELAY', 1)

        responses = [
            {'status': 202,},
            {
                'body': json.dumps({'over_quota': True}),
                'status': 200,
                'headers': {'Content-Type': 'application/json'},
            },
        ]

        quota_url, quota_params = build_signed_url_without_auth(provider_one, 'GET', 'quota_status')
        aiohttpretty.register_json_uri('GET', quota_url, params=quota_params, responses=responses)

        resp = await provider_one._check_resource_quota()
        assert resp == {'over_quota': True}

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test__check_resource_quota_retries_two(self, provider_one, monkeypatch, mock_time):

        monkeypatch.setattr('waterbutler.providers.osfstorage.settings.QUOTA_RETRIES', 2)
        monkeypatch.setattr('waterbutler.providers.osfstorage.settings.QUOTA_RETRIES_DELAY', 1)

        responses = [
            {'status': 202,},
            {'status': 202,},
            {
                'body': json.dumps({'over_quota': True}),
                'status': 200,
                'headers': {'Content-Type': 'application/json'},
            },
        ]

        quota_url, quota_params = build_signed_url_without_auth(provider_one, 'GET', 'quota_status')
        aiohttpretty.register_json_uri('GET', quota_url, params=quota_params, responses=responses)

        resp = await provider_one._check_resource_quota()
        assert resp == {'over_quota': True}

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test__check_resource_quota_exhaust_retries(self, provider_one, monkeypatch, mock_time):

        monkeypatch.setattr('waterbutler.providers.osfstorage.settings.QUOTA_RETRIES', 2)
        monkeypatch.setattr('waterbutler.providers.osfstorage.settings.QUOTA_RETRIES_DELAY', 1)

        responses = [
            {'status': 202,},
            {'status': 202,},
            {'status': 202,},
            {
                'body': json.dumps({'over_quota': True}),
                'status': 200,
                'headers': {'Content-Type': 'application/json'},
            },
        ]

        quota_url, quota_params = build_signed_url_without_auth(provider_one, 'GET', 'quota_status')
        aiohttpretty.register_json_uri('GET', quota_url, params=quota_params, responses=responses)

        resp = await provider_one._check_resource_quota()
        assert resp == {'over_quota': False}


class TestValidatePath:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_path_root(self, provider_one, root_path, mock_time):
        assert root_path == await provider_one.validate_path('/')
        assert root_path == await provider_one.validate_v1_path('/')

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_path_file(self, provider_one, file_lineage, mock_time):
        file_id = file_lineage['data'][0]['id']

        url, params = build_signed_url_without_auth(provider_one, 'GET', file_id, 'lineage')
        aiohttpretty.register_json_uri('GET', url, params=params, status=200, body=file_lineage)

        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider_one.validate_v1_path('/' + file_id + '/')

        assert exc.value.code == client.NOT_FOUND

        wb_path_v0 = await provider_one.validate_path('/' + file_id)
        wb_path_v1 = await provider_one.validate_v1_path('/' + file_id)

        expected = WaterButlerPath('/doc.rst')
        assert wb_path_v0 == expected
        assert wb_path_v1 == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_path_folder(self, provider_one, folder_lineage, mock_time):
        folder_id = folder_lineage['data'][0]['id']

        url, params = build_signed_url_without_auth(provider_one, 'GET', folder_id, 'lineage')
        aiohttpretty.register_json_uri('GET', url, params=params, status=200, body=folder_lineage)

        with pytest.raises(exceptions.NotFoundError):
            await provider_one.validate_v1_path('/' + folder_id)

        wb_path_v0 = await provider_one.validate_path('/' + folder_id)
        wb_path_v1 = await provider_one.validate_v1_path('/' + folder_id + '/')

        expected = WaterButlerPath('/New Folder/')
        assert wb_path_v0 == expected
        assert wb_path_v1 == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_path_404s(self, provider_one, file_lineage, mock_time):
        file_id = file_lineage['data'][0]['id']

        url, params = build_signed_url_without_auth(provider_one, 'GET', file_id, 'lineage')
        aiohttpretty.register_json_uri('GET', url, params=params, status=404, body=file_lineage)

        with pytest.raises(exceptions.UnhandledProviderError):
            await provider_one.validate_v1_path('/' + file_id)

        wb_path_v0 = await provider_one.validate_path('/' + file_id)

        assert wb_path_v0 == WaterButlerPath(file_lineage['data'][0]['path'], prepend=None)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_revalidate_path_new(self, provider_one, folder_path, folder_children_metadata,
                                       mock_time):
        url, params = build_signed_url_without_auth(provider_one, 'GET', folder_path.identifier,
                                                    'children', user_id=provider_one.auth['id'])
        aiohttpretty.register_json_uri('GET', url, params=params, status=200,
                                       body=folder_children_metadata)

        revalidated_path = await provider_one.revalidate_path(folder_path, 'new_file', folder=False)

        assert revalidated_path.name == 'new_file'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_revalidate_path_existing(self, provider_one, folder_path,
                                            folder_children_metadata, mock_time):
        url, params = build_signed_url_without_auth(provider_one, 'GET', folder_path.identifier,
                                                    'children', user_id=provider_one.auth['id'])
        aiohttpretty.register_json_uri('GET', url, params=params, status=200,
                                       body=folder_children_metadata)

        revalidated_path = await provider_one.revalidate_path(folder_path,
                                                              folder_children_metadata[1]['name'],
                                                              folder=False)

        assert revalidated_path.name == 'one'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_path_nested(self, provider_one, file_lineage, folder_lineage,
                                        mock_time):
        file_id = file_lineage['data'][0]['id']

        url, params = build_signed_url_without_auth(provider_one, 'GET', file_id, 'lineage')
        aiohttpretty.register_json_uri('GET', url, params=params, status=200, body=file_lineage)

        url, params = build_signed_url_without_auth(provider_one, 'GET', 'New Folder', 'lineage')
        aiohttpretty.register_json_uri('GET', url, params=params, status=200, body=folder_lineage)

        wb_path_v0 = await provider_one.validate_path('New Folder/' + file_id)

        assert len(wb_path_v0._parts) == 3
        assert wb_path_v0.name == '59a9b628b7d1c903ab5a8f52'


class TestUploads:

    def patch_uuid(self, monkeypatch):
        basepath = 'waterbutler.providers.osfstorage.provider.{}'
        monkeypatch.setattr(basepath.format('uuid.uuid4'), lambda: 'patched_path')

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_new(self, monkeypatch, provider_and_mock_one, file_stream,
                              upload_response, upload_path, mock_time):
        self.patch_uuid(monkeypatch)

        url = 'https://waterbutler.io/{}/children/'.format(upload_path.parent.identifier)
        aiohttpretty.register_json_uri('POST', url, status=201, body=upload_response)

        provider, inner_provider = provider_and_mock_one
        inner_provider.metadata = utils.MockCoroutine(return_value=utils.MockFileMetadata())

        quota_url, quota_params = build_signed_url_without_auth(provider, 'GET', 'quota_status')
        aiohttpretty.register_json_uri('GET', quota_url, params=quota_params, status=200,
                                       body={'over_quota': False})

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
    async def test_upload_existing(self, monkeypatch, provider_and_mock_one, file_stream,
                                   upload_path, upload_response, mock_time):
        self.patch_uuid(monkeypatch)
        provider, inner_provider = provider_and_mock_one

        url = 'https://waterbutler.io/{}/children/'.format(upload_path.parent.identifier)

        quota_url, quota_params = build_signed_url_without_auth(provider, 'GET', 'quota_status')
        aiohttpretty.register_json_uri('GET', quota_url, params=quota_params, status=200,
                                       body={'over_quota': False})

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
    async def test_upload_catch_non_404_errors(self, monkeypatch, provider_and_mock_one, file_stream,
                                               upload_path, mock_time):
        self.patch_uuid(monkeypatch)
        provider, inner_provider = provider_and_mock_one

        quota_url, quota_params = build_signed_url_without_auth(provider, 'GET', 'quota_status')
        aiohttpretty.register_json_uri('GET', quota_url, params=quota_params, status=200,
                                       body={'over_quota': False})

        url = 'https://waterbutler.io/{}/children/'.format(upload_path.parent.identifier)

        inner_provider.metadata.side_effect = exceptions.MetadataError('Boom!', code=500)
        aiohttpretty.register_json_uri('POST', url, status=500)

        with pytest.raises(exceptions.MetadataError):
            await provider.upload(file_stream, upload_path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_fails(self, monkeypatch, provider_and_mock_one, file_stream,
                                upload_response, mock_time):
        self.patch_uuid(monkeypatch)
        provider, inner_provider = provider_and_mock_one
        path = WaterButlerPath('/{}'.format(upload_response['data']['name']),
                               _ids=('Test', upload_response['data']['id']))
        url = 'https://waterbutler.io/{}/children/'.format(path.parent.identifier)

        aiohttpretty.register_json_uri('POST', url, status=201, body=upload_response)
        inner_provider.metadata = utils.MockCoroutine(return_value=utils.MockFileMetadata())
        inner_provider.upload.side_effect = Exception()

        quota_url, quota_params = build_signed_url_without_auth(provider, 'GET', 'quota_status')
        aiohttpretty.register_json_uri('GET', quota_url, params=quota_params, status=200,
                                       body={'over_quota': False})

        with pytest.raises(Exception):
            await provider.upload(file_stream, path)

        inner_provider.upload.assert_called_once_with(
            file_stream,
            WaterButlerPath('/patched_path'),
            check_created=False,
            fetch_metadata=False
        )

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_reject_quota(self, monkeypatch, provider_and_mock_one, file_stream,
                                       upload_path, mock_time):
        self.patch_uuid(monkeypatch)

        provider, inner_provider = provider_and_mock_one
        provider._send_to_storage_provider = utils.MockCoroutine()
        provider._send_to_metadata_provider = utils.MockCoroutine()

        quota_url, quota_params = build_signed_url_without_auth(provider, 'GET', 'quota_status')
        aiohttpretty.register_json_uri('GET', quota_url, params=quota_params, status=200,
                                       body={'over_quota': True})

        with pytest.raises(OsfStorageQuotaExceededError):
            await provider.upload(file_stream, upload_path)

        provider._send_to_storage_provider.assert_not_called()
        provider._send_to_metadata_provider.assert_not_called()


class TestCrossRegionMove:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_move_file(self, provider_one, provider_two, file_stream, upload_response):

        # aliased for clarity
        src_provider, dst_provider = provider_one, provider_two

        src_provider.download = utils.MockCoroutine(return_value=file_stream)
        src_provider.intra_move = utils.MockCoroutine(return_value=(upload_response, True))
        dst_provider._send_to_storage_provider = utils.MockCoroutine()

        src_path = WaterButlerPath('/foo', _ids=('Test', '56ab34'))
        dest_path = WaterButlerPath('/', _ids=('Test',))

        quota_url, quota_params = build_signed_url_without_auth(dst_provider, 'GET', 'quota_status')
        aiohttpretty.register_json_uri('GET', quota_url, params=quota_params, status=200,
                                       body={'over_quota': False})

        metadata, created = await src_provider.move(dst_provider, src_path, dest_path,
                                                    handle_naming=False);

        assert metadata is not None
        assert created is True

        src_provider.download.assert_called_once_with(WaterButlerPath('/foo'))
        dst_provider._send_to_storage_provider.assert_called_once_with(file_stream,
                                                                       WaterButlerPath('/'),
                                                                       rename=None,
                                                                       conflict='replace')
        src_provider.intra_move.assert_called_once_with(dst_provider, WaterButlerPath('/foo'),
                                                        WaterButlerPath('/'))

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_move_file_reject_quota(self, provider_one, provider_two):

        # aliased for clarity
        src_provider, dst_provider = provider_one, provider_two

        src_provider.download = utils.MockCoroutine()
        src_provider.intra_move = utils.MockCoroutine()
        dst_provider._send_to_storage_provider = utils.MockCoroutine()

        src_path = WaterButlerPath('/foo', _ids=('Test', '56ab34'))
        dest_path = WaterButlerPath('/', _ids=('Test',))

        quota_url, quota_params = build_signed_url_without_auth(dst_provider, 'GET', 'quota_status')
        aiohttpretty.register_json_uri('GET', quota_url, params=quota_params, status=200,
                                       body={'over_quota': True})

        with pytest.raises(OsfStorageQuotaExceededError):
            await src_provider.move(dst_provider, src_path, dest_path, handle_naming=False)

        src_provider.download.assert_not_called()
        dst_provider._send_to_storage_provider.assert_not_called()
        src_provider.intra_move.assert_not_called()

    @pytest.mark.asyncio
    async def test_move_folder(self, provider_one, provider_two):

        # aliased for clarity
        src_provider, dst_provider = provider_one, provider_two

        src_provider._folder_file_op = utils.MockCoroutine(return_value=(upload_response, True))
        src_provider.delete = utils.MockCoroutine()

        src_path = WaterButlerPath('/foo/', _ids=('Test', '56ab34'), folder=True)
        dest_path = WaterButlerPath('/', _ids=('Test',), folder=True)

        metadata, created = await src_provider.move(dst_provider, src_path, dest_path,
                                                    handle_naming=False);

        assert metadata is not None
        assert created is True

        src_provider._folder_file_op.assert_called_once_with(src_provider.move,
                                                             dst_provider,
                                                             WaterButlerPath('/foo/'),
                                                             WaterButlerPath('/'),
                                                             rename=None,
                                                             conflict='replace')
        src_provider.delete.assert_called_once_with(WaterButlerPath('/foo/'))

    @pytest.mark.asyncio
    async def test_move_cross_provider(self, monkeypatch, provider_one, provider_two):

        # aliased for clarity
        src_provider, dst_provider = provider_one, provider_two

        src_provider.download = utils.MockCoroutine()
        dst_provider.NAME = 'not-osfstorage'

        core_move = utils.MockCoroutine()
        monkeypatch.setattr('waterbutler.core.provider.BaseProvider.move', core_move)

        src_path = WaterButlerPath('/foo', _ids=('Test', '56ab34'))
        dest_path = WaterButlerPath('/', _ids=('Test',))

        await src_provider.move(dst_provider, src_path, dest_path, handle_naming=False);

        core_move.assert_called_once_with(dst_provider, src_path, dest_path, rename=None,
                                          conflict='replace', handle_naming=False);
        src_provider.download.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_move_but_intra_move(self, provider_one, auth, credentials,
                                       settings_region_one):
        """OSFStorageProvider.move checks to see if intra_move can be called as an optimization.
        If the destination is not `osfstorage`, delegate to the parent method.  Otherwise, check
        whether we can optimize by doing an `intra_move` action.  `intra_move` is permissable when
        both `osfstorage` providers are in the same region."""

        # aliased for clarity
        src_provider = provider_one

        settings_region_one['nid'] = 'fake-nid'
        dst_provider = OSFStorageProvider(auth, credentials, settings_region_one)

        src_provider.can_intra_move = mock.Mock(return_value=True)
        src_provider.intra_move = utils.MockCoroutine()
        src_provider.download = utils.MockCoroutine()

        src_path = WaterButlerPath('/foo', _ids=('Test', '56ab34'))
        dest_path = WaterButlerPath('/', _ids=('Test',))

        quota_url, quota_params = build_signed_url_without_auth(dst_provider, 'GET', 'quota_status')
        aiohttpretty.register_json_uri('GET', quota_url, params=quota_params, status=200,
                                       body={'over_quota': False})

        await src_provider.move(dst_provider, src_path, dest_path, handle_naming=False);

        src_provider.can_intra_move.assert_called_once_with(dst_provider, src_path)
        src_provider.intra_move.assert_called_once_with(dst_provider, src_path, dest_path)
        src_provider.download.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_move_reject_by_quota(self, provider_one, auth, credentials,
                                              settings_region_one):
        """Same as previous, but assume the destination node is not the same as the source node
        and is subject to storage caps."""

        # aliased for clarity
        src_provider = provider_one

        settings_region_one['nid'] = 'fake-nid'
        dst_provider = OSFStorageProvider(auth, credentials, settings_region_one)

        src_provider.can_intra_move = mock.Mock(return_value=True)
        src_provider.intra_move = utils.MockCoroutine()
        src_provider.download = utils.MockCoroutine()

        src_path = WaterButlerPath('/foo', _ids=('Test', '56ab34'))
        dest_path = WaterButlerPath('/', _ids=('Test',))

        quota_url, quota_params = build_signed_url_without_auth(dst_provider, 'GET', 'quota_status')
        aiohttpretty.register_json_uri('GET', quota_url, params=quota_params, status=200,
                                       body={'over_quota': True})

        with pytest.raises(OsfStorageQuotaExceededError):
            await src_provider.move(dst_provider, src_path, dest_path, handle_naming=False);

        src_provider.can_intra_move.assert_called_once_with(dst_provider, src_path)
        src_provider.intra_move.assert_not_called()
        src_provider.download.assert_not_called()


class TestCrossRegionCopy:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_copy_file(self, provider_one, provider_two, file_stream, upload_response):

        # aliased for clarity
        src_provider, dst_provider = provider_one, provider_two

        src_provider.download = utils.MockCoroutine(return_value=file_stream)
        src_provider.intra_copy = utils.MockCoroutine(return_value=(upload_response, True))
        dst_provider._send_to_storage_provider = utils.MockCoroutine()

        src_path = WaterButlerPath('/foo', _ids=('Test', '56ab34'))
        dest_path = WaterButlerPath('/', _ids=('Test',))

        quota_url, quota_params = build_signed_url_without_auth(dst_provider, 'GET', 'quota_status')
        aiohttpretty.register_json_uri('GET', quota_url, params=quota_params, status=200,
                                       body={'over_quota': False})

        metadata, created = await src_provider.copy(dst_provider, src_path, dest_path,
                                                    handle_naming=False);

        assert metadata is not None
        assert created is True

        src_provider.download.assert_called_once_with(WaterButlerPath('/foo'))
        dst_provider._send_to_storage_provider.assert_called_once_with(file_stream,
                                                                       WaterButlerPath('/'),
                                                                       rename=None,
                                                                       conflict='replace')
        src_provider.intra_copy.assert_called_once_with(dst_provider, WaterButlerPath('/foo'),
                                                        WaterButlerPath('/'))

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_copy_file_reject_quota(self, provider_one, provider_two):

        # aliased for clarity
        src_provider, dst_provider = provider_one, provider_two

        src_provider.download = utils.MockCoroutine()
        src_provider.intra_copy = utils.MockCoroutine()
        dst_provider._send_to_storage_provider = utils.MockCoroutine()

        src_path = WaterButlerPath('/foo', _ids=('Test', '56ab34'))
        dest_path = WaterButlerPath('/', _ids=('Test',))

        quota_url, quota_params = build_signed_url_without_auth(dst_provider, 'GET', 'quota_status')
        aiohttpretty.register_json_uri('GET', quota_url, params=quota_params, status=200,
                                       body={'over_quota': True})

        with pytest.raises(OsfStorageQuotaExceededError):
            await src_provider.copy(dst_provider, src_path, dest_path, handle_naming=False)

        src_provider.download.assert_not_called()
        dst_provider._send_to_storage_provider.assert_not_called()
        src_provider.intra_copy.assert_not_called()

    @pytest.mark.asyncio
    async def test_copy_folder(self, provider_one, provider_two):

        # aliased for clarity
        src_provider, dst_provider = provider_one, provider_two

        src_provider._folder_file_op = utils.MockCoroutine(return_value=(upload_response, True))

        src_path = WaterButlerPath('/foo/', _ids=('Test', '56ab34'), folder=True)
        dest_path = WaterButlerPath('/', _ids=('Test',), folder=True)

        metadata, created = await src_provider.copy(dst_provider, src_path, dest_path,
                                                    handle_naming=False);

        assert metadata is not None
        assert created is True

        src_provider._folder_file_op.assert_called_once_with(src_provider.copy,
                                                             dst_provider,
                                                             WaterButlerPath('/foo/'),
                                                             WaterButlerPath('/'),
                                                             rename=None,
                                                             conflict='replace')

    @pytest.mark.asyncio
    async def test_copy_cross_provider(self, monkeypatch, provider_one, provider_two):

        # aliased for clarity
        src_provider, dst_provider = provider_one, provider_two

        src_provider.download = utils.MockCoroutine()
        dst_provider.NAME = 'not-osfstorage'

        core_copy = utils.MockCoroutine()
        monkeypatch.setattr('waterbutler.core.provider.BaseProvider.copy', core_copy)

        src_path = WaterButlerPath('/foo', _ids=('Test', '56ab34'))
        dest_path = WaterButlerPath('/', _ids=('Test',))

        await src_provider.copy(dst_provider, src_path, dest_path, handle_naming=False);

        core_copy.assert_called_once_with(dst_provider, src_path, dest_path, rename=None,
                                          conflict='replace', handle_naming=False);
        src_provider.download.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_copy_but_intra_copy(self, provider_one, auth, credentials,
                                       settings_region_one):
        """OSFStorageProvider.copy checks to see if intra_copy can be called as an optimization.
        If the destination is not `osfstorage`, delegate to the parent method.  Otherwise, check
        whether we can optimize by doing an `intra_copy` action.  `intra_copy` is permissable when
        both `osfstorage` providers are in the same region."""

        # aliased for clarity
        src_provider = provider_one

        settings_region_one['nid'] = 'fake-nid'
        dst_provider = OSFStorageProvider(auth, credentials, settings_region_one)

        src_provider.can_intra_copy = mock.Mock(return_value=True)
        src_provider.intra_copy = utils.MockCoroutine()
        src_provider.download = utils.MockCoroutine()

        src_path = WaterButlerPath('/foo', _ids=('Test', '56ab34'))
        dest_path = WaterButlerPath('/', _ids=('Test',))

        quota_url, quota_params = build_signed_url_without_auth(dst_provider, 'GET', 'quota_status')
        aiohttpretty.register_json_uri('GET', quota_url, params=quota_params, status=200,
                                       body={'over_quota': False})

        await src_provider.copy(dst_provider, src_path, dest_path, handle_naming=False);

        src_provider.can_intra_copy.assert_called_once_with(dst_provider, src_path)
        src_provider.intra_copy.assert_called_once_with(dst_provider, src_path, dest_path)
        src_provider.download.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_copy_reject_by_quota(self, provider_one, auth, credentials,
                                              settings_region_one):
        """Same as previous, but assume the destination node is not the same as the source node
        and is subject to storage caps."""

        # aliased for clarity
        src_provider = provider_one

        settings_region_one['nid'] = 'fake-nid'
        dst_provider = OSFStorageProvider(auth, credentials, settings_region_one)

        src_provider.can_intra_copy = mock.Mock(return_value=True)
        src_provider.intra_copy = utils.MockCoroutine()
        src_provider.download = utils.MockCoroutine()

        src_path = WaterButlerPath('/foo', _ids=('Test', '56ab34'))
        dest_path = WaterButlerPath('/', _ids=('Test',))

        quota_url, quota_params = build_signed_url_without_auth(dst_provider, 'GET', 'quota_status')
        aiohttpretty.register_json_uri('GET', quota_url, params=quota_params, status=200,
                                       body={'over_quota': True})

        with pytest.raises(OsfStorageQuotaExceededError):
            await src_provider.copy(dst_provider, src_path, dest_path, handle_naming=False);

        src_provider.can_intra_copy.assert_called_once_with(dst_provider, src_path)
        src_provider.intra_copy.assert_not_called()
        src_provider.download.assert_not_called()
