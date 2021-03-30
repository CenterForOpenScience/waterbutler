import pytest
import aiohttpretty

from http import HTTPStatus

from waterbutler.core import exceptions

from waterbutler.providers.onedrive.provider import OneDrivePath
from waterbutler.providers.onedrive.metadata import OneDriveFileMetadata
from waterbutler.providers.onedrive.metadata import OneDriveFolderMetadata

from tests.providers.onedrive.fixtures import (auth,
                                               settings,
                                               root_settings,
                                               subfolder_settings,
                                               provider,
                                               root_provider,
                                               subfolder_provider,
                                               file_like,
                                               empty_file_like,
                                               credentials,
                                               file_stream,
                                               file_content,
                                               empty_file_stream,
                                               empty_file_content,
                                               other_provider,
                                               # error_fixtures,
                                               other_credentials,
                                               download_fixtures,
                                               revision_fixtures,
                                               root_provider_fixtures,
                                               subfolder_provider_fixtures)


class TestRootProviderValidatePath:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_root(self, root_provider):
        try:
            wb_path_v1 = await root_provider.validate_v1_path('/')
        except Exception as exc:
            pytest.fail(str(exc))

        wb_path_v0 = await root_provider.validate_path('/')

        assert wb_path_v1 == wb_path_v0
        assert wb_path_v1.identifier == 'root'

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_validate_v1_path_file(self, root_provider, root_provider_fixtures):
        file_id = root_provider_fixtures['file_id']
        file_metadata = root_provider_fixtures['file_metadata']

        item_url = root_provider._build_item_url(file_id)
        aiohttpretty.register_json_uri('GET', item_url, body=file_metadata, status=200)

        file_path = '/{}'.format(file_id)
        try:
            wb_path_v1 = await root_provider.validate_v1_path(file_path)
        except Exception as exc:
            pytest.fail(str(exc))

        file_name = '/{}'.format(file_metadata['name'])
        assert str(wb_path_v1) == file_name
        assert wb_path_v1.identifier == file_id

        wb_path_v0 = await root_provider.validate_path(file_path)
        assert str(wb_path_v0) == file_name

        assert wb_path_v1 == wb_path_v0

        with pytest.raises(exceptions.NotFoundError) as exc:
            await root_provider.validate_v1_path(file_path + '/')

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_validate_v1_path_folder(self, root_provider, root_provider_fixtures):
        folder_id = root_provider_fixtures['folder_id']
        folder_metadata = root_provider_fixtures['folder_metadata']

        item_url = root_provider._build_item_url(folder_id)
        aiohttpretty.register_json_uri('GET', item_url, body=folder_metadata, status=200)

        folder_path = '/{}/'.format(folder_id)
        folder_name = '/{}/'.format(folder_metadata['name'])
        try:
            wb_path_v1 = await root_provider.validate_v1_path(folder_path)
        except Exception as exc:
            pytest.fail(str(exc))

        assert str(wb_path_v1) == folder_name
        assert wb_path_v1.identifier == folder_id

        wb_path_v0 = await root_provider.validate_path(folder_path)
        assert str(wb_path_v0) == folder_name

        assert wb_path_v1 == wb_path_v0

        with pytest.raises(exceptions.NotFoundError) as exc:
            await root_provider.validate_v1_path(folder_path.rstrip('/'))


class TestSubfolderProviderValidatePath:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_root(self, subfolder_provider, subfolder_provider_fixtures):
        try:
            wb_path_v1 = await subfolder_provider.validate_v1_path('/')
        except Exception as exc:
            pytest.fail(str(exc))

        wb_path_v0 = await subfolder_provider.validate_path('/')

        assert wb_path_v1 == wb_path_v0
        assert wb_path_v1.identifier == subfolder_provider_fixtures['root_id']

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_validate_v1_path_folder(self, subfolder_provider, subfolder_provider_fixtures):
        folder_id = subfolder_provider_fixtures['folder_id']
        folder_metadata = subfolder_provider_fixtures['folder_metadata']

        item_url = subfolder_provider._build_item_url(folder_id)
        aiohttpretty.register_json_uri('GET', item_url, body=folder_metadata, status=200)

        folder_path = '/{}/'.format(folder_id)
        folder_name = '/{}/'.format(folder_metadata['name'])
        try:
            wb_path_v1 = await subfolder_provider.validate_v1_path(folder_path)
        except Exception as exc:
            pytest.fail(str(exc))

        assert str(wb_path_v1) == folder_name
        assert wb_path_v1.identifier == folder_id

        wb_path_v0 = await subfolder_provider.validate_path(folder_path)
        assert str(wb_path_v0) == folder_name

        assert wb_path_v1 == wb_path_v0

        with pytest.raises(exceptions.NotFoundError) as exc:
            await subfolder_provider.validate_v1_path(folder_path.rstrip('/'))

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_validate_v1_path_file_is_child(self, subfolder_provider,
                                                  subfolder_provider_fixtures):
        """file is immediate child of provider base folder"""
        file_id = subfolder_provider_fixtures['file_id']
        file_metadata = subfolder_provider_fixtures['file_metadata']

        item_url = subfolder_provider._build_item_url(file_id)
        aiohttpretty.register_json_uri('GET', item_url, body=file_metadata, status=200)

        file_path = '/{}'.format(file_id)
        file_name = '/{}'.format(file_metadata['name'])
        try:
            wb_path_v1 = await subfolder_provider.validate_v1_path(file_path)
        except Exception as exc:
            pytest.fail(str(exc))

        assert str(wb_path_v1) == file_name
        assert wb_path_v1.identifier == file_id

        wb_path_v0 = await subfolder_provider.validate_path(file_path)
        assert str(wb_path_v0) == file_name

        assert wb_path_v1 == wb_path_v0

        with pytest.raises(exceptions.NotFoundError) as exc:
            await subfolder_provider.validate_v1_path(file_path + '/')

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_validate_v1_path_file_is_grandchild(self, subfolder_provider,
                                                       subfolder_provider_fixtures):
        """file is *not* immediate child of provider base folder"""
        subfile_id = subfolder_provider_fixtures['subfile_id']
        subfile_metadata = subfolder_provider_fixtures['subfile_metadata']

        item_url = subfolder_provider._build_item_url(subfile_id)
        aiohttpretty.register_json_uri('GET', item_url, body=subfile_metadata, status=200)

        root_url = subfolder_provider._build_item_url(subfolder_provider_fixtures['root_id'])
        aiohttpretty.register_json_uri('GET', root_url,
                                       body=subfolder_provider_fixtures['root_metadata'],
                                       status=200)

        subfile_path = '/{}'.format(subfile_id)
        subfile_name = '/{}/{}'.format(subfolder_provider_fixtures['folder_metadata']['name'],
                                       subfile_metadata['name'])
        try:
            wb_path_v1 = await subfolder_provider.validate_v1_path(subfile_path)
        except Exception as exc:
            pytest.fail(str(exc))
        assert str(wb_path_v1) == subfile_name

        wb_path_v0 = await subfolder_provider.validate_path(subfile_path)
        assert str(wb_path_v0) == subfile_name

        assert wb_path_v1 == wb_path_v0

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_validate_v1_path_file_is_outside_root(self, subfolder_provider,
                                                         subfolder_provider_fixtures):
        """file is outside of the base storage root"""
        file_id = subfolder_provider_fixtures['outside_file_id']
        file_metadata = subfolder_provider_fixtures['outside_file_metadata']

        item_url = subfolder_provider._build_item_url(file_id)
        aiohttpretty.register_json_uri('GET', item_url, body=file_metadata, status=200)

        root_url = subfolder_provider._build_item_url(subfolder_provider_fixtures['root_id'])
        aiohttpretty.register_json_uri('GET', root_url,
                                       body=subfolder_provider_fixtures['root_metadata'],
                                       status=200)

        file_path = '/{}'.format(file_id)
        with pytest.raises(exceptions.NotFoundError):
            await subfolder_provider.validate_v1_path(file_path)

        with pytest.raises(exceptions.NotFoundError):
            await subfolder_provider.validate_path(file_path)


class TestRevalidatePath:

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_revalidate_path_file(self, root_provider, root_provider_fixtures):
        file_name = 'toes.txt'
        file_id = root_provider_fixtures['file_id']
        root_id = 'root'

        parent_path = OneDrivePath('/', _ids=[root_id])
        expected_path = OneDrivePath('/{}'.format(file_name), _ids=[root_id, file_id])

        parent_url = root_provider._build_drive_url(*parent_path.api_identifier, expand='children')
        aiohttpretty.register_json_uri('GET', parent_url,
                                       body=root_provider_fixtures['root_metadata'], status=200)

        actual_path = await root_provider.revalidate_path(parent_path, file_name, False)
        assert actual_path == expected_path

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_revalidate_path_folder(self, root_provider, root_provider_fixtures):
        folder_name = 'teeth'
        folder_id = root_provider_fixtures['folder_id']
        root_id = 'root'

        parent_path = OneDrivePath('/', _ids=[root_id])
        expected_path = OneDrivePath('/{}/'.format(folder_name), _ids=[root_id, folder_id])

        parent_url = root_provider._build_drive_url(*parent_path.api_identifier, expand='children')
        aiohttpretty.register_json_uri('GET', parent_url,
                                       body=root_provider_fixtures['root_metadata'], status=200)

        actual_path = await root_provider.revalidate_path(parent_path, folder_name, True)
        assert actual_path == expected_path

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_revalidate_path_subfile(self, root_provider, root_provider_fixtures):
        root_id = 'root'
        parent_id = root_provider_fixtures['folder_id']
        subfile_id = root_provider_fixtures['subfile_id']

        parent_name = 'teeth'
        subfile_name = 'bicuspid.txt'

        parent_path = OneDrivePath('/{}/'.format(parent_name), _ids=[root_id, parent_id])
        expected_path = OneDrivePath('/{}/{}'.format(parent_name, subfile_name),
                                     _ids=[root_id, parent_id, subfile_id])

        parent_url = root_provider._build_drive_url(*parent_path.api_identifier, expand='children')
        aiohttpretty.register_json_uri('GET', parent_url,
                                       body=root_provider_fixtures['folder_metadata'], status=200)

        actual_path = await root_provider.revalidate_path(parent_path, subfile_name, False)
        assert actual_path == expected_path


class TestMetadata:

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_metadata_root(self, subfolder_provider, subfolder_provider_fixtures):

        path = OneDrivePath('/', _ids=(subfolder_provider_fixtures['root_id'], ))

        list_url = subfolder_provider._build_drive_url(*path.api_identifier, **{'$expand': 'children'})
        aiohttpretty.register_json_uri('GET', list_url, body=subfolder_provider_fixtures['root_metadata'])

        result = await subfolder_provider.metadata(path)
        assert len(result) == 2

        folder_metadata = result[0]
        assert folder_metadata.kind == 'folder'
        assert folder_metadata.name == 'crushers'
        assert folder_metadata.materialized_path == '/crushers/'

        file_metadata = result[1]
        assert file_metadata.kind == 'file'
        assert file_metadata.name == 'bicuspid.txt'
        assert file_metadata.materialized_path == '/bicuspid.txt'

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_metadata_folder(self, subfolder_provider, subfolder_provider_fixtures):
        folder_id = subfolder_provider_fixtures['folder_id']
        folder_metadata = subfolder_provider_fixtures['folder_metadata']
        folder_name = folder_metadata['name']
        path = OneDrivePath('/{}/'.format(folder_name),
                            _ids=(subfolder_provider_fixtures['root_id'], folder_id, ))

        list_url = subfolder_provider._build_drive_url(*path.api_identifier, **{'$expand': 'children'})
        aiohttpretty.register_json_uri('GET', list_url, body=folder_metadata)

        result = await subfolder_provider.metadata(path)
        assert len(result) == 1

        file_metadata = result[0]
        assert file_metadata.kind == 'file'
        assert file_metadata.name == 'molars.txt'
        assert file_metadata.materialized_path == '/crushers/molars.txt'

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_metadata_file(self, subfolder_provider, subfolder_provider_fixtures):
        file_id = subfolder_provider_fixtures['file_id']
        file_metadata = subfolder_provider_fixtures['file_metadata']
        file_name = file_metadata['name']
        path = OneDrivePath('/{}'.format(file_name),
                            _ids=(subfolder_provider_fixtures['root_id'], file_id, ))

        list_url = subfolder_provider._build_drive_url(*path.api_identifier, expand='children')
        aiohttpretty.register_json_uri('GET', list_url, body=file_metadata)

        result = await subfolder_provider.metadata(path)
        assert result.kind == 'file'
        assert result.name == 'bicuspid.txt'
        assert result.materialized_path == '/bicuspid.txt'

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_metadata_folder_with_more_metadata(self, subfolder_provider, subfolder_provider_fixtures):
        folder_id = subfolder_provider_fixtures['folder_id']
        folder_metadata1 = subfolder_provider_fixtures['folder_with_more_metadata1']
        folder_metadata2 = subfolder_provider_fixtures['folder_with_more_metadata2']
        folder_metadata3 = subfolder_provider_fixtures['folder_with_more_metadata3']
        folder_name = folder_metadata1['name']
        path = OneDrivePath('/{}/'.format(folder_name),
                            _ids=(subfolder_provider_fixtures['root_id'], folder_id, ))

        list_url = subfolder_provider._build_drive_url(*path.api_identifier, **{'$expand': 'children'})
        list_more1_url = folder_metadata1['children@odata.nextLink']
        list_more2_url = folder_metadata2['@odata.nextLink']
        aiohttpretty.register_json_uri('GET', list_url, body=folder_metadata1)
        aiohttpretty.register_json_uri('GET', list_more1_url, body=folder_metadata2)
        aiohttpretty.register_json_uri('GET', list_more2_url, body=folder_metadata3)
        print('Register {}'.format(list_more1_url))
        print('Register {}'.format(list_more2_url))

        result = await subfolder_provider.metadata(path)
        assert len(result) == 3

        assert result[0].kind == 'file'
        assert result[0].name == 'molars1.txt'
        assert result[0].materialized_path == '/crushers/molars1.txt'
        assert result[1].kind == 'file'
        assert result[1].name == 'molars2.txt'
        assert result[1].materialized_path == '/crushers/molars2.txt'
        assert result[2].kind == 'file'
        assert result[2].name == 'molars3.txt'
        assert result[2].materialized_path == '/crushers/molars3.txt'


class TestRevisions:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_get_revisions(self, provider, revision_fixtures):
        file_id = revision_fixtures['file_id']
        path = OneDrivePath('/bicuspids.txt', _ids=[revision_fixtures['root_id'], file_id])

        revision_response = revision_fixtures['file_revisions']
        revisions_url = provider._build_drive_url(*path.api_identifier, 'versions')
        aiohttpretty.register_json_uri('GET', revisions_url, body=revision_response)

        result = await provider.revisions(path)

        assert len(result) == 5



class TestCRUD:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload(self, root_provider, root_provider_fixtures, file_stream):

        file_metadata = root_provider_fixtures['file_metadata']
        file_name = file_metadata['name']
        file_path = OneDrivePath('/{}'.format(file_name),
                                 _ids=(root_provider_fixtures['root_id'], ))

        upload_url = 'https://osf.dummy.com'
        create_session_url = '{}:/{}:/createUploadSession'.format(
            root_provider._build_drive_url(*file_path.parent.api_identifier),
            file_path.name,
        )
        aiohttpretty.register_json_uri('POST', create_session_url, status=HTTPStatus.OK, body={
            'uploadUrl': upload_url
        })
        aiohttpretty.register_json_uri('PUT', upload_url, status=HTTPStatus.CREATED, body=file_metadata)

        result, created = await root_provider.upload(file_stream, file_path)
        expected = OneDriveFileMetadata(file_metadata, file_path, root_provider.NAME)

        assert created is True
        assert result == expected
        assert aiohttpretty.has_call(method='POST', uri=create_session_url)
        assert aiohttpretty.has_call(method='PUT', uri=upload_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_empty_file(self, root_provider, root_provider_fixtures, empty_file_stream):

        file_metadata = root_provider_fixtures['file_metadata']
        file_name = file_metadata['name']
        file_path = OneDrivePath('/{}'.format(file_name),
                                 _ids=(root_provider_fixtures['root_id'], ))

        url = '{}:/{}:/content'.format(
            root_provider._build_drive_url(*file_path.parent.api_identifier),
            file_path.name,
        )
        aiohttpretty.register_json_uri('PUT', url, status=HTTPStatus.CREATED, body=file_metadata)

        result, created = await root_provider.upload(empty_file_stream, file_path)
        expected = OneDriveFileMetadata(file_metadata, file_path, root_provider.NAME)

        assert created is True
        assert result == expected
        assert aiohttpretty.has_call(method='PUT', uri=url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_create_folder(self, root_provider, root_provider_fixtures):

        folder_id = root_provider_fixtures['folder_id']
        folder_metadata = root_provider_fixtures['folder_metadata']
        folder_name = folder_metadata['name']
        path = OneDrivePath('/{}/'.format(folder_name),
                            _ids=(root_provider_fixtures['root_id'], ))
        url = root_provider._build_drive_url(*path.parent.api_identifier, 'children')
        aiohttpretty.register_json_uri('POST', url, status=HTTPStatus.CREATED, body=folder_metadata)

        result = await root_provider.create_folder(path)

        assert result.kind == 'folder'
        assert result.name == folder_name
        assert result.path == '/{}/'.format(folder_id)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete(self, root_provider, root_provider_fixtures):

        file_id = root_provider_fixtures['file_id']
        file_metadata = root_provider_fixtures['file_metadata']
        file_name = file_metadata['name']
        file_path = OneDrivePath('/{}'.format(file_name),
                                 _ids=(root_provider_fixtures['root_id'], file_id, ))

        url = root_provider._build_drive_url(*file_path.api_identifier)
        aiohttpretty.register_json_uri('DELETE', url, status=HTTPStatus.NO_CONTENT)

        await root_provider.delete(file_path)

        assert aiohttpretty.has_call(method='DELETE', uri=url)


class TestDownload:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_standard_file(self, provider, download_fixtures):
        file_id = download_fixtures['file_id']
        path = OneDrivePath('/toes.txt', _ids=[download_fixtures['root_id'], file_id])

        metadata_response = download_fixtures['file_metadata']
        metadata_url = provider._build_drive_url('items', file_id, **{'$expand': 'children'})
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_response)

        aiohttpretty.register_uri('GET', download_fixtures['file_download_url'],
                                  body=download_fixtures['file_content'],
                                  headers={'Content-Length': '11'})

        response = await provider.download(path)
        content = await response.read()
        assert content == b'ten of them'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_standard_file_range(self, provider, download_fixtures):
        file_id = download_fixtures['file_id']
        path = OneDrivePath('/toes.txt', _ids=[download_fixtures['root_id'], file_id])

        metadata_response = download_fixtures['file_metadata']
        metadata_url = provider._build_drive_url('items', file_id, **{'$expand': 'children'})
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_response)

        download_url = download_fixtures['file_download_url']
        aiohttpretty.register_uri('GET', download_url, status=206,
                                  body=download_fixtures['file_content'][0:2])

        response = await provider.download(path, range=(0, 1))
        assert response.partial
        content = await response.read()
        assert content == b'te'
        assert aiohttpretty.has_call(method='GET', uri=download_url,
                                     headers={'Range': 'bytes=0-1',
                                              'Authorization': 'bearer wrote harry potter',
                                              'accept-encoding': ''})

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_by_revision(self, provider, download_fixtures, revision_fixtures):
        file_id = download_fixtures['file_id']
        path = OneDrivePath('/toes.txt', _ids=[download_fixtures['root_id'], file_id])

        revision_response = revision_fixtures['file_revisions']
        revisions_url = provider._build_drive_url(*path.api_identifier, 'versions')
        aiohttpretty.register_json_uri('GET', revisions_url, body=revision_response)

        aiohttpretty.register_uri('GET', revision_fixtures['file_revision_download_url'],
                                  body=download_fixtures['file_content'],
                                  headers={'Content-Length': '11'})

        response = await provider.download(path, revision=revision_fixtures['revision_id'])
        content = await response.read()
        assert content == b'ten of them'

    @pytest.mark.asyncio
    async def test_download_no_such_file(self, provider):
        od_path = OneDrivePath('/does-not-exists', _ids=[None, None])
        with pytest.raises(exceptions.DownloadError):
            await provider.download(od_path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_by_bad_revision(self, provider, download_fixtures):
        file_id = download_fixtures['file_id']
        path = OneDrivePath('/toes.txt', _ids=[download_fixtures['root_id'], file_id])

        revision_response = download_fixtures['file_revisions']
        revisions_url = provider._build_drive_url(*path.api_identifier, 'versions')
        aiohttpretty.register_json_uri('GET', revisions_url, body=revision_response)

        with pytest.raises(exceptions.NotFoundError):
            await provider.download(path, revision='thisisafakerevision')

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_unexportable_file(self, provider, download_fixtures):
        onenote_id = download_fixtures['onenote_id']
        path = OneDrivePath('/onenote', _ids=[download_fixtures['root_id'], onenote_id])

        metadata_response = download_fixtures['onenote_metadata']
        metadata_url = provider._build_drive_url('items', onenote_id, **{'$expand': 'children'})
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_response)

        with pytest.raises(exceptions.UnexportableFileTypeError):
            await provider.download(path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_unexportable_by_revision(self, provider, download_fixtures):
        onenote_id = download_fixtures['onenote_id']
        path = OneDrivePath('/onenote', _ids=[download_fixtures['root_id'], onenote_id])

        revision_response = download_fixtures['onenote_revisions']
        revisions_url = provider._build_drive_url('items', onenote_id, 'versions')
        aiohttpretty.register_json_uri('GET', revisions_url, body=revision_response)

        with pytest.raises(exceptions.UnexportableFileTypeError):
            await provider.download(path,
                                    revision=download_fixtures['onenote_revision_non_exportable'])


class TestIntraMoveCopy:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_move_file(self, root_provider, root_provider_fixtures):
        file_id = root_provider_fixtures['file_id']
        file_metadata = root_provider_fixtures['file_metadata']
        file_name = file_metadata['name']
        folder_id = root_provider_fixtures['folder_id']

        src_path = OneDrivePath('/{}'.format(file_name),
                                _ids=(root_provider_fixtures['root_id'], file_id, ))
        dest_path = OneDrivePath('/{}/{}'.format(folder_id, file_name),
                                 _ids=(root_provider_fixtures['root_id'], folder_id, ))

        url = root_provider._build_drive_url(*src_path.api_identifier)
        data = {
            'parentReference': {
                'id': dest_path.parent.identifier
            },
            'name': dest_path.name,
            '@microsoft.graph.conflictBehavior': 'replace',
        }
        aiohttpretty.register_json_uri('PATCH', url, data=data, body=file_metadata, status=HTTPStatus.OK)

        result = await root_provider.intra_move(root_provider, src_path, dest_path)
        expected = OneDriveFileMetadata(file_metadata, dest_path, root_provider.NAME)

        assert result == (expected, True)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_copy_file(self, root_provider, root_provider_fixtures):
        file_id = root_provider_fixtures['file_id']
        file_metadata = root_provider_fixtures['file_metadata']
        file_name = file_metadata['name']
        folder_id = root_provider_fixtures['folder_id']

        src_path = OneDrivePath('/{}'.format(file_name),
                                _ids=(root_provider_fixtures['root_id'], file_id, ))
        dest_path = OneDrivePath('/{}/{}'.format(folder_id, file_name),
                                 _ids=(root_provider_fixtures['root_id'], folder_id, ))

        copy_url = root_provider._build_drive_url(*src_path.api_identifier, 'copy')
        copy_data = {
            'parentReference': {
                'id': dest_path.parent.identifier,
            },
            'name': dest_path.name,
            '@microsoft.graph.conflictBehavior': 'replace',
        }
        action_url = 'https://dummy.osf.com'
        metadata_url = root_provider._build_item_url(file_id)
        aiohttpretty.register_json_uri(
            'POST',
            copy_url,
            data=copy_data,
            body=file_metadata,
            headers={
                'Location': action_url
            },
            status=HTTPStatus.ACCEPTED
        )
        aiohttpretty.register_json_uri('GET', action_url, status=HTTPStatus.OK,
                                       body={'resourceId': file_id, 'status': 'completed'})
        aiohttpretty.register_json_uri('GET', metadata_url, status=HTTPStatus.OK, body=file_metadata)

        result = await root_provider.intra_copy(root_provider, src_path, dest_path)
        expected = OneDriveFileMetadata(file_metadata, dest_path, root_provider.NAME)

        assert result == (expected, True)
        assert aiohttpretty.has_call(method='POST', uri=copy_url)
        assert aiohttpretty.has_call(method='GET', uri=action_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_move_folder(self, root_provider, root_provider_fixtures):
        folder_id = root_provider_fixtures['folder_id']
        folder_metadata = root_provider_fixtures['folder_metadata']
        folder_name = folder_metadata['name']
        folder_id2 = root_provider_fixtures['folder_id'] + '0'

        src_path = OneDrivePath('/{}/'.format(folder_name),
                                _ids=(root_provider_fixtures['root_id'], folder_id, ))
        dest_path = OneDrivePath('/{}/{}/'.format(folder_id2, folder_name),
                                 _ids=(root_provider_fixtures['root_id'], folder_id2, ))
        moved_path = OneDrivePath('/{}/{}/'.format(folder_id2, folder_name),
                                  _ids=(root_provider_fixtures['root_id'], folder_id2, folder_id))

        move_url = root_provider._build_drive_url(*src_path.api_identifier)
        metadata_url = root_provider._build_drive_url(*moved_path.api_identifier, **{'$expand': 'children'})
        data = {
            'parentReference': {
                'id': dest_path.parent.identifier
            },
            'name': dest_path.name,
            '@microsoft.graph.conflictBehavior': 'replace',
        }
        aiohttpretty.register_json_uri('PATCH', move_url, data=data, body=folder_metadata, status=HTTPStatus.OK)
        aiohttpretty.register_json_uri('GET', metadata_url, body=folder_metadata, status=HTTPStatus.OK)

        result = await root_provider.intra_move(root_provider, src_path, dest_path)
        expected = OneDriveFolderMetadata(folder_metadata, moved_path, root_provider.NAME)
        expected._children = root_provider._construct_metadata(folder_metadata, moved_path)

        assert result == (expected, True)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_copy_folder(self, root_provider, root_provider_fixtures):
        folder_id = root_provider_fixtures['folder_id']
        folder_metadata = root_provider_fixtures['folder_metadata']
        folder_name = folder_metadata['name']
        folder_id2 = root_provider_fixtures['folder_id'] + '0'

        src_path = OneDrivePath('/{}/'.format(folder_name),
                                _ids=(root_provider_fixtures['root_id'], folder_id, ))
        dest_path = OneDrivePath('/{}/{}/'.format(folder_id2, folder_name),
                                 _ids=(root_provider_fixtures['root_id'], folder_id2, ))
        copied_path = OneDrivePath('/{}/{}/'.format(folder_id2, folder_name),
                                  _ids=(root_provider_fixtures['root_id'], folder_id2, folder_id))

        copy_url = root_provider._build_drive_url(*src_path.api_identifier, 'copy')
        copy_data = {
            'parentReference': {
                'id': dest_path.parent.identifier,
            },
            'name': dest_path.name,
            '@microsoft.graph.conflictBehavior': 'replace',
        }
        action_url = 'https://dummy.osf.com'
        metadata_url1 = root_provider._build_item_url(folder_id)
        metadata_url2 = root_provider._build_drive_url(*copied_path.api_identifier, **{'$expand': 'children'})
        aiohttpretty.register_json_uri(
            'POST',
            copy_url,
            data=copy_data,
            body=folder_metadata,
            headers={
                'Location': action_url
            },
            status=HTTPStatus.ACCEPTED
        )
        aiohttpretty.register_json_uri('GET', action_url, status=HTTPStatus.OK,
                                       body={'resourceId': folder_id, 'status': 'completed'})
        aiohttpretty.register_json_uri('GET', metadata_url1, status=HTTPStatus.OK, body=folder_metadata)
        aiohttpretty.register_json_uri('GET', metadata_url2, status=HTTPStatus.OK, body=folder_metadata)

        result = await root_provider.intra_copy(root_provider, src_path, dest_path)
        expected = OneDriveFolderMetadata(folder_metadata, copied_path, root_provider.NAME)
        expected._children = root_provider._construct_metadata(folder_metadata, copied_path)

        assert result == (expected, True)
        assert aiohttpretty.has_call(method='POST', uri=copy_url)
        assert aiohttpretty.has_call(method='GET', uri=action_url)


# leftover bits
class TestOperations:

    def test_can_intra_copy(self, provider):
        assert provider.can_intra_copy(provider)

    def test_can_intra_copy_other(self, provider, other_provider):
        assert provider.can_intra_copy(other_provider) is False

    def test_can_intra_move(self, provider):
        assert provider.can_intra_move(provider)

    def test_can_intra_move_other(self, provider, other_provider):
        assert provider.can_intra_move(other_provider) is False

    def test_can_duplicate_names(self, provider):
        assert provider.can_duplicate_names() is False
