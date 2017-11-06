import pytest

import io
from http import client

import aiohttpretty

from waterbutler.core import streams
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.box import BoxProvider
from waterbutler.providers.box.metadata import (BoxRevision,
                                                BoxFileMetadata,
                                                BoxFolderMetadata)

from tests.providers.box.fixtures import(intra_fixtures,
                                         revision_fixtures,
                                         root_provider_fixtures)


@pytest.fixture
def auth():
    return {
        'name': 'cat',
        'email': 'cat@cat.com',
    }


@pytest.fixture
def credentials():
    return {'token': 'wrote harry potter'}


@pytest.fixture
def other_credentials():
    return {'token': 'wrote lord of the rings'}


@pytest.fixture
def settings():
    return {'folder': '11446498'}


@pytest.fixture
def provider(auth, credentials, settings):
    return BoxProvider(auth, credentials, settings)


@pytest.fixture
def other_provider(auth, other_credentials, settings):
    return BoxProvider(auth, other_credentials, settings)


@pytest.fixture
def file_content():
    return b'SLEEP IS FOR THE WEAK GO SERVE STREAMS'


@pytest.fixture
def file_like(file_content):
    return io.BytesIO(file_content)


@pytest.fixture
def file_stream(file_like):
    return streams.FileStreamReader(file_like)


class TestValidatePath:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_file(self, provider, root_provider_fixtures):
        file_id = '5000948880'

        good_url = provider.build_url('files', file_id, fields='id,name,path_collection')
        bad_url = provider.build_url('folders', file_id, fields='id,name,path_collection')

        aiohttpretty.register_json_uri('get', good_url,
                                       body=root_provider_fixtures['file_metadata']['entries'][0],
                                       status=200)
        aiohttpretty.register_uri('get', bad_url, status=404)

        try:
            wb_path_v1 = await provider.validate_v1_path('/' + file_id)
        except Exception as exc:
            pytest.fail(str(exc))

        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider.validate_v1_path('/' + file_id + '/')

        assert exc.value.code == client.NOT_FOUND

        wb_path_v0 = await provider.validate_path('/' + file_id)

        assert wb_path_v1 == wb_path_v0

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_folder(self, provider, root_provider_fixtures):
        provider.folder = '0'
        folder_id = '11446498'

        good_url = provider.build_url('folders', folder_id, fields='id,name,path_collection')
        bad_url = provider.build_url('files', folder_id, fields='id,name,path_collection')

        aiohttpretty.register_json_uri('get', good_url,
                                       body=root_provider_fixtures['folder_object_metadata'],
                                       status=200)
        aiohttpretty.register_uri('get', bad_url, status=404)
        try:
            wb_path_v1 = await provider.validate_v1_path('/' + folder_id + '/')
        except Exception as exc:
            pytest.fail(str(exc))

        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider.validate_v1_path('/' + folder_id)

        assert exc.value.code == client.NOT_FOUND

        wb_path_v0 = await provider.validate_path('/' + folder_id + '/')

        assert wb_path_v1 == wb_path_v0

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_path_root(self, provider):
        path = await provider.validate_path('/')
        assert path.is_dir
        assert len(path.parts) == 1
        assert path.name == ''

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_root(self, provider):
        path = await provider.validate_v1_path('/')
        assert path.is_dir
        assert len(path.parts) == 1
        assert path.name == ''

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_bad_path(self, provider):

        with pytest.raises(exceptions.NotFoundError) as e:
            await provider.validate_v1_path('/bulbasaur')

        assert e.value.message == 'Could not retrieve file or directory /bulbasaur'
        assert e.value.code == 404

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_path_bad_path(self, provider):

        with pytest.raises(exceptions.MetadataError) as e:
            await provider.validate_path('/bulbasaur/charmander')

        assert e.value.message == 'Could not find /bulbasaur/charmander'
        assert e.value.code == 404

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_path(self, provider, root_provider_fixtures):
        provider.folder = '0'
        folder_id = '0'

        good_url = provider.build_url('folders', folder_id, 'items', fields='id,name,type', limit=1000)
        aiohttpretty.register_json_uri('GET', good_url,
                                       body=root_provider_fixtures['revalidate_metadata'],
                                       status=200)

        result = await provider.validate_path('/bulbasaur')
        assert result == WaterButlerPath('/bulbasaur', folder=False)


class TestDownload:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download(self, provider, root_provider_fixtures):
        item = root_provider_fixtures['file_metadata']['entries'][0]
        path = WaterButlerPath('/triangles.txt', _ids=(provider.folder, item['id']))

        metadata_url = provider.build_url('files', item['id'])
        content_url = provider.build_url('files', item['id'], 'content')

        aiohttpretty.register_json_uri('GET', metadata_url, body=item)
        aiohttpretty.register_uri('GET', content_url, body=b'better', auto_length=True)

        result = await provider.download(path)
        content = await result.read()

        assert content == b'better'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_revision(self, provider, root_provider_fixtures):
        revision = '21753842'
        item = root_provider_fixtures['file_metadata']['entries'][0]
        path = WaterButlerPath('/triangles.txt', _ids=(provider.folder, item['id']))

        metadata_url = provider.build_url('files', item['id'])
        content_url = provider.build_url('files', item['id'], 'content', **{'version': revision})

        aiohttpretty.register_json_uri('GET', metadata_url, body=item)
        aiohttpretty.register_uri('GET', content_url, body=b'better', auto_length=True)

        result = await provider.download(path, revision)
        content = await result.read()

        assert content == b'better'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_not_found(self, provider, root_provider_fixtures):
        item = root_provider_fixtures['file_metadata']['entries'][0]
        path = WaterButlerPath('/vectors.txt', _ids=(provider.folder, None))
        metadata_url = provider.build_url('files', item['id'])
        aiohttpretty.register_uri('GET', metadata_url, status=404)

        with pytest.raises(exceptions.DownloadError) as e:
            await provider.download(path)

        assert e.value.code == 404


class TestUpload:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_create(self, provider, root_provider_fixtures, file_stream):
        path = WaterButlerPath('/newfile', _ids=(provider.folder, None))
        upload_url = provider._build_upload_url('files', 'content')
        upload_metadata = root_provider_fixtures['upload_metadata']
        aiohttpretty.register_json_uri('POST', upload_url, status=201, body=upload_metadata)

        metadata, created = await provider.upload(file_stream, path)
        expected = BoxFileMetadata(upload_metadata['entries'][0], path).serialized()

        assert metadata.serialized() == expected
        assert created is True
        assert path.identifier_path == metadata.path
        assert aiohttpretty.has_call(method='POST', uri=upload_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_conflict_keep(self, provider, root_provider_fixtures, file_stream):
        upload_metadata = root_provider_fixtures['upload_metadata']
        item = upload_metadata['entries'][0]
        path = WaterButlerPath('/newfile', _ids=(provider.folder, item['id']))

        upload_url = provider._build_upload_url('files', 'content')
        aiohttpretty.register_json_uri('POST', upload_url, status=201, body=upload_metadata)

        metadata_url = provider.build_url('files', path.identifier)
        aiohttpretty.register_json_uri('GET', metadata_url, body=upload_metadata)

        list_url = provider.build_url('folders', item['path_collection']['entries'][1]['id'],
                                      'items', fields='id,name,type', limit=1000)
        aiohttpretty.register_json_uri('GET', list_url,
                                       body=root_provider_fixtures['folder_list_metadata'])

        metadata, created = await provider.upload(file_stream, path, conflict='keep')
        expected = BoxFileMetadata(item, path).serialized()

        # since the metadata for the renamed conflict file isn't actually saved, this one is odd to
        # test.
        assert metadata.serialized() == expected
        assert created is True
        assert path.identifier_path == metadata.path
        assert aiohttpretty.has_call(method='POST', uri=upload_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_update(self, provider, root_provider_fixtures, file_stream):
        upload_metadata = root_provider_fixtures['upload_metadata']
        item_to_overwrite = root_provider_fixtures['folder_list_metadata']['entries'][0]
        path = WaterButlerPath('/newfile', _ids=(provider.folder, item_to_overwrite['id']))
        upload_url = provider._build_upload_url('files', item_to_overwrite['id'], 'content')
        aiohttpretty.register_json_uri('POST', upload_url, status=201, body=upload_metadata)

        metadata, created = await provider.upload(file_stream, path)
        expected = BoxFileMetadata(upload_metadata['entries'][0], path).serialized()

        assert metadata.serialized() == expected
        assert created is False
        assert aiohttpretty.has_call(method='POST', uri=upload_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_checksum_mismatch(self, provider, root_provider_fixtures, file_stream):
        path = WaterButlerPath('/newfile', _ids=(provider.folder, None))
        upload_url = provider._build_upload_url('files', 'content')
        aiohttpretty.register_json_uri('POST', upload_url, status=201,
                                       body=root_provider_fixtures['checksum_mismatch_metadata'])

        with pytest.raises(exceptions.UploadChecksumMismatchError) as exc:
            await provider.upload(file_stream, path)

        assert aiohttpretty.has_call(method='POST', uri=upload_url)


class TestDelete:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_file(self, provider, root_provider_fixtures):
        item = root_provider_fixtures['file_metadata']['entries'][0]
        path = WaterButlerPath('/{}'.format(item['name']), _ids=(provider.folder, item['id']))
        url = provider.build_url('files', path.identifier)

        aiohttpretty.register_uri('DELETE', url, status=204)

        await provider.delete(path)

        assert aiohttpretty.has_call(method='DELETE', uri=url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_folder(self, provider, root_provider_fixtures):
        item = root_provider_fixtures['folder_object_metadata']
        path = WaterButlerPath('/{}/'.format(item['name']), _ids=(provider.folder, item['id']))
        url = provider.build_url('folders', path.identifier, recursive=True)

        aiohttpretty.register_uri('DELETE', url, status=204)

        await provider.delete(path)

        assert aiohttpretty.has_call(method='DELETE', uri=url)

    @pytest.mark.asyncio
    async def test_must_not_be_none(self, provider):
        path = WaterButlerPath('/Goats', _ids=(provider.folder, None))

        with pytest.raises(exceptions.NotFoundError) as e:
            await provider.delete(path)

        assert e.value.code == 404
        assert str(path) in e.value.message

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_root_no_confirm(self, provider, root_provider_fixtures):
        path = WaterButlerPath('/', _ids=('0'))

        with pytest.raises(exceptions.DeleteError) as e:
            await provider.delete(path)

        assert e.value.message == 'confirm_delete=1 is required for deleting root provider folder'
        assert e.value.code == 400

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_root(self, provider, root_provider_fixtures):
        item = root_provider_fixtures['file_metadata']['entries'][0]
        path = WaterButlerPath('/newfile', _ids=(provider.folder, item['id']))
        root_path = WaterButlerPath('/', _ids=('0'))

        url = provider.build_url('folders', root_path.identifier, 'items',
                                 fields='id,name,size,modified_at,etag,total_count',
                                 offset=(0), limit=1000)
        aiohttpretty.register_json_uri('GET', url,
                                       body=root_provider_fixtures['one_entry_folder_list_metadata'])

        url = provider.build_url('files', item['id'], fields='id,name,path_collection')
        delete_url = provider.build_url('files', path.identifier)
        aiohttpretty.register_json_uri('get', url,
                                       body=root_provider_fixtures['file_metadata']['entries'][0])
        aiohttpretty.register_json_uri('DELETE', delete_url, status=204)

        await provider.delete(root_path, 1)

        assert aiohttpretty.has_call(method='DELETE', uri=delete_url)


class TestMetadata:

    @pytest.mark.asyncio
    async def test_must_not_be_none(self, provider):
        path = WaterButlerPath('/Goats', _ids=(provider.folder, None))

        with pytest.raises(exceptions.NotFoundError) as e:
            await provider.metadata(path)

        assert e.value.code == 404
        assert str(path) in e.value.message

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_revision_metadata(self, provider, root_provider_fixtures, revision_fixtures):
        list_metadata = revision_fixtures['revisions_list_metadata']
        item = list_metadata['entries'][0]

        path = WaterButlerPath('/goats', _ids=(provider.folder, item['id']))
        url = provider.build_url('files', path.identifier, 'versions')

        aiohttpretty.register_json_uri('GET', url, body=list_metadata)

        result = await provider.metadata(path, revision=item['id'])
        expected = BoxFileMetadata(item, path)

        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_revision_metadata_error(self, provider, root_provider_fixtures,
                                           revision_fixtures):
        list_metadata = revision_fixtures['revisions_list_metadata']
        item = list_metadata['entries'][0]

        path = WaterButlerPath('/goats', _ids=(provider.folder, item['id']))
        url = provider.build_url('files', path.identifier, 'versions')

        aiohttpretty.register_json_uri('GET', url, body=list_metadata)

        with pytest.raises(exceptions.NotFoundError) as e:
            await provider.metadata(path, revision='this is a bad revision id')

        assert e.value.code == 404
        assert str(path) in e.value.message

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_bad_response(self, provider, root_provider_fixtures):
        item = root_provider_fixtures['file_metadata']['entries'][0]

        path = WaterButlerPath('/goats', _ids=(provider.folder, item['id']))
        url = provider.build_url('files', path.identifier)

        aiohttpretty.register_json_uri('GET', url, body=None)

        with pytest.raises(exceptions.NotFoundError) as e:
            await provider.metadata(path)

        assert e.value.code == 404
        assert str(path) in e.value.message

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_folder_metadata(self, provider, root_provider_fixtures):
        item = root_provider_fixtures['folder_object_metadata']
        path = WaterButlerPath('/goats/', _ids=(provider.folder, item['id']))
        url = provider.build_url('folders', path.identifier)

        aiohttpretty.register_json_uri('GET', url, body=item)

        result = await provider.metadata(path, raw=True, folder=True)

        assert result == item

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata(self, provider, root_provider_fixtures):
        path = WaterButlerPath('/', _ids=(provider.folder, ))

        list_url = provider.build_url('folders', provider.folder, 'items',
                                      fields='id,name,size,modified_at,etag,total_count',
                                      offset=0, limit=1000)

        list_metadata = root_provider_fixtures['folder_list_metadata']
        aiohttpretty.register_json_uri('GET', list_url, body=list_metadata)

        result = await provider.metadata(path)

        expected = []

        for x in list_metadata['entries']:
            if x['type'] == 'file':
                expected.append(BoxFileMetadata(x, path.child(x['name'])))
            else:
                expected.append(BoxFolderMetadata(x, path.child(x['name'], folder=True)))

        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_raw(self, provider, root_provider_fixtures):
        item = root_provider_fixtures['folder_list_metadata']
        path = WaterButlerPath('/', _ids=(provider.folder, ))

        list_url = provider.build_url('folders', provider.folder, 'items',
                                      fields='id,name,size,modified_at,etag,total_count',
                                      offset=0, limit=1000)

        aiohttpretty.register_json_uri('GET', list_url, body=item)

        result = await provider.metadata(path, raw=True)

        assert result == item

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_nested(self, provider, root_provider_fixtures):
        item = root_provider_fixtures['file_metadata']['entries'][0]
        path = WaterButlerPath('/name.txt', _ids=(provider, item['id']))

        file_url = provider.build_url('files', path.identifier)
        aiohttpretty.register_json_uri('GET', file_url, body=item)

        result = await provider.metadata(path)

        expected = BoxFileMetadata(item, path)
        assert result == expected
        assert aiohttpretty.has_call(method='GET', uri=file_url)
        assert result.extra == {
            'etag': '3',
            'hashes': {
                'sha1': '134b65991ed521fcfe4724b7d814ab8ded5185dc',
            },
        }

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_missing(self, provider):
        path = WaterButlerPath('/Something', _ids=(provider.folder, None))

        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider.metadata(path)

        assert exc.value.code == client.NOT_FOUND


class TestRevisions:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_get_revisions(self, provider, root_provider_fixtures, revision_fixtures):
        item = root_provider_fixtures['file_metadata']['entries'][0]
        revisions_list = revision_fixtures['revisions_list_metadata']

        path = WaterButlerPath('/name.txt', _ids=(provider, item['id']))

        file_url = provider.build_url('files', path.identifier)
        revisions_url = provider.build_url('files', path.identifier, 'versions')

        aiohttpretty.register_json_uri('GET', file_url, body=item)
        aiohttpretty.register_json_uri('GET', revisions_url, body=revisions_list)

        result = await provider.revisions(path)

        expected = [
            BoxRevision(each)
            for each in [item] + revisions_list['entries']
        ]

        assert result == expected
        assert aiohttpretty.has_call(method='GET', uri=file_url)
        assert aiohttpretty.has_call(method='GET', uri=revisions_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_get_revisions_free_account(self, provider, root_provider_fixtures):
        item = root_provider_fixtures['file_metadata']['entries'][0]
        path = WaterButlerPath('/name.txt', _ids=(provider, item['id']))

        file_url = provider.build_url('files', path.identifier)
        revisions_url = provider.build_url('files', path.identifier, 'versions')

        aiohttpretty.register_json_uri('GET', file_url, body=item)
        aiohttpretty.register_json_uri('GET', revisions_url, body={}, status=403)

        result = await provider.revisions(path)
        expected = [BoxRevision(item)]
        assert result == expected
        assert aiohttpretty.has_call(method='GET', uri=file_url)
        assert aiohttpretty.has_call(method='GET', uri=revisions_url)


class TestIntraCopy:
    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_copy_file(self, provider, root_provider_fixtures):
        item = root_provider_fixtures['file_metadata']['entries'][0]
        src_path = WaterButlerPath('/name.txt', _ids=(provider, item['id']))
        dest_path = WaterButlerPath('/charmander/name.txt', _ids=(provider, item['id']))

        file_url = provider.build_url('files', src_path.identifier, 'copy')
        aiohttpretty.register_json_uri('POST', file_url, body=item)

        result = await provider.intra_copy(provider, src_path, dest_path)
        expected = (BoxFileMetadata(item, dest_path), True)

        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_copy_file_replace(self, provider, root_provider_fixtures):
        item = root_provider_fixtures['file_metadata']['entries'][0]
        src_path = WaterButlerPath('/name.txt', _ids=(provider, item['id']))
        dest_path = WaterButlerPath('/charmander/name.txt', _ids=(provider, item['id'], item['id']))

        file_url = provider.build_url('files', src_path.identifier, 'copy')
        delete_url = provider.build_url('files', dest_path.identifier)
        aiohttpretty.register_uri('DELETE', delete_url, status=204)
        aiohttpretty.register_json_uri('POST', file_url, body=item)

        result = await provider.intra_copy(provider, src_path, dest_path)
        expected = (BoxFileMetadata(item, dest_path), False)

        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_copy_folder(self, provider, intra_fixtures, root_provider_fixtures):
        item = intra_fixtures['intra_folder_metadata']
        list_metadata = root_provider_fixtures['folder_list_metadata']

        src_path = WaterButlerPath('/name/', _ids=(provider, item['id']))
        dest_path = WaterButlerPath('/charmander/name/', _ids=(provider, item['id']))

        file_url = provider.build_url('folders', src_path.identifier, 'copy')
        list_url = provider.build_url('folders', item['id'], 'items',
                                      fields='id,name,size,modified_at,etag,total_count',
                                      offset=0, limit=1000)

        aiohttpretty.register_json_uri('GET', list_url, body=list_metadata)
        aiohttpretty.register_json_uri('POST', file_url, body=item)

        expected_folder = BoxFolderMetadata(item, dest_path)
        expected_folder._children = []
        for child_item in list_metadata['entries']:
            child_path = dest_path.child(child_item['name'], folder=(child_item['type'] == 'folder'))
            serialized_child = provider._serialize_item(child_item, child_path)
            expected_folder._children.append(serialized_child)
        expected = (expected_folder, True)

        result = await provider.intra_copy(provider, src_path, dest_path)

        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_copy_folder_replace(self, provider, intra_fixtures, root_provider_fixtures):
        item = intra_fixtures['intra_folder_metadata']
        list_metadata = root_provider_fixtures['folder_list_metadata']

        src_path = WaterButlerPath('/name/', _ids=(provider, item['id']))
        dest_path = WaterButlerPath('/charmander/name/', _ids=(provider, item['id'], item['id']))

        file_url = provider.build_url('folders', src_path.identifier, 'copy')
        delete_url = provider.build_url('folders', dest_path.identifier, recursive=True)
        list_url = provider.build_url('folders', item['id'], 'items',
                                      fields='id,name,size,modified_at,etag,total_count',
                                      offset=0, limit=1000)

        aiohttpretty.register_json_uri('GET', list_url, body=list_metadata)
        aiohttpretty.register_uri('DELETE', delete_url, status=204)
        aiohttpretty.register_json_uri('POST', file_url, body=item)

        expected_folder = BoxFolderMetadata(item, dest_path)
        expected_folder._children = []
        for child_item in list_metadata['entries']:
            child_path = dest_path.child(child_item['name'], folder=(child_item['type'] == 'folder'))
            serialized_child = provider._serialize_item(child_item, child_path)
            expected_folder._children.append(serialized_child)
        expected = (expected_folder, False)

        result = await provider.intra_copy(provider, src_path, dest_path)

        assert result == expected
        assert aiohttpretty.has_call(method='DELETE', uri=delete_url)


class TestIntraMove:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_move_file(self, provider, root_provider_fixtures):
        item = root_provider_fixtures['file_metadata']['entries'][0]
        src_path = WaterButlerPath('/name.txt', _ids=(provider, item['id']))
        dest_path = WaterButlerPath('/charmander/name.txt', _ids=(provider, item['id']))

        file_url = provider.build_url('files', src_path.identifier)
        aiohttpretty.register_json_uri('PUT', file_url, body=item)

        result = await provider.intra_move(provider, src_path, dest_path)
        expected = (BoxFileMetadata(item, dest_path), True)

        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_move_file_replace(self, provider, root_provider_fixtures):
        item = root_provider_fixtures['file_metadata']['entries'][0]
        src_path = WaterButlerPath('/name.txt', _ids=(provider, item['id']))
        dest_path = WaterButlerPath('/charmander/name.txt', _ids=(provider, item['id'], item['id']))

        file_url = provider.build_url('files', src_path.identifier)
        delete_url = provider.build_url('files', dest_path.identifier)
        aiohttpretty.register_uri('DELETE', delete_url, status=204)
        aiohttpretty.register_json_uri('PUT', file_url, body=item)

        result = await provider.intra_move(provider, src_path, dest_path)
        expected = (BoxFileMetadata(item, dest_path), False)

        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_move_folder(self, provider, intra_fixtures, root_provider_fixtures):
        item = intra_fixtures['intra_folder_metadata']
        list_metadata = root_provider_fixtures['folder_list_metadata']

        src_path = WaterButlerPath('/name/', _ids=(provider, item['id']))
        dest_path = WaterButlerPath('/charmander/name/', _ids=(provider, item['id']))

        file_url = provider.build_url('folders', src_path.identifier)
        list_url = provider.build_url('folders', item['id'], 'items',
                                      fields='id,name,size,modified_at,etag,total_count',
                                      offset=0, limit=1000)

        aiohttpretty.register_json_uri('PUT', file_url, body=item)
        aiohttpretty.register_json_uri('GET', list_url, body=list_metadata)

        expected_folder = BoxFolderMetadata(item, dest_path)
        expected_folder._children = []
        for child_item in list_metadata['entries']:
            child_path = dest_path.child(child_item['name'], folder=(child_item['type'] == 'folder'))
            serialized_child = provider._serialize_item(child_item, child_path)
            expected_folder._children.append(serialized_child)
        expected = (expected_folder, True)

        result = await provider.intra_move(provider, src_path, dest_path)

        assert result == expected


    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_move_folder_replace(self, provider, intra_fixtures, root_provider_fixtures):
        item = intra_fixtures['intra_folder_metadata']
        list_metadata = root_provider_fixtures['folder_list_metadata']

        src_path = WaterButlerPath('/name/', _ids=(provider, item['id']))
        dest_path = WaterButlerPath('/charmander/name/', _ids=(provider, item['id'], item['id']))

        file_url = provider.build_url('folders', src_path.identifier)
        delete_url = provider.build_url('folders', dest_path.identifier, recursive=True)
        list_url = provider.build_url('folders', item['id'], 'items',
                                      fields='id,name,size,modified_at,etag,total_count',
                                      offset=0, limit=1000)

        aiohttpretty.register_json_uri('PUT', file_url, body=item)
        aiohttpretty.register_uri('DELETE', delete_url, status=204)
        aiohttpretty.register_json_uri('GET', list_url, body=list_metadata)

        expected_folder = BoxFolderMetadata(item, dest_path)
        expected_folder._children = []
        for child_item in list_metadata['entries']:
            child_path = dest_path.child(child_item['name'], folder=(child_item['type'] == 'folder'))
            serialized_child = provider._serialize_item(child_item, child_path)
            expected_folder._children.append(serialized_child)
        expected = (expected_folder, False)

        result = await provider.intra_move(provider, src_path, dest_path)

        assert result == expected
        assert aiohttpretty.has_call(method='DELETE', uri=delete_url)


class TestCreateFolder:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_must_be_folder(self, provider):
        path = WaterButlerPath('/Just a poor file from a poor folder', _ids=(provider.folder, None))

        with pytest.raises(exceptions.CreateFolderError) as e:
            await provider.create_folder(path)

        assert e.value.code == 400
        assert e.value.message == 'Path must be a directory'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_id_must_be_none(self, provider):
        path = WaterButlerPath('/Just a poor file from a poor folder/',
                               _ids=(provider.folder, 'someid'))

        assert path.identifier is not None

        with pytest.raises(exceptions.FolderNamingConflict) as e:
            await provider.create_folder(path)

        assert e.value.code == 409
        assert e.value.message == ('Cannot create folder "Just a poor file from a poor folder", '
                                   'because a file or folder already exists with that name')

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_already_exists(self, provider):
        url = provider.build_url('folders')
        data_url = provider.build_url('folders', provider.folder)
        path = WaterButlerPath('/50 shades of nope/', _ids=(provider.folder, None))

        aiohttpretty.register_json_uri('POST', url, status=409)
        aiohttpretty.register_json_uri('GET', data_url, body={
            'id': provider.folder,
            'type': 'folder',
            'name': 'All Files',
            'path_collection': {
                'entries': []
            }
        })

        with pytest.raises(exceptions.FolderNamingConflict) as e:
            await provider.create_folder(path)

        assert e.value.code == 409
        assert e.value.message == ('Cannot create folder "50 shades of nope", because a file or '
                                   'folder already exists with that name')

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_returns_metadata(self, provider, root_provider_fixtures):
        url = provider.build_url('folders')
        folder_metadata = root_provider_fixtures['folder_object_metadata']
        folder_metadata['name'] = '50 shades of nope'
        path = WaterButlerPath('/50 shades of nope/', _ids=(provider.folder, None))

        aiohttpretty.register_json_uri('POST', url, status=201, body=folder_metadata)

        resp = await provider.create_folder(path)

        assert resp.kind == 'folder'
        assert resp.name == '50 shades of nope'
        assert resp.path == '/{}/'.format(folder_metadata['id'])
        assert isinstance(resp, BoxFolderMetadata)
        assert path.identifier_path == '/' + folder_metadata['id'] + '/'


class TestOperations:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_can_duplicate_names(self, provider):
        assert provider.can_duplicate_names() is False

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_shares_storage_root(self, provider, other_provider):
        assert provider.shares_storage_root(other_provider) is False
        assert provider.shares_storage_root(provider) is True

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_can_intra_move(self, provider, other_provider):
        assert provider.can_intra_move(other_provider) is False
        assert provider.can_intra_move(provider) is True

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_can_intra_copy(self, provider, other_provider):
        assert provider.can_intra_copy(other_provider) is False
        assert provider.can_intra_copy(provider) is True
