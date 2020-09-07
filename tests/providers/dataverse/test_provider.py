import pytest

import io
import json
from http import client

import aiohttpretty

from waterbutler.core import streams
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath
from waterbutler.providers.dataverse import settings as dvs
from waterbutler.providers.dataverse import DataverseProvider
from waterbutler.providers.dataverse.metadata import DataverseFileMetadata, DataverseRevision

from tests.providers.dataverse.fixtures import (
    native_file_metadata,
    native_dataset_metadata,
    empty_native_dataset_metadata,
    checksum_mismatch_dataset_metadata,
    auth,
    credentials,
    settings
)


@pytest.fixture
def provider(auth, credentials, settings):
    return DataverseProvider(auth, credentials, settings)


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
    async def test_validate_v1_path_file(self, provider, native_dataset_metadata):
        draft_url = provider.build_url(dvs.JSON_BASE_URL.format(provider._id, 'latest'),
                                       key=provider.token)
        published_url = provider.build_url(dvs.JSON_BASE_URL.format(provider._id,
                                                                    'latest-published'),
                                           key=provider.token)

        aiohttpretty.register_json_uri('GET',
                                       draft_url,
                                       status=200,
                                       body=native_dataset_metadata)
        aiohttpretty.register_json_uri('GET',
                                       published_url,
                                       status=200,
                                       body=native_dataset_metadata)

        path = '/21'

        try:
            wb_path_v1 = await provider.validate_v1_path(path)
        except Exception as exc:
            pytest.fail(str(exc))

        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider.validate_v1_path(path + '/')

        assert exc.value.code == client.NOT_FOUND

        wb_path_v0 = await provider.validate_path(path)

        assert wb_path_v1 == wb_path_v0

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_folder(self, provider):
        try:
            wb_path_v1 = await provider.validate_v1_path('/')
        except Exception as exc:
            pytest.fail(str(exc))

        wb_path_v0 = await provider.validate_path('/')

        assert wb_path_v1 == wb_path_v0

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_revalidate_path(self, provider, native_dataset_metadata):
        draft_url = provider.build_url(dvs.JSON_BASE_URL.format(provider._id, 'latest'),
                                       key=provider.token)
        published_url = provider.build_url(dvs.JSON_BASE_URL.format(provider._id,
                                                                    'latest-published'),
                                           key=provider.token)

        aiohttpretty.register_json_uri('GET',
                                       draft_url,
                                       status=200,
                                       body=native_dataset_metadata)
        aiohttpretty.register_json_uri('GET',
                                       published_url,
                                       status=200,
                                       body=native_dataset_metadata)


        base = await provider.validate_v1_path('/')

        wb_path = await provider.revalidate_path(base, '/thefile.txt')
        assert wb_path.name == 'thefile.txt'

        wb_path = await provider.revalidate_path(base, '/new_path')
        assert wb_path.name == 'new_path'


class TestCRUD:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download(self, provider, native_dataset_metadata):
        path = '/21'
        url = provider.build_url(dvs.DOWN_BASE_URL, path, key=provider.token)
        draft_url = provider.build_url(dvs.JSON_BASE_URL.format(provider._id, 'latest'),
                                       key=provider.token)
        published_url = provider.build_url(dvs.JSON_BASE_URL.format(provider._id,
                                                                    'latest-published'),
                                           key=provider.token)

        aiohttpretty.register_uri('GET', url, body=b'better', auto_length=True)
        aiohttpretty.register_json_uri('GET',
                                       draft_url,
                                       status=200,
                                       body=native_dataset_metadata)
        aiohttpretty.register_json_uri('GET',
                                       published_url,
                                       status=200,
                                       body=native_dataset_metadata)

        path = await provider.validate_path(path)

        result = await provider.download(path)
        content = await result.read()

        assert content == b'better'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_not_found(self, provider, native_dataset_metadata):
        path = '/21'
        url = provider.build_url(dvs.DOWN_BASE_URL, path, key=provider.token)
        aiohttpretty.register_uri('GET', url, status=404)
        draft_url = provider.build_url(dvs.JSON_BASE_URL.format(provider._id, 'latest'),
                                       key=provider.token)
        aiohttpretty.register_json_uri('GET', draft_url, status=200, body=native_dataset_metadata)
        published_url = provider.build_url(dvs.JSON_BASE_URL.format(provider._id,
                                                                    'latest-published'),
                                           key=provider.token)
        aiohttpretty.register_json_uri('GET',
                                       published_url,
                                       status=200,
                                       body=native_dataset_metadata)

        path = await provider.validate_path(path)

        with pytest.raises(exceptions.DownloadError):
            await provider.download(path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_invalid_path(self, provider, native_dataset_metadata):
        path = '/50'
        draft_url = provider.build_url(dvs.JSON_BASE_URL.format(provider._id,
                                                                'latest'),
                                       key=provider.token)
        aiohttpretty.register_json_uri('GET', draft_url, status=200, body=native_dataset_metadata)
        published_url = provider.build_url(dvs.JSON_BASE_URL.format(provider._id,
                                                                    'latest-published'),
                                           key=provider.token)
        aiohttpretty.register_json_uri('GET',
                                       published_url,
                                       status=200,
                                       body=native_dataset_metadata)

        path = await provider.validate_path(path)

        with pytest.raises(exceptions.NotFoundError):
            await provider.download(path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_create(self, provider, file_stream, native_file_metadata,
                                 empty_native_dataset_metadata, native_dataset_metadata):
        path = '/thefile.txt'
        url = provider.build_url(dvs.EDIT_MEDIA_BASE_URL, 'study', provider.doi)
        aiohttpretty.register_uri('POST', url, status=201)
        latest_url = provider.build_url(
            dvs.JSON_BASE_URL.format(provider._id, 'latest'),
            key=provider.token
        )
        latest_published_url = provider.build_url(
            dvs.JSON_BASE_URL.format(provider._id, 'latest-published'),
            key=provider.token
        )

        aiohttpretty.register_json_uri('GET', latest_published_url, body={'data': {'files': []}})
        aiohttpretty.register_uri('GET', latest_url, responses=[
            {
                'status': 200,
                'body': json.dumps(empty_native_dataset_metadata).encode('utf-8'),
                'headers': {'Content-Type': 'application/json'},
            },
            {
                'status': 200,
                'body': json.dumps(native_dataset_metadata).encode('utf-8'),
                'headers': {'Content-Type': 'application/json'},
            },
        ])

        path = await provider.validate_path(path)
        metadata, created = await provider.upload(file_stream, path)

        entry = native_file_metadata['datafile']
        expected = DataverseFileMetadata(entry, 'latest')

        assert created is True
        assert metadata == expected
        assert aiohttpretty.has_call(method='POST', uri=url)
        assert aiohttpretty.has_call(method='GET', uri=latest_url)
        assert aiohttpretty.has_call(method='GET', uri=latest_published_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_updates(self, provider,
                                  file_stream,
                                  native_file_metadata,
                                  native_dataset_metadata):
        path = '/20'
        url = provider.build_url(dvs.EDIT_MEDIA_BASE_URL, 'study', provider.doi)
        aiohttpretty.register_uri('POST', url, status=201)
        published_url = provider.build_url(dvs.JSON_BASE_URL.format(provider._id, 'latest'),
                                           key=provider.token)
        aiohttpretty.register_json_uri('GET',
                                       published_url,
                                       status=200,
                                       body=native_dataset_metadata)
        delete_url = provider.build_url(dvs.EDIT_MEDIA_BASE_URL, 'file', '/20')  # Old file id
        aiohttpretty.register_json_uri('DELETE', delete_url, status=204)
        latest_published_url = provider.build_url(dvs.JSON_BASE_URL.format(provider._id,
                                                                           'latest-published'),
                                                  key=provider.token)

        aiohttpretty.register_json_uri('GET', latest_published_url, body={'data': {'files': []}})

        path = await provider.validate_path(path)
        metadata, created = await provider.upload(file_stream, path)

        entry = native_file_metadata['datafile']
        expected = DataverseFileMetadata(entry, 'latest')

        assert metadata == expected
        assert created is False
        assert aiohttpretty.has_call(method='POST', uri=url)
        assert aiohttpretty.has_call(method='GET', uri=published_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_checksum_mismatch(self, provider, file_stream,
                                            empty_native_dataset_metadata,
                                            checksum_mismatch_dataset_metadata):
        path = '/thefile.txt'
        url = provider.build_url(dvs.EDIT_MEDIA_BASE_URL, 'study', provider.doi)
        aiohttpretty.register_uri('POST', url, status=201)
        latest_url = provider.build_url(
            dvs.JSON_BASE_URL.format(provider._id, 'latest'),
            key=provider.token
        )
        latest_published_url = provider.build_url(
            dvs.JSON_BASE_URL.format(provider._id, 'latest-published'),
            key=provider.token
        )

        aiohttpretty.register_json_uri('GET', latest_published_url, body={'data': {'files': []}})
        aiohttpretty.register_uri('GET', latest_url, responses=[
            {
                'status': 200,
                'body': json.dumps(empty_native_dataset_metadata).encode('utf-8'),
                'headers': {'Content-Type': 'application/json'},
            },
            {
                'status': 200,
                'body': json.dumps(checksum_mismatch_dataset_metadata).encode('utf-8'),
                'headers': {'Content-Type': 'application/json'},
            },
        ])

        path = await provider.validate_path(path)
        with pytest.raises(exceptions.UploadChecksumMismatchError):
            await provider.upload(file_stream, path)

        assert aiohttpretty.has_call(method='POST', uri=url)
        assert aiohttpretty.has_call(method='GET', uri=latest_url)
        assert aiohttpretty.has_call(method='GET', uri=latest_published_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_file(self, provider, native_dataset_metadata):
        path = '21'
        url = provider.build_url(dvs.EDIT_MEDIA_BASE_URL, 'file', path)
        aiohttpretty.register_json_uri('DELETE', url, status=204)
        draft_url = provider.build_url(dvs.JSON_BASE_URL.format(provider._id,
                                                                'latest'),
                                       key=provider.token)
        aiohttpretty.register_json_uri('GET', draft_url, status=200, body=native_dataset_metadata)
        published_url = provider.build_url(dvs.JSON_BASE_URL.format(provider._id,
                                                                    'latest-published'),
                                           key=provider.token)
        aiohttpretty.register_json_uri('GET',
                                       published_url,
                                       status=200,
                                       body=native_dataset_metadata)

        path = await provider.validate_path(path)
        await provider.delete(path)

        assert aiohttpretty.has_call(method='DELETE', uri=url)


class TestRevisions:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_revisions(self, provider, native_dataset_metadata):

        url = provider.build_url(dvs.JSON_BASE_URL.format(provider._id, 'latest-published'),
                                 key=provider.token)
        aiohttpretty.register_json_uri('GET', url, status=200, body=native_dataset_metadata)

        url = provider.build_url(dvs.JSON_BASE_URL.format(provider._id, 'latest'),
                                 key=provider.token)
        aiohttpretty.register_json_uri('GET', url, status=200, body=native_dataset_metadata)

        path = WaterButlerPath('/thefile.txt', _ids=('?', '19'))
        result = await provider.revisions(path, version='latest')

        isinstance(result, DataverseRevision)
        assert result[0].raw == 'latest-published'


class TestMetadata:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata(self, provider, native_dataset_metadata):

        url = provider.build_url(dvs.JSON_BASE_URL.format(provider._id, 'latest-published'),
                                 key=provider.token)
        aiohttpretty.register_json_uri('GET', url, status=200, body=native_dataset_metadata)

        url = provider.build_url(dvs.JSON_BASE_URL.format(provider._id, 'latest'),
                                 key=provider.token)
        aiohttpretty.register_json_uri('GET', url, status=200, body=native_dataset_metadata)

        path = WaterButlerPath('/thefile.txt', _ids=('?', '19'))
        result = await provider.metadata(path, version='latest')

        assert result.provider == 'dataverse'
        assert result.kind == 'file'
        assert result.name == 'UnZip.java'
        assert result.path == '/19'
        assert result.extra['fileId'] == '19'
        assert result.materialized_path == '/UnZip.java'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_root(self, provider, native_dataset_metadata):
        url = provider.build_url(dvs.JSON_BASE_URL.format(provider._id, 'latest'),
                                 key=provider.token)
        aiohttpretty.register_json_uri('GET', url, status=200, body=native_dataset_metadata)

        path = await provider.validate_path('/')
        result = await provider.metadata(path, version='latest')

        assert len(result) == 3
        assert result[0].provider == 'dataverse'
        assert result[0].kind == 'file'
        assert result[0].name == 'UnZip.java'
        assert result[0].path == '/19'
        assert result[0].extra['fileId'] == '19'
        assert result[0].materialized_path == '/UnZip.java'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_no_files(self, provider, empty_native_dataset_metadata):
        url = provider.build_url(dvs.JSON_BASE_URL.format(provider._id, 'latest'),
                                 key=provider.token)
        aiohttpretty.register_json_uri('GET', url, status=200, body=empty_native_dataset_metadata)
        path = await provider.validate_path('/')
        result = await provider.metadata(path, version='latest')

        assert result == []

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_404(self, provider, native_dataset_metadata):

        url = provider.build_url(dvs.JSON_BASE_URL.format(provider._id, 'latest-published'),
                                 key=provider.token)
        aiohttpretty.register_json_uri('GET', url, status=404, body=native_dataset_metadata)

        url = provider.build_url(dvs.JSON_BASE_URL.format(provider._id, 'latest'),
                                 key=provider.token)
        aiohttpretty.register_json_uri('GET', url, status=200, body=native_dataset_metadata)

        path = WaterButlerPath('/thefilenotfound.txt', _ids=('?', 'nobody has this fileId'))

        with pytest.raises(exceptions.MetadataError):
            await provider.metadata(path, version='latest')

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_published(self, provider, native_dataset_metadata):
        url = provider.build_url(dvs.JSON_BASE_URL.format(provider._id, 'latest-published'),
                                 key=provider.token)
        aiohttpretty.register_json_uri('GET', url, status=200, body=native_dataset_metadata)

        path = await provider.validate_path('/')
        result = await provider.metadata(path, version='latest-published')

        assert len(result) == 3
        assert result[0].provider == 'dataverse'
        assert result[0].kind == 'file'
        assert result[0].name == 'UnZip.java'
        assert result[0].path == '/19'
        assert result[0].extra['fileId'] == '19'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_published_no_files(self, provider, empty_native_dataset_metadata):
        url = provider.build_url(dvs.JSON_BASE_URL.format(provider._id, 'latest-published'),
                                 key=provider.token)
        aiohttpretty.register_json_uri('GET', url, status=200, body=empty_native_dataset_metadata)

        path = await provider.validate_path('/')
        result = await provider.metadata(path, version='latest-published')

        assert result == []

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_draft_metadata_missing(self, provider):
        url = provider.build_url(dvs.JSON_BASE_URL.format(provider._id, 'latest'),
                                 key=provider.token)
        aiohttpretty.register_json_uri('GET', url, status=404)

        path = await provider.validate_path('/')

        with pytest.raises(exceptions.MetadataError):
            await provider.metadata(path, version='latest')

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_draft_metadata_no_state_catches_all(self, provider, native_dataset_metadata):
        draft_url = provider.build_url(dvs.JSON_BASE_URL.format(provider._id, 'latest'),
                                       key=provider.token)
        aiohttpretty.register_json_uri('GET', draft_url, status=200, body=native_dataset_metadata)
        published_url = provider.build_url(dvs.JSON_BASE_URL.format(provider._id,
                                                                    'latest-published'),
                                           key=provider.token)
        aiohttpretty.register_json_uri('GET',
                                       published_url,
                                       status=200,
                                       body=native_dataset_metadata)

        path = await provider.validate_path('/')
        result = await provider.metadata(path)

        assert isinstance(result, list)
        assert len(result) == 6

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_never_published(self, provider, native_dataset_metadata):
        published_url = provider.build_url(dvs.JSON_BASE_URL.format(provider._id,
                                                                    'latest-published'),
                                           key=provider.token)
        aiohttpretty.register_json_uri('GET', published_url, status=404)
        draft_url = provider.build_url(dvs.JSON_BASE_URL.format(provider._id,
                                                                'latest'),
                                       key=provider.token)
        aiohttpretty.register_json_uri('GET', draft_url, status=200, body=native_dataset_metadata)

        path = await provider.validate_path('/')
        result = await provider.metadata(path)

        assert len(result) == 3

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_never_published_raises_errors(self, provider):
        published_url = provider.build_url(dvs.JSON_BASE_URL.format(provider._id,
                                                                    'latest-published'),
                                           key=provider.token)
        aiohttpretty.register_json_uri('GET', published_url, status=400)

        path = await provider.validate_path('/')
        with pytest.raises(exceptions.MetadataError) as e:
            _ = await provider.metadata(path)

        assert e.value.code == 400


class TestUtils:

    def test_utils(self, provider):
        assert not provider.can_duplicate_names()
