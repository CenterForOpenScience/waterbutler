import io
import json
import time
from unittest import mock
from http import HTTPStatus

import pytest
import aiohttpretty

from waterbutler.core import exceptions
from waterbutler.core.streams import FileStreamReader, ResponseStreamReader
from waterbutler.providers.googlecloud.metadata import GoogleCloudFileMetadata
from waterbutler.providers.googlecloud import utils, settings, GoogleCloudProvider

from tests.providers.googlecloud.fixtures.providers import (mock_auth,
                                                            mock_auth_2,
                                                            mock_creds,
                                                            mock_creds_2,
                                                            mock_settings,
                                                            mock_settings_2)

from tests.providers.googlecloud.fixtures.files import (file_raw,
                                                        file_name,
                                                        file_wb_path,
                                                        file_obj_name,
                                                        meta_file_raw,
                                                        meta_file_parsed,
                                                        meta_file_upload_raw,
                                                        meta_file_copy_raw,
                                                        file_2_wb_path,
                                                        file_2_obj_name,
                                                        file_2_copy_obj_name)

from tests.providers.googlecloud.fixtures.folders import folder_wb_path, folder_obj_name


@pytest.fixture()
def mock_provider(mock_auth, mock_creds, mock_settings):
    return GoogleCloudProvider(mock_auth, mock_creds, mock_settings)


@pytest.fixture()
def mock_provider_2(mock_auth_2, mock_creds_2, mock_settings_2):
    return GoogleCloudProvider(mock_auth_2, mock_creds_2, mock_settings_2)


@pytest.fixture
def mock_time(monkeypatch):
    mock_time = mock.Mock(return_value=1234567890.0)
    monkeypatch.setattr(time, 'time', mock_time)


@pytest.fixture
def file_stream_file(file_raw):
    return FileStreamReader(io.BytesIO(file_raw))


class TestProviderInit:

    def test_provider_init(self, mock_provider, mock_settings, mock_creds):

        assert mock_provider is not None
        assert mock_provider.NAME == 'googlecloud'
        assert mock_provider.BASE_URL == settings.BASE_URL
        assert mock_provider.bucket == mock_settings.get('bucket')

        json_creds = mock_creds.get('json_creds')
        assert mock_provider.creds is not None
        assert mock_provider.creds.project_id == json_creds.get('project_id')
        assert mock_provider.creds.service_account_email == json_creds.get('client_email')


class TestValidatePath:

    @pytest.mark.asyncio
    async def test_validate_v1_path_file(self, mock_provider, file_wb_path):
        file_path = '/{}'.format(file_wb_path.path)
        assert file_path.startswith('/') and not file_path.endswith('/')
        wb_path = await mock_provider.validate_v1_path(file_path)
        assert wb_path == file_wb_path

    @pytest.mark.asyncio
    async def test_validate_v1_path_folder(self, mock_provider, folder_wb_path):
        folder_path = '/{}'.format(folder_wb_path.path)
        assert folder_path.startswith('/') and folder_path.endswith('/')
        wb_path = await mock_provider.validate_v1_path(folder_path)
        assert wb_path == folder_wb_path

    @pytest.mark.asyncio
    async def test_validate_path_file(self, mock_provider, file_wb_path):
        file_path = '/{}'.format(file_wb_path.path)
        assert file_path.startswith('/') and not file_path.endswith('/')
        wb_path = await mock_provider.validate_path(file_path)
        assert wb_path == file_wb_path

    @pytest.mark.asyncio
    async def test_validate_path_folder(self, mock_provider, folder_wb_path):
        folder_path = '/{}'.format(folder_wb_path.path)
        assert folder_path.startswith('/') and folder_path.endswith('/')
        wb_path = await mock_provider.validate_path(folder_path)
        assert wb_path == folder_wb_path


class TestMetadata:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file(self, mock_time, mock_provider, file_wb_path, meta_file_raw,
                                 meta_file_parsed):
        file_obj_name = utils.get_obj_name(file_wb_path, is_folder=False)
        signed_url = mock_provider._build_and_sign_url('HEAD', file_obj_name, **{})

        resp_headers = utils.get_multi_dict_from_python_dict(dict(json.loads(meta_file_raw)))
        aiohttpretty.register_uri(
            'HEAD',
            signed_url,
            headers=resp_headers,
            status=HTTPStatus.OK
        )

        metadata_json = json.loads(meta_file_parsed)
        metadata_expected = GoogleCloudFileMetadata(metadata_json)

        metadata = await mock_provider._metadata_object(file_wb_path, is_folder=False)

        assert isinstance(metadata, GoogleCloudFileMetadata)
        assert metadata == metadata_expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_object_401_unauthorized(self, mock_time, mock_provider, file_wb_path):
        file_obj_name = utils.get_obj_name(file_wb_path, is_folder=False)
        signed_url = mock_provider._build_and_sign_url('HEAD', file_obj_name, **{})

        aiohttpretty.register_uri(
            'HEAD',
            signed_url,
            status=HTTPStatus.UNAUTHORIZED
        )

        with pytest.raises(exceptions.MetadataError) as exc:
            await mock_provider._metadata_object(file_wb_path, is_folder=False)

        assert exc.value.code == HTTPStatus.UNAUTHORIZED
        assert aiohttpretty.has_call(method='HEAD', uri=signed_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_object_404_not_found(self, mock_time, mock_provider, file_wb_path):
        file_obj_name = utils.get_obj_name(file_wb_path, is_folder=False)
        signed_url = mock_provider._build_and_sign_url('HEAD', file_obj_name, **{})

        aiohttpretty.register_uri(
            'HEAD',
            signed_url,
            status=HTTPStatus.NOT_FOUND
        )

        with pytest.raises(exceptions.MetadataError) as exc:
            await mock_provider._metadata_object(file_wb_path, is_folder=False)

        assert exc.value.code == HTTPStatus.NOT_FOUND
        assert aiohttpretty.has_call(method='HEAD', uri=signed_url)


class TestOperations:

    def test_provider_equality(self, mock_provider, mock_provider_2):

        assert mock_provider != mock_provider_2
        assert type(mock_provider) == type(mock_provider_2)

    def test_can_intra_move(self, mock_provider, mock_provider_2, file_wb_path, folder_wb_path):

        assert mock_provider.can_intra_move(mock_provider, file_wb_path)
        assert not mock_provider.can_intra_move(mock_provider_2, file_wb_path)
        assert not mock_provider.can_intra_move(mock_provider, folder_wb_path)
        assert not mock_provider.can_intra_move(mock_provider_2, folder_wb_path)

    def test_can_intra_copy(self, mock_provider, mock_provider_2, file_wb_path, folder_wb_path):

        assert mock_provider.can_intra_copy(mock_provider, file_wb_path)
        assert not mock_provider.can_intra_copy(mock_provider_2, file_wb_path)
        assert not mock_provider.can_intra_copy(mock_provider, folder_wb_path)
        assert not mock_provider.can_intra_copy(mock_provider_2, folder_wb_path)

    def test_can_duplicate_names(self, mock_provider):

        assert mock_provider.can_duplicate_names()


class TestCRUD:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_file(self, mock_time, mock_provider, file_wb_path, file_raw):
        file_obj_name = utils.get_obj_name(file_wb_path, is_folder=False)
        signed_url = mock_provider._build_and_sign_url('GET', file_obj_name, **{})

        aiohttpretty.register_uri(
            'GET',
            signed_url,
            body=file_raw,
            status=HTTPStatus.OK
        )

        resp_stream_reader = await mock_provider.download(file_wb_path)
        file_content = await resp_stream_reader.read()

        assert aiohttpretty.has_call(method='GET', uri=signed_url)
        assert isinstance(resp_stream_reader, ResponseStreamReader)
        assert file_content == file_raw

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_file_with_accept_url(self, mock_time, mock_provider, file_wb_path):
        file_obj_name = utils.get_obj_name(file_wb_path, is_folder=False)
        query = {
            'response-content-disposition': ('attachment; filename="text-file-1.txt"; '
                                             'filename*=UTF-8\'\'text-file-1.txt')
        }
        signed_url = mock_provider._build_and_sign_url('GET', file_obj_name, **query)
        return_url = await mock_provider.download(file_wb_path, accept_url=True)

        assert not aiohttpretty.has_call(method='GET', uri=signed_url)
        assert isinstance(return_url, str)
        assert signed_url == return_url

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize("display_name_arg,expected_name", [
        ('meow.txt', 'meow.txt'),
        ('',         'text-file-1.txt'),
        (None,       'text-file-1.txt'),
    ])
    async def test_download_file_with_display_name(self, mock_time, mock_provider, file_wb_path,
                                                   display_name_arg, expected_name):
        file_obj_name = utils.get_obj_name(file_wb_path, is_folder=False)
        query = {
            'response-content-disposition': ('attachment; filename="{}"; '
                                             'filename*=UTF-8\'\'{}').format(expected_name,
                                                                             expected_name)
        }
        signed_url = mock_provider._build_and_sign_url('GET', file_obj_name, **query)
        return_url = await mock_provider.download(file_wb_path, accept_url=True,
                                                  display_name=display_name_arg)

        assert not aiohttpretty.has_call(method='GET', uri=signed_url)
        assert isinstance(return_url, str)
        assert signed_url == return_url

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_file_not_found(self, mock_time, mock_provider, file_wb_path):

        file_obj_name = utils.get_obj_name(file_wb_path, is_folder=False)
        signed_url = mock_provider._build_and_sign_url('GET', file_obj_name, **{})

        aiohttpretty.register_uri(
            'GET',
            signed_url,
            status=HTTPStatus.NOT_FOUND
        )

        with pytest.raises(exceptions.DownloadError) as exc:
            await mock_provider.download(file_wb_path, is_folder=False)

        assert exc.value.code == HTTPStatus.NOT_FOUND
        assert aiohttpretty.has_call(method='GET', uri=signed_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_file(self, mock_time, mock_provider, file_wb_path, meta_file_raw,
                               meta_file_parsed, meta_file_upload_raw, file_stream_file):
        file_obj_name = utils.get_obj_name(file_wb_path, is_folder=False)

        signed_url_upload = mock_provider._build_and_sign_url('PUT', file_obj_name, **{})
        resp_headers = utils.get_multi_dict_from_python_dict(dict(json.loads(meta_file_upload_raw)))
        aiohttpretty.register_uri(
            'PUT',
            signed_url_upload,
            headers=resp_headers,
            status=HTTPStatus.OK
        )

        signed_url_metadata = mock_provider._build_and_sign_url('HEAD', file_obj_name, **{})
        resp_headers = utils.get_multi_dict_from_python_dict(dict(json.loads(meta_file_raw)))
        aiohttpretty.register_uri(
            'HEAD',
            signed_url_metadata,
            headers=resp_headers,
            status=HTTPStatus.OK
        )

        metadata_json = json.loads(meta_file_parsed)
        metadata_expected = GoogleCloudFileMetadata(metadata_json)

        metadata, _ = await mock_provider.upload(file_stream_file, file_wb_path)

        assert metadata == metadata_expected
        assert aiohttpretty.has_call(method='PUT', uri=signed_url_upload)
        assert aiohttpretty.has_call(method='HEAD', uri=signed_url_metadata)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_file_checksum_mismatch(self, mock_time, mock_provider, file_wb_path,
                                                 meta_file_raw, meta_file_upload_raw,
                                                 file_stream_file):
        file_obj_name = utils.get_obj_name(file_wb_path, is_folder=False)

        signed_url_upload = mock_provider._build_and_sign_url('PUT', file_obj_name, **{})
        # There is no need to use `MultiDict` since the hashes are not used
        resp_headers_dict = dict(json.loads(meta_file_upload_raw))
        resp_headers_dict.update({'etag': '"9e780e1c4ee28c44642160b349b3aab0"'})
        resp_headers = utils.get_multi_dict_from_python_dict(resp_headers_dict)
        aiohttpretty.register_uri(
            'PUT',
            signed_url_upload,
            headers=resp_headers,
            status=HTTPStatus.OK
        )

        signed_url_metadata = mock_provider._build_and_sign_url('HEAD', file_obj_name, **{})
        # There is no need to use `MultiDict` since the hashes are not used
        resp_headers = utils.get_multi_dict_from_python_dict(dict(json.loads(meta_file_raw)))
        aiohttpretty.register_uri(
            'HEAD',
            signed_url_metadata,
            headers=resp_headers,
            status=HTTPStatus.OK
        )

        with pytest.raises(exceptions.UploadChecksumMismatchError) as exc:
            await mock_provider.upload(file_stream_file, file_wb_path)

        assert exc.value.code == HTTPStatus.INTERNAL_SERVER_ERROR
        assert aiohttpretty.has_call(method='HEAD', uri=signed_url_metadata)
        assert aiohttpretty.has_call(method='PUT', uri=signed_url_upload)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_file(self, mock_time, mock_provider, file_wb_path):

        file_obj_name = utils.get_obj_name(file_wb_path, is_folder=False)
        signed_url = mock_provider._build_and_sign_url('DELETE', file_obj_name, **{})

        aiohttpretty.register_uri(
            'DELETE',
            signed_url,
            status=HTTPStatus.NO_CONTENT
        )

        await mock_provider.delete(file_wb_path)

        assert aiohttpretty.has_call(method='DELETE', uri=signed_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_file_not_found(self, mock_time, mock_provider, file_wb_path):

        file_obj_name = utils.get_obj_name(file_wb_path, is_folder=False)
        signed_url = mock_provider._build_and_sign_url('DELETE', file_obj_name, **{})

        aiohttpretty.register_uri(
            'DELETE',
            signed_url,
            status=HTTPStatus.NOT_FOUND
        )

        with pytest.raises(exceptions.DeleteError) as exc:
            await mock_provider.delete(file_wb_path)

        assert exc.value.code == HTTPStatus.NOT_FOUND
        assert aiohttpretty.has_call(method='DELETE', uri=signed_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_copy_file(self, mock_time, mock_provider, file_wb_path, file_2_wb_path,
                                   meta_file_raw, meta_file_parsed, meta_file_copy_raw):
        src_file_path = file_2_wb_path
        dest_file_path = file_wb_path
        src_file_obj_name = utils.get_obj_name(src_file_path, is_folder=False)
        dest_file_obj_name = utils.get_obj_name(dest_file_path, is_folder=False)

        object_name_with_bucket = '{}/{}'.format(mock_provider.bucket, src_file_obj_name)
        canonical_ext_headers = {'x-goog-copy-source': object_name_with_bucket}
        signed_url_intra_copy = mock_provider._build_and_sign_url(
            'PUT',
            dest_file_obj_name,
            canonical_ext_headers=canonical_ext_headers,
            **{}
        )
        resp_headers = utils.get_multi_dict_from_python_dict(dict(json.loads(meta_file_copy_raw)))
        aiohttpretty.register_uri(
            'PUT',
            signed_url_intra_copy,
            headers=resp_headers,
            status=HTTPStatus.OK
        )

        signed_url_metadata = mock_provider._build_and_sign_url('HEAD', dest_file_obj_name, **{})
        resp_headers = utils.get_multi_dict_from_python_dict(dict(json.loads(meta_file_raw)))
        aiohttpretty.register_uri(
            'HEAD',
            signed_url_metadata,
            headers=resp_headers,
            status=HTTPStatus.OK
        )

        metadata_json = json.loads(meta_file_parsed)
        metadata_expected = GoogleCloudFileMetadata(metadata_json)

        metadata, _ = await mock_provider.intra_copy(mock_provider, src_file_path, dest_file_path)

        assert metadata == metadata_expected
        assert aiohttpretty.has_call(method='PUT', uri=signed_url_intra_copy)
        assert aiohttpretty.has_call(method='HEAD', uri=signed_url_metadata)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_copy_file_not_found(self, mock_time, mock_provider, file_wb_path,
                                             file_2_wb_path, meta_file_raw, meta_file_copy_raw):
        src_file_path = file_2_wb_path
        dest_file_path = file_wb_path
        src_file_obj_name = utils.get_obj_name(src_file_path, is_folder=False)
        dest_file_obj_name = utils.get_obj_name(dest_file_path, is_folder=False)

        object_name_with_bucket = '{}/{}'.format(mock_provider.bucket, src_file_obj_name)
        canonical_ext_headers = {'x-goog-copy-source': object_name_with_bucket}
        signed_url_intra_copy = mock_provider._build_and_sign_url(
            'PUT',
            dest_file_obj_name,
            canonical_ext_headers=canonical_ext_headers,
            **{}
        )
        resp_headers = utils.get_multi_dict_from_python_dict(dict(json.loads(meta_file_copy_raw)))
        aiohttpretty.register_uri(
            'PUT',
            signed_url_intra_copy,
            headers=resp_headers,
            status=HTTPStatus.NOT_FOUND
        )

        signed_url_metadata = mock_provider._build_and_sign_url('HEAD', dest_file_obj_name, **{})
        resp_headers = utils.get_multi_dict_from_python_dict(dict(json.loads(meta_file_raw)))
        aiohttpretty.register_uri(
            'HEAD',
            signed_url_metadata,
            headers=resp_headers,
            status=HTTPStatus.OK
        )

        with pytest.raises(exceptions.CopyError) as exc:
            await mock_provider.intra_copy(mock_provider, src_file_path, dest_file_path)

        assert exc.value.code == HTTPStatus.NOT_FOUND
        assert aiohttpretty.has_call(method='PUT', uri=signed_url_intra_copy)
        assert aiohttpretty.has_call(method='HEAD', uri=signed_url_metadata)
