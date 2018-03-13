import io
import json
import time
from unittest import mock
from http import HTTPStatus
from aiohttp import MultiDict

import pytest
import aiohttpretty

from waterbutler.core.path import WaterButlerPath
from waterbutler.core.streams import FileStreamReader, ResponseStreamReader
from waterbutler.core import exceptions as core_exceptions
from waterbutler.providers.googlecloud import utils
from waterbutler.providers.googlecloud import settings as pd_settings
from waterbutler.providers.googlecloud import (GoogleCloudProvider,
                                               BaseGoogleCloudMetadata,
                                               GoogleCloudFileMetadata,
                                               GoogleCloudFolderMetadata,
                                               )

from tests.providers.googlecloud.fixtures.providers import (mock_auth,
                                                            mock_auth_2,
                                                            mock_creds,
                                                            mock_creds_2,
                                                            mock_settings,
                                                            mock_settings_2,
                                                            )

from tests.providers.googlecloud.fixtures.files import (file_raw,
                                                        file_wb_path,
                                                        file_obj_name,
                                                        meta_file_raw,
                                                        meta_file_parsed,
                                                        file_2_obj_name,
                                                        file_2_copy_obj_name,
                                                        )

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
def file_stream_file_1(file_raw):
    return FileStreamReader(io.BytesIO(file_raw))


class TestProviderInit:
    """Test that provider initialization set properties correctly.
    """

    async def test_provider_init(self, mock_provider):

        assert mock_provider is not None
        assert mock_provider.NAME == 'googlecloud'
        assert mock_provider.BASE_URL == pd_settings.BASE_URL
        assert mock_provider.bucket == mock_settings.get('bucket')
        assert mock_provider.region == mock_settings.get('region')

        json_creds = mock_creds.get('json_creds')
        assert mock_provider.creds is not None
        assert mock_provider.creds.project_id == json_creds.get('project_id')
        assert mock_provider.creds.service_account_email == json_creds.get('client_email')


class TestValidatePath:

    @pytest.mark.asyncio
    async def test_validate_v1_path_file(self, mock_provider, file_wb_path):
        file_path = '/{}'.format(file_wb_path.path)
        assert file_path.startswith('/') and not file_path.endswith('/')
        wb_path = await mock_provider.validate_path(file_path)
        assert wb_path == file_wb_path

    @pytest.mark.asyncio
    async def test_validate_v1_path_folder(self, mock_provider, folder_wb_path):
        folder_path = '/{}'.format(folder_wb_path.path)
        assert folder_path.startswith('/') and folder_path.endswith('/')
        wb_path = await mock_provider.validate_path(folder_path)
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
    async def test_metadata_file(
            self,
            mock_time,
            mock_provider,
            file_wb_path,
            meta_file_raw,
            meta_file_parsed
    ):

        file_obj_name = utils.get_obj_name(file_wb_path, is_folder=False)
        signed_url = mock_provider._build_and_sign_url('HEAD', file_obj_name, **{})

        resp_headers = MultiDict(json.loads(meta_file_raw))
        google_hash = resp_headers.get('x-goog-hash', None)
        resp_headers.pop('x-goog-hash')
        google_hash_list = google_hash.split(',')
        for google_hash in google_hash_list:
            resp_headers.add('x-goog-hash', google_hash)
        metadata_json = json.loads(meta_file_parsed)

        aiohttpretty.register_uri(
            'HEAD',
            signed_url,
            headers=resp_headers,
            status=HTTPStatus.OK
        )

        metadata = await mock_provider._metadata_object(file_wb_path, is_folder=False)
        metadata_expected = GoogleCloudFileMetadata(metadata_json)
        assert isinstance(metadata, GoogleCloudFileMetadata)
        assert metadata == metadata_expected


    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_object_401_unauthorized(
            self,
            mock_time,
            file_wb_path,
            mock_provider
    ):
        file_obj_name = utils.get_obj_name(file_wb_path, is_folder=False)
        signed_url = mock_provider._build_and_sign_url('HEAD', file_obj_name, **{})

        aiohttpretty.register_uri(
            'HEAD',
            signed_url,
            status=HTTPStatus.UNAUTHORIZED
        )

        with pytest.raises(core_exceptions.MetadataError) as exc:
            await mock_provider._metadata_object(file_wb_path, is_folder=False)

        assert exc.value.code == HTTPStatus.UNAUTHORIZED

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_object_404_not_found(
            self,
            mock_time,
            file_wb_path,
            mock_provider,
    ):
        file_obj_name = utils.get_obj_name(file_wb_path, is_folder=False)
        signed_url = mock_provider._build_and_sign_url('HEAD', file_obj_name, **{})

        aiohttpretty.register_uri(
            'HEAD',
            signed_url,
            status=HTTPStatus.NOT_FOUND
        )

        with pytest.raises(core_exceptions.MetadataError) as exc:
            await mock_provider._metadata_object(file_wb_path, is_folder=False)

        assert exc.value.code == HTTPStatus.NOT_FOUND


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
