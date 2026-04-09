import base64
import hashlib
import json
from http import HTTPStatus
from unittest import mock

import aiohttpretty
import pytest

from tests.providers.oraclecloud.fixtures.providers import (
    mock_auth,
    mock_creds,
    mock_settings,
)
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath
from waterbutler.core.streams import ResponseStreamReader, StringStream
from waterbutler.providers.oraclecloud import OracleCloudProvider
from waterbutler.providers.oraclecloud.metadata import (
    OracleCloudFileMetadata,
    OracleCloudFolderMetadata,
)

BASE_URL = "https://objectstorage.us-ashburn-1.oraclecloud.com"


@pytest.fixture()
def mock_provider(mock_auth, mock_creds, mock_settings):
    with mock.patch(
        "waterbutler.providers.oraclecloud.signing.load_private_key"
    ) as mock_load:
        mock_load.return_value = mock.Mock()
        provider = OracleCloudProvider(mock_auth, mock_creds, mock_settings)
    # bypass actual RSA signing in unit tests
    provider._signer.sign_request = lambda method, url, headers, body=None: headers
    return provider


@pytest.fixture()
def file_wb_path():
    return WaterButlerPath("/folder-1/text-file-1.txt")


@pytest.fixture()
def folder_wb_path():
    return WaterButlerPath("/folder-1/")


@pytest.fixture()
def file_content():
    return b"file content for testing upload and download"


def _head_headers(content_length=1024, content_type="text/plain",
                  etag="abc123", md5="rL0Y20zC+Fzt72VPzMSk2A=="):
    return {
        "content-type": content_type,
        "content-length": str(content_length),
        "last-modified": "Thu, 01 Mar 2025 19:04:45 GMT",
        "etag": f'"{etag}"',
        "opc-content-md5": md5,
        "storage-tier": "Standard",
    }


def _obj_url(obj_name):
    return f"{BASE_URL}/n/test-namespace/b/test-bucket/o/{obj_name}"


def _list_url(prefix=""):
    base = f"{BASE_URL}/n/test-namespace/b/test-bucket/o"
    fields = "name%2Csize%2Cetag%2Cmd5%2CtimeCreated%2CtimeModified%2CstorageTier%2CarchivalState"
    url = f"{base}?fields={fields}"
    if prefix:
        url += f"&prefix={prefix}"
    url += "&delimiter=%2F"
    return url


class TestProviderInit:

    def test_provider_init(self, mock_provider, mock_settings):
        assert mock_provider is not None
        assert mock_provider.NAME == "oraclecloud"
        assert mock_provider.bucket == mock_settings.get("bucket")
        assert mock_provider.namespace == mock_settings.get("namespace")

    def test_provider_init_missing_bucket(self, mock_auth, mock_creds):
        with pytest.raises(exceptions.InvalidProviderConfigError):
            with mock.patch(
                "waterbutler.providers.oraclecloud.signing.load_private_key"
            ):
                OracleCloudProvider(mock_auth, mock_creds, {"namespace": "ns"})

    def test_provider_init_missing_namespace(self, mock_auth, mock_creds):
        with pytest.raises(exceptions.InvalidProviderConfigError):
            with mock.patch(
                "waterbutler.providers.oraclecloud.signing.load_private_key"
            ):
                OracleCloudProvider(mock_auth, mock_creds, {"bucket": "bkt"})

    def test_provider_init_missing_credentials(self, mock_auth, mock_settings):
        incomplete_creds = {"oci_user": "ocid1.user.oc1..fake"}
        with pytest.raises(exceptions.InvalidProviderConfigError):
            with mock.patch(
                "waterbutler.providers.oraclecloud.signing.load_private_key"
            ):
                OracleCloudProvider(mock_auth, incomplete_creds, mock_settings)


class TestValidatePath:

    @pytest.mark.asyncio
    async def test_validate_v1_path_file(self, mock_provider, file_wb_path):
        wb_path = await mock_provider.validate_v1_path("/folder-1/text-file-1.txt")
        assert wb_path == file_wb_path

    @pytest.mark.asyncio
    async def test_validate_v1_path_folder(self, mock_provider, folder_wb_path):
        wb_path = await mock_provider.validate_v1_path("/folder-1/")
        assert wb_path == folder_wb_path

    @pytest.mark.asyncio
    async def test_validate_path_file(self, mock_provider, file_wb_path):
        wb_path = await mock_provider.validate_path("/folder-1/text-file-1.txt")
        assert wb_path == file_wb_path


class TestOperations:

    def test_can_duplicate_names(self, mock_provider):
        assert mock_provider.can_duplicate_names()

    def test_can_intra_copy(self, mock_provider):
        assert not mock_provider.can_intra_copy(mock_provider)

    def test_can_intra_move(self, mock_provider):
        assert not mock_provider.can_intra_move(mock_provider)


class TestMetadata:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file(self, mock_provider, file_wb_path):
        url = _obj_url("folder-1/text-file-1.txt")
        aiohttpretty.register_uri(
            "HEAD", url, headers=_head_headers(), status=HTTPStatus.OK
        )

        metadata = await mock_provider.metadata(file_wb_path)

        assert isinstance(metadata, OracleCloudFileMetadata)
        assert metadata.name == "text-file-1.txt"
        assert metadata.size == 1024
        assert aiohttpretty.has_call(method="HEAD", uri=url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file_not_found(self, mock_provider, file_wb_path):
        url = _obj_url("folder-1/text-file-1.txt")
        aiohttpretty.register_uri("HEAD", url, status=HTTPStatus.NOT_FOUND)

        with pytest.raises(exceptions.MetadataError):
            await mock_provider.metadata(file_wb_path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder(self, mock_provider, folder_wb_path):
        url = _list_url(prefix="folder-1/")
        body = json.dumps({
            "prefixes": ["folder-1/subfolder/"],
            "objects": [
                {
                    "name": "folder-1/file.txt",
                    "size": 512,
                    "etag": "etag1",
                    "md5": "md5hash",
                    "storageTier": "Standard",
                    "archivalState": None,
                    "timeModified": "2025-03-01T00:00:00+00:00",
                    "timeCreated": None,
                }
            ],
        }).encode()

        aiohttpretty.register_uri(
            "GET", url, body=body,
            headers={"Content-Type": "application/json"},
            status=HTTPStatus.OK,
        )

        items = await mock_provider.metadata(folder_wb_path)

        assert len(items) == 2
        assert isinstance(items[0], OracleCloudFolderMetadata)
        assert isinstance(items[1], OracleCloudFileMetadata)
        assert items[0].name == "subfolder"
        assert items[1].name == "file.txt"


class TestCRUD:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_file(self, mock_provider, file_wb_path, file_content):
        url = _obj_url("folder-1/text-file-1.txt")
        aiohttpretty.register_uri("GET", url, body=file_content, status=HTTPStatus.OK)

        stream = await mock_provider.download(file_wb_path)

        assert isinstance(stream, ResponseStreamReader)
        data = await stream.read()
        assert data == file_content
        assert aiohttpretty.has_call(method="GET", uri=url)

    @pytest.mark.asyncio
    async def test_download_folder_raises(self, mock_provider, folder_wb_path):
        with pytest.raises(exceptions.DownloadError):
            await mock_provider.download(folder_wb_path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_not_found(self, mock_provider, file_wb_path):
        url = _obj_url("folder-1/text-file-1.txt")
        aiohttpretty.register_uri("GET", url, status=HTTPStatus.NOT_FOUND)

        with pytest.raises(exceptions.DownloadError):
            await mock_provider.download(file_wb_path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_file(self, mock_provider, file_wb_path, file_content):
        obj_url = _obj_url("folder-1/text-file-1.txt")
        content_md5 = base64.b64encode(hashlib.md5(file_content).digest()).decode()

        # HEAD 404 for the exists() check, then HEAD 200 for post-upload metadata
        aiohttpretty.register_uri("HEAD", obj_url, responses=[
            {"status": HTTPStatus.NOT_FOUND},
            {"status": HTTPStatus.OK, "headers": _head_headers(
                content_length=len(file_content),
            )},
        ])

        aiohttpretty.register_uri(
            "PUT", obj_url, status=HTTPStatus.OK,
            headers={"opc-content-md5": content_md5, "etag": "newetag"},
        )

        metadata, created = await mock_provider.upload(
            StringStream(file_content), file_wb_path
        )

        assert created is True
        assert isinstance(metadata, OracleCloudFileMetadata)
        assert aiohttpretty.has_call(method="PUT", uri=obj_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_checksum_mismatch(self, mock_provider, file_wb_path, file_content):
        obj_url = _obj_url("folder-1/text-file-1.txt")

        aiohttpretty.register_uri("HEAD", obj_url, status=HTTPStatus.NOT_FOUND)
        aiohttpretty.register_uri(
            "PUT", obj_url, status=HTTPStatus.OK,
            headers={"opc-content-md5": "bWlzbWF0Y2g=", "etag": "newetag"},
        )

        with pytest.raises(exceptions.UploadChecksumMismatchError):
            await mock_provider.upload(StringStream(file_content), file_wb_path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_file(self, mock_provider, file_wb_path):
        url = _obj_url("folder-1/text-file-1.txt")
        aiohttpretty.register_uri("DELETE", url, status=HTTPStatus.NO_CONTENT)

        await mock_provider.delete(file_wb_path)

        assert aiohttpretty.has_call(method="DELETE", uri=url)

    @pytest.mark.asyncio
    async def test_delete_folder_raises(self, mock_provider, folder_wb_path):
        with pytest.raises(exceptions.DeleteError):
            await mock_provider.delete(folder_wb_path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_not_found(self, mock_provider, file_wb_path):
        url = _obj_url("folder-1/text-file-1.txt")
        aiohttpretty.register_uri("DELETE", url, status=HTTPStatus.NOT_FOUND)

        with pytest.raises(exceptions.DeleteError):
            await mock_provider.delete(file_wb_path)
