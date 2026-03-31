import base64
import hashlib
import io
from http import HTTPStatus
from unittest import mock

import pytest

from tests.providers.oraclecloud.fixtures.providers import (
    mock_auth,
    mock_creds,
    mock_settings,
)
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath
from waterbutler.core.streams import StringStream
from waterbutler.providers.oraclecloud import OracleCloudProvider
from waterbutler.providers.oraclecloud.metadata import (
    OracleCloudFileMetadata,
    OracleCloudFolderMetadata,
)


@pytest.fixture()
def mock_provider(mock_auth, mock_creds, mock_settings):
    with mock.patch("oci.config.validate_config"):
        with mock.patch("oci.object_storage.ObjectStorageClient"):
            provider = OracleCloudProvider(mock_auth, mock_creds, mock_settings)
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


def _make_head_response(
    content_length=1024,
    content_type="text/plain",
    etag="abc123",
    md5="rL0Y20zC+Fzt72VPzMSk2A==",
):
    resp = mock.Mock()
    resp.headers = {
        "content-type": content_type,
        "content-length": str(content_length),
        "last-modified": "Thu, 01 Mar 2025 19:04:45 GMT",
        "etag": f'"{etag}"',
        "opc-content-md5": md5,
        "storage-tier": "Standard",
    }
    return resp


class TestProviderInit:

    def test_provider_init(self, mock_provider, mock_settings):

        assert mock_provider is not None
        assert mock_provider.NAME == "oraclecloud"
        assert mock_provider.bucket == mock_settings.get("bucket")
        assert mock_provider.namespace == mock_settings.get("namespace")

    def test_provider_init_missing_bucket(self, mock_auth, mock_creds):

        with pytest.raises(exceptions.InvalidProviderConfigError):
            with mock.patch("oci.config.validate_config"):
                with mock.patch("oci.object_storage.ObjectStorageClient"):
                    OracleCloudProvider(mock_auth, mock_creds, {"namespace": "ns"})

    def test_provider_init_missing_namespace(self, mock_auth, mock_creds):

        with pytest.raises(exceptions.InvalidProviderConfigError):
            with mock.patch("oci.config.validate_config"):
                with mock.patch("oci.object_storage.ObjectStorageClient"):
                    OracleCloudProvider(mock_auth, mock_creds, {"bucket": "bkt"})

    def test_provider_init_missing_credentials(self, mock_auth, mock_settings):

        incomplete_creds = {"oci_user": "ocid1.user.oc1..fake"}
        with pytest.raises(exceptions.InvalidProviderConfigError):
            with mock.patch("oci.config.validate_config"):
                with mock.patch("oci.object_storage.ObjectStorageClient"):
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
    async def test_metadata_file(self, mock_provider, file_wb_path):

        head_resp = _make_head_response()
        mock_provider._client.head_object.return_value = head_resp

        metadata = await mock_provider.metadata(file_wb_path)

        assert isinstance(metadata, OracleCloudFileMetadata)
        assert metadata.name == "text-file-1.txt"
        assert metadata.size == 1024
        mock_provider._client.head_object.assert_called_once_with(
            "test-namespace", "test-bucket", "folder-1/text-file-1.txt"
        )

    @pytest.mark.asyncio
    async def test_metadata_file_not_found(self, mock_provider, file_wb_path):

        import oci

        mock_provider._client.head_object.side_effect = oci.exceptions.ServiceError(
            status=404, code="ObjectNotFound", headers={}, message="Not Found"
        )

        with pytest.raises(exceptions.NotFoundError):
            await mock_provider.metadata(file_wb_path)

    @pytest.mark.asyncio
    async def test_metadata_folder(self, mock_provider, folder_wb_path):

        list_resp = mock.Mock()
        list_resp.data.prefixes = ["folder-1/subfolder/"]
        obj_summary = mock.Mock()
        obj_summary.name = "folder-1/file.txt"
        obj_summary.size = 512
        obj_summary.etag = "etag1"
        obj_summary.md5 = "md5hash"
        obj_summary.storage_tier = "Standard"
        obj_summary.archival_state = None
        obj_summary.time_modified = mock.Mock()
        obj_summary.time_modified.isoformat.return_value = "2025-03-01T00:00:00+00:00"
        obj_summary.time_created = None
        list_resp.data.objects = [obj_summary]

        mock_provider._client.list_objects.return_value = list_resp

        items = await mock_provider.metadata(folder_wb_path)

        assert len(items) == 2
        assert isinstance(items[0], OracleCloudFolderMetadata)
        assert isinstance(items[1], OracleCloudFileMetadata)
        assert items[0].name == "subfolder"
        assert items[1].name == "file.txt"


class TestCRUD:

    @pytest.mark.asyncio
    async def test_download_file(self, mock_provider, file_wb_path, file_content):

        get_resp = mock.Mock()
        get_resp.data.content = file_content
        mock_provider._client.get_object.return_value = get_resp

        stream = await mock_provider.download(file_wb_path)

        assert isinstance(stream, StringStream)
        data = await stream.read()
        assert data == file_content
        mock_provider._client.get_object.assert_called_once_with(
            "test-namespace", "test-bucket", "folder-1/text-file-1.txt"
        )

    @pytest.mark.asyncio
    async def test_download_folder_raises(self, mock_provider, folder_wb_path):

        with pytest.raises(exceptions.DownloadError):
            await mock_provider.download(folder_wb_path)

    @pytest.mark.asyncio
    async def test_download_not_found(self, mock_provider, file_wb_path):

        import oci

        mock_provider._client.get_object.side_effect = oci.exceptions.ServiceError(
            status=404, code="ObjectNotFound", headers={}, message="Not Found"
        )

        with pytest.raises(exceptions.DownloadError) as exc:
            await mock_provider.download(file_wb_path)
        assert exc.value.code == HTTPStatus.NOT_FOUND

    @pytest.mark.asyncio
    async def test_upload_file(self, mock_provider, file_wb_path, file_content):

        # exists check: head_object raises 404 => file is new
        import oci

        mock_provider._client.head_object.side_effect = [
            oci.exceptions.ServiceError(
                status=404, code="ObjectNotFound", headers={}, message="Not Found"
            ),
            _make_head_response(content_length=len(file_content)),
        ]

        content_md5 = base64.b64encode(hashlib.md5(file_content).digest()).decode()
        put_resp = mock.Mock()
        put_resp.headers = {"opc-content-md5": content_md5, "etag": "newetag"}
        mock_provider._client.put_object.return_value = put_resp

        metadata, created = await mock_provider.upload(
            StringStream(file_content), file_wb_path
        )

        assert created is True
        assert isinstance(metadata, OracleCloudFileMetadata)
        mock_provider._client.put_object.assert_called_once()

    @pytest.mark.asyncio
    async def test_upload_checksum_mismatch(
        self, mock_provider, file_wb_path, file_content
    ):

        import oci

        mock_provider._client.head_object.side_effect = oci.exceptions.ServiceError(
            status=404, code="ObjectNotFound", headers={}, message="Not Found"
        )

        put_resp = mock.Mock()
        put_resp.headers = {"opc-content-md5": "bWlzbWF0Y2g=", "etag": "newetag"}
        mock_provider._client.put_object.return_value = put_resp

        with pytest.raises(exceptions.UploadChecksumMismatchError):
            await mock_provider.upload(StringStream(file_content), file_wb_path)

    @pytest.mark.asyncio
    async def test_delete_file(self, mock_provider, file_wb_path):

        mock_provider._client.delete_object.return_value = mock.Mock()

        await mock_provider.delete(file_wb_path)

        mock_provider._client.delete_object.assert_called_once_with(
            "test-namespace", "test-bucket", "folder-1/text-file-1.txt"
        )

    @pytest.mark.asyncio
    async def test_delete_folder_raises(self, mock_provider, folder_wb_path):

        with pytest.raises(exceptions.DeleteError):
            await mock_provider.delete(folder_wb_path)

    @pytest.mark.asyncio
    async def test_delete_not_found(self, mock_provider, file_wb_path):

        import oci

        mock_provider._client.delete_object.side_effect = oci.exceptions.ServiceError(
            status=404, code="ObjectNotFound", headers={}, message="Not Found"
        )

        with pytest.raises(exceptions.NotFoundError):
            await mock_provider.delete(file_wb_path)
