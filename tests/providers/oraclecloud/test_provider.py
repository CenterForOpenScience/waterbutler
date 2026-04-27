import hashlib
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
from waterbutler.core.streams import ResponseStreamReader, StringStream
from waterbutler.providers.oraclecloud import OracleCloudProvider
from waterbutler.providers.oraclecloud.metadata import (
    OracleCloudFileMetadata,
    OracleCloudFolderMetadata,
)


@pytest.fixture()
def mock_provider(mock_auth, mock_creds, mock_settings):
    return OracleCloudProvider(mock_auth, mock_creds, mock_settings)


@pytest.fixture()
def file_wb_path():
    return WaterButlerPath("/folder-1/text-file-1.txt")


@pytest.fixture()
def folder_wb_path():
    return WaterButlerPath("/folder-1/")


@pytest.fixture()
def file_content():
    return b"file content for testing upload and download"


def _mock_response(status=200, headers=None, body=b"", text=""):
    """Build a mock aiohttp-style response for ``make_request``."""
    resp = mock.AsyncMock()
    resp.status = status
    resp.headers = headers or {}
    resp.read = mock.AsyncMock(return_value=body)
    resp.text = mock.AsyncMock(
        return_value=text or body.decode("utf-8", errors="replace")
    )
    resp.release = mock.AsyncMock()
    resp.close = mock.Mock()
    # ResponseStreamReader reads from response.content
    content = mock.AsyncMock()
    content.read = mock.AsyncMock(return_value=body)
    resp.content = content
    return resp


class TestProviderInit:

    def test_provider_init(self, mock_provider, mock_settings):

        assert mock_provider is not None
        assert mock_provider.NAME == "oraclecloud"
        assert mock_provider.bucket == mock_settings["bucket"]
        assert "compat.objectstorage" in mock_provider.BASE_URL
        assert "us-ashburn-1" in mock_provider.BASE_URL
        assert "test-namespace" in mock_provider.BASE_URL

    def test_provider_init_missing_bucket(self, mock_auth, mock_creds):

        with pytest.raises(exceptions.InvalidProviderConfigError):
            OracleCloudProvider(mock_auth, mock_creds, {"namespace": "ns"})

    def test_provider_init_missing_namespace(self, mock_auth, mock_creds):

        with pytest.raises(exceptions.InvalidProviderConfigError):
            OracleCloudProvider(mock_auth, mock_creds, {"bucket": "bkt"})

    def test_provider_init_missing_access_key(self, mock_auth, mock_settings):

        creds = {"secret_key": "sk", "region": "us-ashburn-1"}
        with pytest.raises(exceptions.InvalidProviderConfigError):
            OracleCloudProvider(mock_auth, creds, mock_settings)

    def test_provider_init_missing_secret_key(self, mock_auth, mock_settings):

        creds = {"access_key": "ak", "region": "us-ashburn-1"}
        with pytest.raises(exceptions.InvalidProviderConfigError):
            OracleCloudProvider(mock_auth, creds, mock_settings)

    def test_provider_init_missing_region(self, mock_auth, mock_settings):

        creds = {"access_key": "ak", "secret_key": "sk"}
        with pytest.raises(exceptions.InvalidProviderConfigError):
            OracleCloudProvider(mock_auth, creds, mock_settings)


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

    def test_can_intra_copy_file(self, mock_provider, file_wb_path):
        assert mock_provider.can_intra_copy(mock_provider, file_wb_path)

    def test_can_intra_copy_folder(self, mock_provider, folder_wb_path):
        assert not mock_provider.can_intra_copy(mock_provider, folder_wb_path)

    def test_can_intra_move_file(self, mock_provider, file_wb_path):
        assert mock_provider.can_intra_move(mock_provider, file_wb_path)

    def test_can_intra_move_folder(self, mock_provider, folder_wb_path):
        assert not mock_provider.can_intra_move(mock_provider, folder_wb_path)


class TestMetadata:

    @pytest.mark.asyncio
    async def test_metadata_file(self, mock_provider, file_wb_path):

        head_resp = _mock_response(
            status=200,
            headers={
                "Content-Type": "text/plain",
                "Content-Length": "1024",
                "ETag": '"abc123"',
                "Last-Modified": "Thu, 01 Mar 2025 19:04:45 GMT",
            },
        )

        with mock.patch.object(
            mock_provider,
            "make_request",
            new_callable=mock.AsyncMock,
            return_value=head_resp,
        ):
            metadata = await mock_provider.metadata(file_wb_path)

        assert isinstance(metadata, OracleCloudFileMetadata)
        assert metadata.name == "text-file-1.txt"
        assert metadata.size == 1024
        assert metadata.etag == "abc123"

    @pytest.mark.asyncio
    async def test_metadata_file_not_found(self, mock_provider, file_wb_path):

        head_resp = _mock_response(status=404)

        with mock.patch.object(
            mock_provider,
            "make_request",
            new_callable=mock.AsyncMock,
            return_value=head_resp,
        ):
            with pytest.raises(exceptions.NotFoundError):
                await mock_provider.metadata(file_wb_path)

    @pytest.mark.asyncio
    async def test_metadata_folder(self, mock_provider, folder_wb_path):

        xml_body = """<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult>
            <CommonPrefixes><Prefix>folder-1/subfolder/</Prefix></CommonPrefixes>
            <Contents>
                <Key>folder-1/file.txt</Key>
                <LastModified>2025-03-01T00:00:00.000Z</LastModified>
                <ETag>"etag1"</ETag>
                <Size>512</Size>
            </Contents>
        </ListBucketResult>"""

        list_resp = _mock_response(status=200, text=xml_body)

        with mock.patch.object(
            mock_provider,
            "make_request",
            new_callable=mock.AsyncMock,
            return_value=list_resp,
        ):
            items = await mock_provider.metadata(folder_wb_path)

        assert len(items) == 2
        assert isinstance(items[0], OracleCloudFolderMetadata)
        assert isinstance(items[1], OracleCloudFileMetadata)
        assert items[0].name == "subfolder"
        assert items[1].name == "file.txt"
        assert items[1].size == 512

    @pytest.mark.asyncio
    async def test_metadata_folder_empty(self, mock_provider, folder_wb_path):

        xml_body = """<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult>
            <Name>test-bucket</Name>
        </ListBucketResult>"""

        list_resp = _mock_response(status=200, text=xml_body)

        with mock.patch.object(
            mock_provider,
            "make_request",
            new_callable=mock.AsyncMock,
            return_value=list_resp,
        ):
            items = await mock_provider.metadata(folder_wb_path)

        assert items == []

    @pytest.mark.asyncio
    async def test_metadata_folder_multiple_files(self, mock_provider, folder_wb_path):

        xml_body = """<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult>
            <Contents>
                <Key>folder-1/a.txt</Key>
                <LastModified>2025-03-01T00:00:00.000Z</LastModified>
                <ETag>"e1"</ETag>
                <Size>100</Size>
            </Contents>
            <Contents>
                <Key>folder-1/b.txt</Key>
                <LastModified>2025-03-02T00:00:00.000Z</LastModified>
                <ETag>"e2"</ETag>
                <Size>200</Size>
            </Contents>
        </ListBucketResult>"""

        list_resp = _mock_response(status=200, text=xml_body)

        with mock.patch.object(
            mock_provider,
            "make_request",
            new_callable=mock.AsyncMock,
            return_value=list_resp,
        ):
            items = await mock_provider.metadata(folder_wb_path)

        assert len(items) == 2
        assert all(isinstance(i, OracleCloudFileMetadata) for i in items)


class TestCRUD:

    @pytest.mark.asyncio
    async def test_download_file(self, mock_provider, file_wb_path, file_content):

        get_resp = _mock_response(
            status=200,
            body=file_content,
            headers={"Content-Length": str(len(file_content))},
        )

        with mock.patch.object(
            mock_provider,
            "make_request",
            new_callable=mock.AsyncMock,
            return_value=get_resp,
        ):
            stream = await mock_provider.download(file_wb_path)

        assert isinstance(stream, ResponseStreamReader)

    @pytest.mark.asyncio
    async def test_download_folder_raises(self, mock_provider, folder_wb_path):

        with pytest.raises(exceptions.DownloadError):
            await mock_provider.download(folder_wb_path)

    @pytest.mark.asyncio
    async def test_download_not_found(self, mock_provider, file_wb_path):

        get_resp = _mock_response(status=404)

        with mock.patch.object(
            mock_provider,
            "make_request",
            new_callable=mock.AsyncMock,
            return_value=get_resp,
        ):
            with pytest.raises(exceptions.DownloadError) as exc:
                await mock_provider.download(file_wb_path)
            assert exc.value.code == HTTPStatus.NOT_FOUND

    @pytest.mark.asyncio
    async def test_download_with_range(self, mock_provider, file_wb_path):

        get_resp = _mock_response(
            status=206,
            body=b"partial",
            headers={"Content-Length": "7"},
        )

        with mock.patch.object(
            mock_provider,
            "make_request",
            new_callable=mock.AsyncMock,
            return_value=get_resp,
        ) as mocked:
            stream = await mock_provider.download(file_wb_path, range=(0, 6))

        assert isinstance(stream, ResponseStreamReader)
        # Verify Range header was included in the signed headers
        call_kwargs = mocked.call_args
        assert "Range" in call_kwargs.kwargs["headers"]

    @pytest.mark.asyncio
    async def test_upload_file(self, mock_provider, file_wb_path, file_content):

        expected_md5 = hashlib.md5(file_content).hexdigest()

        # exists check (HEAD) returns 404 -> file is new
        head_404 = _mock_response(status=404)
        # PUT returns 200 with ETag
        put_resp = _mock_response(
            status=200, headers={"ETag": f'"{expected_md5}"'}
        )
        # post-upload metadata fetch (HEAD) returns 200
        head_200 = _mock_response(
            status=200,
            headers={
                "Content-Type": "application/octet-stream",
                "Content-Length": str(len(file_content)),
                "ETag": f'"{expected_md5}"',
                "Last-Modified": "Thu, 01 Mar 2025 19:04:45 GMT",
            },
        )

        with mock.patch.object(
            mock_provider,
            "make_request",
            new_callable=mock.AsyncMock,
            side_effect=[head_404, put_resp, head_200],
        ):
            metadata, created = await mock_provider.upload(
                StringStream(file_content), file_wb_path
            )

        assert created is True
        assert isinstance(metadata, OracleCloudFileMetadata)
        assert metadata.name == "text-file-1.txt"

    @pytest.mark.asyncio
    async def test_upload_existing_file(self, mock_provider, file_wb_path, file_content):

        expected_md5 = hashlib.md5(file_content).hexdigest()

        # exists check (HEAD) returns 200 -> file already exists
        head_exists = _mock_response(
            status=200,
            headers={
                "Content-Type": "text/plain",
                "Content-Length": "100",
                "ETag": '"oldmd5"',
                "Last-Modified": "Thu, 01 Mar 2025 00:00:00 GMT",
            },
        )
        # PUT returns 200
        put_resp = _mock_response(
            status=200, headers={"ETag": f'"{expected_md5}"'}
        )
        # post-upload metadata fetch
        head_200 = _mock_response(
            status=200,
            headers={
                "Content-Type": "application/octet-stream",
                "Content-Length": str(len(file_content)),
                "ETag": f'"{expected_md5}"',
                "Last-Modified": "Thu, 01 Mar 2025 19:04:45 GMT",
            },
        )

        with mock.patch.object(
            mock_provider,
            "make_request",
            new_callable=mock.AsyncMock,
            side_effect=[head_exists, put_resp, head_200],
        ):
            metadata, created = await mock_provider.upload(
                StringStream(file_content), file_wb_path
            )

        assert created is False
        assert isinstance(metadata, OracleCloudFileMetadata)

    @pytest.mark.asyncio
    async def test_upload_checksum_mismatch(
        self, mock_provider, file_wb_path, file_content
    ):

        # exists check returns 404
        head_404 = _mock_response(status=404)
        # PUT returns 200 with wrong ETag
        put_resp = _mock_response(
            status=200, headers={"ETag": '"mismatch"'}
        )

        with mock.patch.object(
            mock_provider,
            "make_request",
            new_callable=mock.AsyncMock,
            side_effect=[head_404, put_resp],
        ):
            with pytest.raises(exceptions.UploadChecksumMismatchError):
                await mock_provider.upload(StringStream(file_content), file_wb_path)

    @pytest.mark.asyncio
    async def test_delete_file(self, mock_provider, file_wb_path):

        del_resp = _mock_response(status=204)

        with mock.patch.object(
            mock_provider,
            "make_request",
            new_callable=mock.AsyncMock,
            return_value=del_resp,
        ):
            await mock_provider.delete(file_wb_path)

    @pytest.mark.asyncio
    async def test_delete_folder_raises(self, mock_provider, folder_wb_path):

        with pytest.raises(exceptions.DeleteError):
            await mock_provider.delete(folder_wb_path)


class TestURLBuilding:

    def test_object_url(self, mock_provider):
        url = mock_provider._object_url("folder/file.txt")
        assert url == (
            "https://test-namespace.compat.objectstorage"
            ".us-ashburn-1.oraclecloud.com/test-bucket/folder/file.txt"
        )

    def test_object_url_encodes_special_chars(self, mock_provider):
        url = mock_provider._object_url("folder/file name.txt")
        assert "file%20name.txt" in url

    def test_bucket_url_no_query(self, mock_provider):
        url = mock_provider._bucket_url()
        assert url.endswith("/test-bucket")

    def test_bucket_url_with_query(self, mock_provider):
        url = mock_provider._bucket_url(**{"list-type": "2", "delimiter": "/"})
        assert "list-type=2" in url
        assert "delimiter=%2F" in url

    def test_get_obj_name(self, mock_provider):
        path = WaterButlerPath("/folder/file.txt")
        assert mock_provider._get_obj_name(path) == "folder/file.txt"

    def test_get_obj_name_root(self, mock_provider):
        path = WaterButlerPath("/")
        assert mock_provider._get_obj_name(path) == ""
