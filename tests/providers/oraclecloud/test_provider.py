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
from waterbutler.providers.oraclecloud.metadata import OracleCloudFileMetadata


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


def _request_sequence(items):
    """``make_request`` side effect that drains any streamed body, then yields each
    queued response in order (an item may be an exception to raise instead).
    """
    queue = list(items)

    async def _side_effect(method, url, *args, **kwargs):
        data = kwargs.get("data")
        if data is not None and hasattr(data, "read"):
            # Drain in positive-size chunks the way aiohttp does, so tee'd hash
            # writers see the body exactly once.
            while await data.read(8192):
                pass
        item = queue.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    return _side_effect


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

        creds = {"json_creds": {"s3compat": {"secret_key": "sk", "region": "us-ashburn-1"}}}
        with pytest.raises(exceptions.InvalidProviderConfigError):
            OracleCloudProvider(mock_auth, creds, mock_settings)

    def test_provider_init_missing_secret_key(self, mock_auth, mock_settings):

        creds = {"json_creds": {"s3compat": {"access_key": "ak", "region": "us-ashburn-1"}}}
        with pytest.raises(exceptions.InvalidProviderConfigError):
            OracleCloudProvider(mock_auth, creds, mock_settings)

    def test_provider_init_missing_region(self, mock_auth, mock_settings):

        creds = {"json_creds": {"s3compat": {"access_key": "ak", "secret_key": "sk"}}}
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

    def test_can_intra_copy_other_provider(self, mock_provider, file_wb_path):
        assert not mock_provider.can_intra_copy(mock.Mock(), file_wb_path)

    def test_can_intra_copy_different_endpoint(
        self, mock_provider, mock_auth, mock_creds, mock_settings, file_wb_path
    ):
        other = OracleCloudProvider(
            mock_auth, mock_creds, {**mock_settings, "namespace": "other-ns"}
        )
        assert not mock_provider.can_intra_copy(other, file_wb_path)


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

        with mock.patch.object(
            mock_provider,
            "make_request",
            new_callable=mock.AsyncMock,
            side_effect=exceptions.MetadataError("not found", code=HTTPStatus.NOT_FOUND),
        ):
            with pytest.raises(exceptions.MetadataError):
                await mock_provider.metadata(file_wb_path)

    @pytest.mark.asyncio
    async def test_metadata_folder_raises(self, mock_provider, folder_wb_path):

        with pytest.raises(exceptions.MetadataError):
            await mock_provider.metadata(folder_wb_path)


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

        # workaround: for small files, we slurp into memory and return a StringStream
        # assert isinstance(stream, ResponseStreamReader)
        assert isinstance(stream, StringStream)

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

        # workaround: for small files, we slurp into memory and return a StringStream
        # assert isinstance(stream, ResponseStreamReader)
        assert isinstance(stream, StringStream)

        # Verify Range header was included in the signed headers
        call_kwargs = mocked.call_args
        assert call_kwargs.kwargs["headers"]["Range"] == "bytes=0-6"

    @pytest.mark.asyncio
    async def test_download_open_ended_range(self, mock_provider, file_wb_path):

        get_resp = _mock_response(
            status=206, body=b"tail", headers={"Content-Length": "4"}
        )

        with mock.patch.object(
            mock_provider,
            "make_request",
            new_callable=mock.AsyncMock,
            return_value=get_resp,
        ) as mocked:
            await mock_provider.download(file_wb_path, range=(5, None))

        assert mocked.call_args.kwargs["headers"]["Range"] == "bytes=5-"

    @pytest.mark.asyncio
    async def test_upload_file(self, mock_provider, file_wb_path, file_content):

        expected_md5 = hashlib.md5(file_content).hexdigest()
        put_resp = _mock_response(status=200, headers={"ETag": f'"{expected_md5}"'})
        head_200 = _mock_response(
            status=200,
            headers={
                "Content-Type": "application/octet-stream",
                "Content-Length": str(len(file_content)),
                "ETag": f'"{expected_md5}"',
                "Last-Modified": "Thu, 01 Mar 2025 19:04:45 GMT",
            },
        )
        # exists() HEAD raises 404, PUT succeeds, post-upload HEAD returns metadata
        sequence = _request_sequence([
            exceptions.MetadataError("not found", code=HTTPStatus.NOT_FOUND),
            put_resp,
            head_200,
        ])

        with mock.patch.object(mock_provider, "make_request", side_effect=sequence):
            metadata, created = await mock_provider.upload(
                StringStream(file_content), file_wb_path
            )

        assert created is True
        assert isinstance(metadata, OracleCloudFileMetadata)
        assert metadata.name == "text-file-1.txt"
        assert metadata.etag == expected_md5

    @pytest.mark.asyncio
    async def test_upload_existing_file(self, mock_provider, file_wb_path, file_content):

        expected_md5 = hashlib.md5(file_content).hexdigest()
        head_exists = _mock_response(
            status=200,
            headers={
                "Content-Type": "text/plain",
                "Content-Length": "100",
                "ETag": '"oldmd5"',
                "Last-Modified": "Thu, 01 Mar 2025 00:00:00 GMT",
            },
        )
        put_resp = _mock_response(status=200, headers={"ETag": f'"{expected_md5}"'})
        head_200 = _mock_response(
            status=200,
            headers={
                "Content-Type": "application/octet-stream",
                "Content-Length": str(len(file_content)),
                "ETag": f'"{expected_md5}"',
                "Last-Modified": "Thu, 01 Mar 2025 19:04:45 GMT",
            },
        )
        # exists() HEAD finds the object, so created is False
        sequence = _request_sequence([head_exists, put_resp, head_200])

        with mock.patch.object(mock_provider, "make_request", side_effect=sequence):
            metadata, created = await mock_provider.upload(
                StringStream(file_content), file_wb_path
            )

        assert created is False
        assert isinstance(metadata, OracleCloudFileMetadata)

    @pytest.mark.asyncio
    async def test_upload_no_etag_skips_verification(
        self, mock_provider, file_wb_path, file_content
    ):
        # No ETag on the PUT response -> integrity check is skipped, upload still succeeds
        put_resp = _mock_response(status=200, headers={})
        head_200 = _mock_response(
            status=200,
            headers={
                "Content-Type": "application/octet-stream",
                "Content-Length": str(len(file_content)),
                "ETag": '"abc123"',
            },
        )
        sequence = _request_sequence([
            exceptions.MetadataError("not found", code=HTTPStatus.NOT_FOUND),
            put_resp,
            head_200,
        ])

        with mock.patch.object(mock_provider, "make_request", side_effect=sequence):
            metadata, created = await mock_provider.upload(
                StringStream(file_content), file_wb_path
            )

        assert created is True
        assert isinstance(metadata, OracleCloudFileMetadata)

    @pytest.mark.asyncio
    async def test_upload_checksum_mismatch(
        self, mock_provider, file_wb_path, file_content
    ):

        # PUT returns 200 with an ETag that won't match the uploaded body
        put_resp = _mock_response(status=200, headers={"ETag": '"deadbeef"'})
        sequence = _request_sequence([
            exceptions.MetadataError("not found", code=HTTPStatus.NOT_FOUND),
            put_resp,
        ])

        with mock.patch.object(mock_provider, "make_request", side_effect=sequence):
            with pytest.raises(exceptions.UploadChecksumMismatchError):
                await mock_provider.upload(StringStream(file_content), file_wb_path)

    @pytest.mark.asyncio
    async def test_download_accept_url(self, mock_provider, file_wb_path):

        signed_url = await mock_provider.download(file_wb_path, accept_url=True)

        assert isinstance(signed_url, str)
        assert "X-Amz-Algorithm=AWS4-HMAC-SHA256" in signed_url
        assert "X-Amz-Credential=fake-access-key-id" in signed_url
        assert "X-Amz-Signature=" in signed_url
        assert "response-content-disposition" in signed_url

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

    @pytest.mark.asyncio
    async def test_delete_not_found(self, mock_provider, file_wb_path):

        with mock.patch.object(
            mock_provider,
            "make_request",
            new_callable=mock.AsyncMock,
            side_effect=exceptions.DeleteError("not found", code=HTTPStatus.NOT_FOUND),
        ):
            with pytest.raises(exceptions.DeleteError):
                await mock_provider.delete(file_wb_path)


class TestIntraCopy:

    @pytest.mark.asyncio
    async def test_intra_copy_file(self, mock_provider, file_wb_path):

        dest_path = WaterButlerPath("/folder-2/copy.txt")
        copy_resp = _mock_response(status=200, body=b"<CopyObjectResult/>")
        head_200 = _mock_response(
            status=200,
            headers={
                "Content-Type": "application/octet-stream",
                "Content-Length": "44",
                "ETag": '"abc123"',
            },
        )
        # exists(dest) HEAD raises 404, copy PUT succeeds, then HEAD for metadata
        sequence = _request_sequence([
            exceptions.MetadataError("not found", code=HTTPStatus.NOT_FOUND),
            copy_resp,
            head_200,
        ])

        with mock.patch.object(mock_provider, "make_request", side_effect=sequence):
            metadata, created = await mock_provider.intra_copy(
                mock_provider, file_wb_path, dest_path
            )

        assert created is True
        assert isinstance(metadata, OracleCloudFileMetadata)
        assert metadata.path == "/folder-2/copy.txt"

    @pytest.mark.asyncio
    async def test_intra_copy_folder_raises(self, mock_provider, folder_wb_path):

        with pytest.raises(exceptions.CopyError):
            await mock_provider.intra_copy(
                mock_provider, folder_wb_path, folder_wb_path
            )

    @pytest.mark.asyncio
    async def test_intra_copy_file_folder_mismatch_raises(
        self, mock_provider, file_wb_path, folder_wb_path
    ):
        with pytest.raises(exceptions.CopyError):
            await mock_provider.intra_copy(
                mock_provider, file_wb_path, folder_wb_path
            )


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
