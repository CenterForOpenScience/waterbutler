import pytest

from waterbutler.providers.oraclecloud.metadata import (
    BaseOracleCloudMetadata,
    OracleCloudFileMetadata,
    OracleCloudFolderMetadata,
)


class TestOracleCloudFileMetadata:

    def test_file_metadata_from_dict(self):

        raw = {
            "object_name": "path/to/file.txt",
            "content_type": "text/plain",
            "last_modified": "Thu, 01 Mar 2025 19:04:45 GMT",
            "size": 1024,
            "etag": "abc123def456",
            "extra": {
                "md5": "abc123def456",
            },
            "time_created": "2025-03-01T19:00:00+00:00",
        }
        metadata = OracleCloudFileMetadata(raw)

        assert isinstance(metadata, BaseOracleCloudMetadata)
        assert metadata.provider == "oraclecloud"
        assert metadata.kind == "file"
        assert metadata.name == "file.txt"
        assert metadata.path == "/path/to/file.txt"
        assert metadata.content_type == "text/plain"
        assert metadata.modified == "Thu, 01 Mar 2025 19:04:45 GMT"
        assert metadata.size == 1024
        assert metadata.size_as_int == 1024
        assert metadata.etag == "abc123def456"
        assert metadata.extra["md5"] == "abc123def456"
        assert metadata.created_utc == "2025-03-01T19:00:00+00:00"

    def test_file_metadata_missing_optional_fields(self):

        raw = {
            "object_name": "simple.txt",
            "content_type": None,
            "last_modified": None,
            "size": None,
            "etag": None,
            "extra": {},
        }
        metadata = OracleCloudFileMetadata(raw)

        assert metadata.name == "simple.txt"
        assert metadata.content_type is None
        assert metadata.modified is None
        assert metadata.size is None
        assert metadata.etag is None
        assert metadata.extra == {}
        assert metadata.created_utc is None

    def test_file_metadata_from_head_response(self):

        headers = {
            "Content-Type": "application/pdf",
            "Content-Length": "2048",
            "Last-Modified": "Fri, 14 Mar 2025 12:00:00 GMT",
            "ETag": '"deadbeef"',
        }

        metadata = OracleCloudFileMetadata.new_from_head_response(
            "folder/report.pdf", headers
        )

        assert metadata.provider == "oraclecloud"
        assert metadata.name == "report.pdf"
        assert metadata.path == "/folder/report.pdf"
        assert metadata.content_type == "application/pdf"
        assert metadata.size == 2048
        assert metadata.etag == "deadbeef"
        assert metadata.extra["md5"] == "deadbeef"

    def test_file_metadata_from_s3_list_entry(self):

        entry = {
            "Key": "data/results.csv",
            "LastModified": "2025-03-10T08:00:00.000Z",
            "ETag": '"etag123"',
            "Size": "4096",
            "StorageClass": "STANDARD",
        }

        metadata = OracleCloudFileMetadata.new_from_s3_list_entry(entry)

        assert metadata.name == "results.csv"
        assert metadata.path == "/data/results.csv"
        assert metadata.size == 4096
        assert metadata.etag == "etag123"
        assert metadata.extra["md5"] == "etag123"
        assert metadata.modified == "2025-03-10T08:00:00.000Z"
        assert metadata.created_utc is None

    def test_file_metadata_from_s3_list_entry_no_quotes(self):

        entry = {
            "Key": "file.bin",
            "LastModified": "2025-01-01T00:00:00.000Z",
            "ETag": "notag",
            "Size": "0",
        }

        metadata = OracleCloudFileMetadata.new_from_s3_list_entry(entry)

        assert metadata.name == "file.bin"
        assert metadata.etag == "notag"
        assert metadata.size == 0


class TestOracleCloudFolderMetadata:

    def test_folder_metadata(self):

        raw = {"object_name": "path/to/folder/"}
        metadata = OracleCloudFolderMetadata(raw)

        assert isinstance(metadata, BaseOracleCloudMetadata)
        assert metadata.provider == "oraclecloud"
        assert metadata.kind == "folder"
        assert metadata.name == "folder"
        assert metadata.path == "/path/to/folder/"

    def test_folder_metadata_nested(self):

        raw = {"object_name": "a/b/c/"}
        metadata = OracleCloudFolderMetadata(raw)

        assert metadata.name == "c"
        assert metadata.path == "/a/b/c/"
