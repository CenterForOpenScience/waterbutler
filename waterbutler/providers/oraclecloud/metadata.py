import logging
import os

from waterbutler.core import metadata

logger = logging.getLogger(__name__)


class BaseOracleCloudMetadata(metadata.BaseMetadata):
    """Base metadata for the OracleCloud provider (S3-compatible API).

    This is an abstract class; it does not implement all abstract methods and
    properties in :class:`.BaseMetadata`.
    """

    @property
    def provider(self) -> str:
        return "oraclecloud"

    @property
    def path(self) -> str:
        return self.build_path(self.raw.get("object_name", ""))


class OracleCloudFileMetadata(BaseOracleCloudMetadata, metadata.BaseFileMetadata):
    """File metadata for the OracleCloud provider (S3-compatible API)."""

    @property
    def name(self) -> str:
        return os.path.split(self.path)[1]

    @property
    def content_type(self) -> str | None:
        return self.raw.get("content_type", None)

    @property
    def modified(self) -> str | None:
        return self.raw.get("last_modified", None)

    @property
    def created_utc(self) -> str | None:
        return self.raw.get("time_created", None)

    @property
    def size(self) -> int | None:
        size = self.raw.get("size", None)
        return int(size) if size is not None else None

    @property
    def etag(self) -> str | None:
        return self.raw.get("etag", None)

    @property
    def extra(self) -> dict:
        return self.raw.get("extra", {})

    @classmethod
    def new_from_s3_list_entry(cls, entry: dict) -> "OracleCloudFileMetadata":
        """Construct from a parsed S3 ``ListBucketResult/Contents`` element.

        :param entry: a dict produced by ``xmltodict`` from the ``<Contents>`` element
        :rtype: :class:`.OracleCloudFileMetadata`
        """
        etag = entry.get("ETag", "").strip('"')
        return cls(
            {
                "object_name": entry["Key"],
                "content_type": None,
                "last_modified": entry.get("LastModified"),
                "size": int(entry.get("Size", 0)),
                "etag": etag,
                "extra": {
                    "md5": etag,
                },
            }
        )

    @classmethod
    def new_from_head_response(
        cls, obj_name: str, headers
    ) -> "OracleCloudFileMetadata":
        """Construct from the response headers of an S3-compatible ``HEAD`` request.

        Works with both ``CIMultiDict`` (aiohttp) and plain ``dict`` headers.

        :param str obj_name: the object key (no leading ``/``)
        :param headers: the response headers
        :rtype: :class:`.OracleCloudFileMetadata`
        """
        etag = headers.get("ETag", "").strip('"')
        return cls(
            {
                "object_name": obj_name,
                "content_type": headers.get("Content-Type"),
                "last_modified": headers.get("Last-Modified"),
                "size": int(headers.get("Content-Length", 0)),
                "etag": etag,
                "extra": {
                    "md5": etag,
                },
            }
        )


class OracleCloudFolderMetadata(BaseOracleCloudMetadata, metadata.BaseFolderMetadata):
    """Folder metadata for the OracleCloud provider.

    OCI Object Storage uses a flat namespace; folders are represented by common
    prefixes returned by the S3-compatible ``ListObjectsV2`` operation.
    """

    @property
    def name(self) -> str:
        return os.path.split(self.path.rstrip("/"))[1]
