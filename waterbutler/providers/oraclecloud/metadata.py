import logging
import os

from waterbutler.core import metadata

logger = logging.getLogger(__name__)


class BaseOracleCloudMetadata(metadata.BaseMetadata):

    @property
    def provider(self) -> str:
        return "oraclecloud"

    @property
    def path(self) -> str:
        return self.build_path(self.raw.get("object_name", ""))


class OracleCloudFileMetadata(BaseOracleCloudMetadata, metadata.BaseFileMetadata):

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
    def new_from_list_entry(cls, entry: dict) -> "OracleCloudFileMetadata":
        """Build from one element of the ``objects`` array in a ListObjects JSON response.

        The ``fields`` query param must request the full set for this to populate everything.
        """
        return cls(
            {
                "object_name": entry["name"],
                "content_type": None,
                "last_modified": entry.get("timeModified"),
                "size": entry.get("size"),
                "etag": entry.get("etag"),
                "extra": {
                    "md5": entry.get("md5"),
                    "storage_tier": entry.get("storageTier"),
                    "archival_state": entry.get("archivalState"),
                },
                "time_created": entry.get("timeCreated"),
            }
        )

    @classmethod
    def new_from_head_response(cls, obj_name, headers) -> "OracleCloudFileMetadata":
        """Build from response headers of a HeadObject call."""
        return cls(
            {
                "object_name": obj_name,
                "content_type": headers.get("content-type", None),
                "last_modified": headers.get("last-modified", None),
                "size": int(headers.get("content-length", 0)),
                "etag": headers.get("etag", "").strip('"'),
                "extra": {
                    "md5": headers.get("opc-content-md5", None),
                    "storage_tier": headers.get("storage-tier", None),
                    "archival_state": headers.get("archival-state", None),
                },
                "time_created": headers.get("opc-meta-time-created", None),
            }
        )


class OracleCloudFolderMetadata(BaseOracleCloudMetadata, metadata.BaseFolderMetadata):
    """OCI uses a flat namespace; folders are common prefixes from ``ListObjects``."""

    @property
    def name(self) -> str:
        return os.path.split(self.path.rstrip("/"))[1]
