import logging
import os

from waterbutler.core import metadata

logger = logging.getLogger(__name__)


class BaseOracleCloudMetadata(metadata.BaseMetadata):
    """This class provides the base structure of both files and folders metadata for the
    :class:`.OracleCloudProvider`.  It is an abstract class and does not implement all abstract
    methods and properties in :class:`.BaseMetadata`.
    """

    @property
    def provider(self) -> str:
        return "oraclecloud"

    @property
    def path(self) -> str:
        return self.build_path(self.raw.get("object_name", ""))


class OracleCloudFileMetadata(BaseOracleCloudMetadata, metadata.BaseFileMetadata):
    """This class provides the full structure of the files for the :class:`.OracleCloudProvider`.
    It inherits two concrete classes: :class:`.BaseOracleCloudMetadata` and
    :class:`.BaseFileMetadata`.
    """

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
    def new_from_oci_object_summary(cls, obj_summary) -> "OracleCloudFileMetadata":
        """Construct an instance of :class:`.OracleCloudFileMetadata` from an OCI
        ``ObjectSummary`` returned by ``list_objects``.

        :param obj_summary: an ``oci.object_storage.models.ObjectSummary``
        :rtype: :class:`.OracleCloudFileMetadata`
        """

        return cls(
            {
                "object_name": obj_summary.name,
                "content_type": None,
                "last_modified": (
                    obj_summary.time_modified.isoformat()
                    if obj_summary.time_modified
                    else None
                ),
                "size": obj_summary.size,
                "etag": obj_summary.etag,
                "extra": {
                    "md5": obj_summary.md5,
                    "storage_tier": obj_summary.storage_tier,
                    "archival_state": obj_summary.archival_state,
                },
                "time_created": (
                    obj_summary.time_created.isoformat()
                    if getattr(obj_summary, "time_created", None)
                    else None
                ),
            }
        )

    @classmethod
    def new_from_head_response(
        cls, obj_name: str, head_resp
    ) -> "OracleCloudFileMetadata":
        """Construct an instance of :class:`.OracleCloudFileMetadata` from the response headers
        returned by ``head_object``.

        OCI-specific headers used:

        * ``opc-content-md5``: base64-encoded MD5 hash
        * ``storage-tier``: e.g. Standard, InfrequentAccess, Archive
        * ``archival-state``: set when object is in Archive tier

        :param str obj_name: the object name
        :param head_resp: the response from ``head_object()``
        :rtype: :class:`.OracleCloudFileMetadata`
        """

        headers = head_resp.headers
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
    """This class provides the full structure of the folders for the
    :class:`.OracleCloudProvider`.  It inherits two concrete classes:
    :class:`.BaseOracleCloudMetadata` and :class:`.BaseFolderMetadata`.

    OCI Object Storage uses a flat namespace; folders are represented by common prefixes
    returned by ``list_objects`` with a delimiter.
    """

    @property
    def name(self) -> str:
        return os.path.split(self.path.rstrip("/"))[1]
