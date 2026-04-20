import hashlib
import logging
from http import HTTPStatus
from urllib.parse import quote

import xmltodict

from waterbutler.core.exceptions import (
    DeleteError,
    DownloadError,
    InvalidProviderConfigError,
    MetadataError,
    NotFoundError,
    UploadChecksumMismatchError,
    UploadError,
)
from waterbutler.core.path import WaterButlerPath
from waterbutler.core.provider import BaseProvider
from waterbutler.core.streams import BaseStream, ResponseStreamReader
from waterbutler.providers.oraclecloud.metadata import (
    BaseOracleCloudMetadata,
    OracleCloudFileMetadata,
    OracleCloudFolderMetadata,
)
from waterbutler.providers.oraclecloud.signing import EMPTY_SHA256, SigV4Signer

logger = logging.getLogger(__name__)


class OracleCloudProvider(BaseProvider):
    """Provider for Oracle Cloud Infrastructure Object Storage via S3-compatible API.

    Uses OCI's Amazon S3 Compatibility API with manual AWS SigV4 signing (no boto
    dependency).  All requests go through :meth:`BaseProvider.make_request` backed
    by aiohttp.

    S3 Compat docs: https://docs.oracle.com/en-us/iaas/Content/Object/Tasks/s3compatibleapi.htm

    Quirks:

    * OCI's S3-compatible endpoint uses **path-style** addressing:
      ``https://<namespace>.compat.objectstorage.<region>.oraclecloud.com/<bucket>/<key>``
    * Authentication requires an OCI *Customer Secret Key* (access key + secret key)
      which is separate from the native OCI API signing key.

    S3-compatible conversion notes (ENG-10671):

    Successfully converted to S3-compatible API:

    * **Upload** (PUT Object) -- full body upload with SHA-256 signed payload and
      ETag-based MD5 integrity verification.
    * **Download** (GET Object) -- streaming via ResponseStreamReader, range support.
    * **Delete** (DELETE Object) -- single-object deletion.
    * **Metadata** (HEAD Object) -- zero-egress metadata retrieval.
    * **Folder listing** (ListObjectsV2) -- common-prefix based virtual folders.
    * **SigV4 signing** -- fully custom, no boto/botocore dependency.

    Not yet implemented but feasible via S3-compatible API:

    * **Multipart uploads** -- S3 multipart upload API is supported by OCI S3 compat.
      Would be needed for files > ~5 GB.
    * **Presigned URLs** -- S3 query-string SigV4 (presigned URLs) works with OCI.
      Could replace the need for OCI Pre-Authenticated Requests (PARs).
    * **Folder deletion** -- achievable via list + batch DELETE (same as S3 provider).
    * **Intra-copy** -- S3 ``PUT Object - Copy`` is supported by OCI S3 compat for
      same-region, same-namespace copies.
    * **Pagination** -- ListObjectsV2 continuation tokens are supported.

    Cannot be done via S3-compatible API (OCI-native only):

    * **Work Request polling** -- OCI-specific async operation tracking for long-running
      tasks. S3 compat does not expose this.  Multipart upload is the S3 equivalent
      for large operations.
    * **Storage tier management** -- OCI storage tiers (Standard, InfrequentAccess,
      Archive) and archival-state transitions require the native OCI API.
    * **Object lifecycle policies** -- native OCI API only.
    * **Namespace/compartment management** -- native OCI API only.
    """

    NAME = "oraclecloud"

    def __init__(self, auth: dict, credentials: dict, settings: dict, **kwargs) -> None:
        """Initialize a provider instance.

        Example credential / settings layout used by ``OSFStorageProvider``::

            WATERBUTLER_CREDENTIALS = {
                "storage": {
                    "access_key": "<customer-secret-key-access-key>",
                    "secret_key": "<customer-secret-key>",
                    "region": "us-ashburn-1",
                }
            }

            WATERBUTLER_SETTINGS = {
                "storage": {
                    "provider": "oraclecloud",
                    "bucket": "my-bucket",
                    "namespace": "my-namespace",
                }
            }
        """
        super().__init__(auth, credentials, settings, **kwargs)

        self.bucket = settings.get("bucket")
        if not self.bucket:
            raise InvalidProviderConfigError(
                self.NAME,
                message="Missing Object Storage bucket name from OSF",
            )

        namespace = settings.get("namespace")
        if not namespace:
            raise InvalidProviderConfigError(
                self.NAME,
                message="Missing Object Storage namespace from OSF",
            )

        region = credentials.get("region")
        if not region:
            raise InvalidProviderConfigError(
                self.NAME,
                message="Missing required credential: region",
            )

        access_key = credentials.get("access_key")
        if not access_key:
            raise InvalidProviderConfigError(
                self.NAME,
                message="Missing required credential: access_key",
            )

        secret_key = credentials.get("secret_key")
        if not secret_key:
            raise InvalidProviderConfigError(
                self.NAME,
                message="Missing required credential: secret_key",
            )

        self.BASE_URL = (
            f"https://{namespace}.compat.objectstorage.{region}.oraclecloud.com"
        )
        self._signer = SigV4Signer(access_key, secret_key, region)

    # ------------------------------------------------------------------
    # URL helpers
    # ------------------------------------------------------------------

    def _object_url(self, obj_name: str) -> str:
        """Full URL for a single-object operation (HEAD / GET / PUT / DELETE)."""
        return f"{self.BASE_URL}/{self.bucket}/{quote(obj_name, safe='/')}"

    def _bucket_url(self, **query: str) -> str:
        """Bucket URL with optional query-string parameters.

        Parameters are sorted and URI-encoded for SigV4 consistency.
        """
        base = f"{self.BASE_URL}/{self.bucket}"
        if not query:
            return base
        parts = [
            f"{quote(str(k), safe='')}={quote(str(v), safe='')}"
            for k, v in sorted(query.items())
        ]
        return f"{base}?{'&'.join(parts)}"

    # ------------------------------------------------------------------
    # Signing helper
    # ------------------------------------------------------------------

    def _signed_headers(
        self,
        method: str,
        url: str,
        extra_headers: dict[str, str] | None = None,
        payload_hash: str = EMPTY_SHA256,
    ) -> dict[str, str]:
        """Return a signed-headers dict ready for ``make_request``."""
        return self._signer.sign_request(
            method, url, headers=extra_headers, payload_hash=payload_hash
        )

    # ------------------------------------------------------------------
    # Required provider interface
    # ------------------------------------------------------------------

    async def validate_v1_path(self, path: str, **kwargs) -> WaterButlerPath:
        return await self.validate_path(path)

    async def validate_path(self, path: str, **kwargs) -> WaterButlerPath:
        return WaterButlerPath(path)

    async def metadata(
        self, path: WaterButlerPath, **kwargs
    ) -> OracleCloudFileMetadata | list[BaseOracleCloudMetadata]:
        """Get metadata about the object or folder at *path*.

        :param path: the WaterButlerPath to the file or folder
        :rtype: :class:`.OracleCloudFileMetadata` | list[:class:`.BaseOracleCloudMetadata`]
        """
        if path.is_folder:
            return await self._metadata_folder(path)
        return await self._metadata_file(path)

    async def upload(
        self, stream: BaseStream, path: WaterButlerPath, *args, **kwargs
    ) -> tuple[OracleCloudFileMetadata, bool]:
        """Upload a file stream to the given path.

        S3 Compat API: ``PUT /<bucket>/<key>``

        After a successful upload, the ETag (hex-encoded MD5 for non-multipart
        uploads) is verified against a locally computed digest.  An extra HEAD
        request is made to retrieve full metadata for the response.
        """
        created = not await self.exists(path)
        obj_name = self._get_obj_name(path)
        url = self._object_url(obj_name)

        data = await stream.read()
        payload_hash = hashlib.sha256(data).hexdigest()

        headers = self._signed_headers(
            "PUT",
            url,
            extra_headers={
                "Content-Length": str(len(data)),
                "Content-Type": "application/octet-stream",
            },
            payload_hash=payload_hash,
        )

        resp = await self.make_request(
            "PUT",
            url,
            data=data,
            headers=headers,
            skip_auto_headers={"Content-Type"},
            expects=(200,),
            throws=UploadError,
        )
        await resp.release()

        # Verify upload integrity via ETag (hex MD5 for non-multipart uploads)
        resp_etag = resp.headers.get("ETag", "").strip('"')
        if resp_etag:
            expected_md5 = hashlib.md5(data).hexdigest()
            if resp_etag != expected_md5:
                raise UploadChecksumMismatchError()

        file_metadata = await self._metadata_file(path)
        return file_metadata, created

    async def download(
        self,
        path: WaterButlerPath,
        accept_url=False,
        range=None,  # type: ignore[assignment]
        **kwargs,
    ) -> ResponseStreamReader:
        """Download the object at the given path.

        S3 Compat API: ``GET /<bucket>/<key>``

        .. note::

            ``accept_url`` is not supported.  OCI pre-authenticated requests (PARs)
            require a server-side call.  All downloads stream through WB.
        """
        if path.is_folder:
            raise DownloadError("Cannot download folders", code=HTTPStatus.BAD_REQUEST)

        obj_name = self._get_obj_name(path)
        url = self._object_url(obj_name)

        extra: dict[str, str] = {}
        if range is not None:
            start, end = range
            range_str = f"bytes={start}-"
            if end is not None:
                range_str = f"bytes={start}-{end}"
            extra["Range"] = range_str

        headers = self._signed_headers("GET", url, extra_headers=extra)

        resp = await self.make_request(
            "GET",
            url,
            headers=headers,
            expects=(200, 206, 404),
            throws=DownloadError,
        )

        if resp.status == HTTPStatus.NOT_FOUND:
            resp.close()
            raise DownloadError(
                f"Object not found: {path}", code=HTTPStatus.NOT_FOUND
            )

        return ResponseStreamReader(resp)

    async def delete(self, path: WaterButlerPath, *args, **kwargs) -> None:  # type: ignore[override]
        r"""Delete the file at the given path.

        S3 Compat API: ``DELETE /<bucket>/<key>``

        .. note::

            Folder deletion is not supported in this version.
        """
        if path.is_folder:
            raise DeleteError("This limited provider does not support folder deletion.")

        obj_name = self._get_obj_name(path)
        url = self._object_url(obj_name)
        headers = self._signed_headers("DELETE", url)

        resp = await self.make_request(
            "DELETE",
            url,
            headers=headers,
            expects=(200, 204),
            throws=DeleteError,
        )
        await resp.release()

    def can_intra_copy(self, other: BaseProvider, path: WaterButlerPath = None) -> bool:
        return False

    def can_intra_move(self, other: BaseProvider, path: WaterButlerPath = None) -> bool:
        return False

    def can_duplicate_names(self):
        return True

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _metadata_file(self, path: WaterButlerPath) -> OracleCloudFileMetadata:
        """Fetch file metadata via ``HEAD /<bucket>/<key>``.

        Uses HEAD to avoid egress charges (no body transferred).
        """
        obj_name = self._get_obj_name(path)
        url = self._object_url(obj_name)
        headers = self._signed_headers("HEAD", url)

        resp = await self.make_request(
            "HEAD",
            url,
            headers=headers,
            expects=(200, 404),
            throws=MetadataError,
        )

        if resp.status == HTTPStatus.NOT_FOUND:
            raise NotFoundError(str(path))

        return OracleCloudFileMetadata.new_from_head_response(obj_name, resp.headers)

    async def _metadata_folder(
        self, path: WaterButlerPath
    ) -> list[BaseOracleCloudMetadata]:
        """List folder contents via ``GET /<bucket>?list-type=2&prefix=...&delimiter=/``.

        Uses S3-compatible ``ListObjectsV2``.  Does not paginate (single page).
        """
        prefix = self._get_obj_name(path) if not path.is_root else ""

        query: dict[str, str] = {"list-type": "2", "delimiter": "/"}
        if prefix:
            query["prefix"] = prefix

        url = self._bucket_url(**query)
        headers = self._signed_headers("GET", url)

        resp = await self.make_request(
            "GET",
            url,
            headers=headers,
            expects=(200,),
            throws=MetadataError,
        )

        xml_body = await resp.text()
        doc = xmltodict.parse(xml_body)
        result = doc.get("ListBucketResult", {})

        items: list[BaseOracleCloudMetadata] = []

        # Folder entries (common prefixes)
        prefixes = result.get("CommonPrefixes") or []
        if isinstance(prefixes, dict):
            prefixes = [prefixes]
        for pfx in prefixes:
            items.append(
                OracleCloudFolderMetadata({"object_name": pfx["Prefix"]})
            )

        # File entries
        contents = result.get("Contents") or []
        if isinstance(contents, dict):
            contents = [contents]
        for entry in contents:
            items.append(OracleCloudFileMetadata.new_from_s3_list_entry(entry))

        return items

    @staticmethod
    def _get_obj_name(path: WaterButlerPath) -> str:
        """Convert a WaterButlerPath to an S3-compatible object key (no leading ``/``)."""
        return path.path.lstrip("/")
