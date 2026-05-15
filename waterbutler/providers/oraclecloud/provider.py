import base64
import hashlib
import logging
from http import HTTPStatus
from urllib.parse import quote
from xml.sax.saxutils import escape as xml_escape

import xmltodict

from waterbutler.core.exceptions import (
    CopyError,
    DeleteError,
    DownloadError,
    IntraCopyError,
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
    * ...and **vhost-style** addressing:
      ``https://<bucket>.vhcompat.objectstorage.<region>.oci.customer-oci.com``
        # )
    * Authentication requires an OCI *Customer Secret Key* (access key + secret key)
      which is separate from the native OCI API signing key.

    S3-compatible conversion notes (ENG-10671):

    Successfully converted to S3-compatible API:

    * **Upload** (PUT Object) -- full body upload with SHA-256 signed payload and
      ETag-based MD5 integrity verification.
    * **Download** (GET Object) -- streaming via ResponseStreamReader, range support.
    * **Delete** -- single-object via ``DELETE Object``; folder via list + batch
      ``POST ?delete`` (DeleteObjects, 1000 keys/batch, paginated).
    * **Metadata** (HEAD Object) -- zero-egress metadata retrieval.
    * **Folder listing** (ListObjectsV2) -- common-prefix based virtual folders.
    * **Intra-copy** (ENG-10659) -- single-file ``PUT Object - Copy`` via the
      ``x-amz-copy-source`` header for same-namespace, same-region copies.
    * **SigV4 signing** -- fully custom, no boto/botocore dependency.

    Not yet implemented but feasible via S3-compatible API:

    * **Multipart uploads** -- S3 multipart upload API is supported by OCI S3 compat.
      Would be needed for files > ~5 GB.
    * **Presigned URLs** -- S3 query-string SigV4 (presigned URLs) works with OCI.
      Could replace the need for OCI Pre-Authenticated Requests (PARs).
    * **Folder intra-copy** -- the current ``intra_copy`` implementation only
      handles individual files; recursive folder copy would walk the prefix and
      issue one ``PUT Object - Copy`` per key.
    * **Metadata pagination** -- ``_metadata_folder`` reads only the first page
      of ListObjectsV2 results; ``_list_keys_under_prefix`` (used by folder
      delete) already paginates correctly and can serve as the template.

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

        # path style base url
        self.BASE_URL = (
            f"https://{namespace}.compat.objectstorage.{region}.oraclecloud.com"
        )

        # # vhost style base url
        # self.BASE_URL = (
        #     f"https://{self.bucket}.vhcompat.objectstorage.{region}.oci.customer-oci.com"
        # )

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
        r"""Delete the object or folder at the given path.

        Files use S3 Compat ``DELETE /<bucket>/<key>``.  Folders enumerate keys
        under the prefix via ListObjectsV2 and remove them in batches via
        ``POST /<bucket>?delete`` (S3 ``DeleteObjects``, max 1000 keys per call).
        """
        if path.is_folder:
            await self._delete_folder(path)
            return

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

    async def intra_copy(
        self,
        dest_provider: BaseProvider,
        source_path: WaterButlerPath,
        dest_path: WaterButlerPath,
    ) -> tuple[OracleCloudFileMetadata, bool]:
        """Server-side copy a single file via S3 ``PUT Object - Copy``.

        Uses the ``x-amz-copy-source`` header.  Only same-namespace, same-region
        copies between two ``OracleCloudProvider`` instances are supported -- see
        :meth:`can_intra_copy`.  Folder copies fall back to the base
        download/upload pipeline.
        """
        if source_path.is_folder or dest_path.is_folder:
            raise IntraCopyError("Folder intra-copy is not supported", code=HTTPStatus.BAD_REQUEST)

        exists = await dest_provider.exists(dest_path)

        src_obj = self._get_obj_name(source_path)
        dest_obj = dest_provider._get_obj_name(dest_path)
        dest_url = dest_provider._object_url(dest_obj)

        # `x-amz-copy-source` must be URL-encoded and prefixed with the source
        # bucket; SigV4 covers it via the canonical-headers list.
        copy_source = f"/{self.bucket}/{quote(src_obj, safe='/')}"

        headers = dest_provider._signed_headers(
            "PUT",
            dest_url,
            extra_headers={
                "x-amz-copy-source": copy_source,
                "Content-Length": "0",
            },
        )

        resp = await dest_provider.make_request(
            "PUT",
            dest_url,
            headers=headers,
            expects=(200,),
            throws=CopyError,
        )
        await resp.release()

        return await dest_provider._metadata_file(dest_path), not exists

    def can_intra_copy(self, other: BaseProvider, path: WaterButlerPath = None) -> bool:
        """True for file-level copies between two OracleCloudProvider instances
        sharing the same namespace and region (single SigV4 endpoint).
        """
        if path is not None and getattr(path, "is_folder", False):
            return False
        if not isinstance(other, OracleCloudProvider):
            return False
        return self.BASE_URL == other.BASE_URL

    def can_intra_move(self, other: BaseProvider, path: WaterButlerPath = None) -> bool:
        return self.can_intra_copy(other, path)

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

    async def _list_keys_under_prefix(self, prefix: str) -> list[str]:
        """Return every object key under *prefix* (no delimiter, recursive).

        Walks ListObjectsV2 with continuation tokens.  Used by folder delete.
        """
        keys: list[str] = []
        continuation: str | None = None

        while True:
            query: dict[str, str] = {"list-type": "2"}
            if prefix:
                query["prefix"] = prefix
            if continuation:
                query["continuation-token"] = continuation

            url = self._bucket_url(**query)
            headers = self._signed_headers("GET", url)

            resp = await self.make_request(
                "GET",
                url,
                headers=headers,
                expects=(200,),
                throws=DeleteError,
            )
            xml_body = await resp.text()
            doc = xmltodict.parse(xml_body)
            result = doc.get("ListBucketResult", {})

            contents = result.get("Contents") or []
            if isinstance(contents, dict):
                contents = [contents]
            for entry in contents:
                key = entry.get("Key")
                if key:
                    keys.append(key)

            if result.get("IsTruncated") == "true":
                continuation = result.get("NextContinuationToken")
                if not continuation:
                    break
            else:
                break

        return keys

    async def _delete_folder(self, path: WaterButlerPath) -> None:
        """Recursively delete every key under *path*.

        Uses S3 ``POST /<bucket>?delete`` (DeleteObjects) in batches of 1000.
        The S3-compat endpoint expects an MD5 of the request body in
        ``Content-MD5`` and a SHA-256 of the body in the SigV4 payload hash.
        """
        prefix = self._get_obj_name(path)
        if not prefix.endswith("/"):
            prefix = f"{prefix}/" if prefix else prefix

        keys = await self._list_keys_under_prefix(prefix)
        if not keys:
            return

        url = self._bucket_url(delete="")

        for batch_start in range(0, len(keys), 1000):
            batch = keys[batch_start:batch_start + 1000]
            body = self._build_delete_xml(batch).encode("utf-8")
            content_md5 = self._b64_md5(body)
            payload_hash = hashlib.sha256(body).hexdigest()

            headers = self._signed_headers(
                "POST",
                url,
                extra_headers={
                    "Content-Length": str(len(body)),
                    "Content-Type": "application/xml",
                    "Content-MD5": content_md5,
                },
                payload_hash=payload_hash,
            )

            resp = await self.make_request(
                "POST",
                url,
                data=body,
                headers=headers,
                skip_auto_headers={"Content-Type"},
                expects=(200,),
                throws=DeleteError,
            )
            await resp.release()

    @staticmethod
    def _build_delete_xml(keys: list[str]) -> str:
        """Build the S3 DeleteObjects request body for *keys* (Quiet mode)."""
        objects = "".join(f"<Object><Key>{xml_escape(k)}</Key></Object>" for k in keys)
        return (
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            f"<Delete>{objects}<Quiet>true</Quiet></Delete>"
        )

    @staticmethod
    def _b64_md5(data: bytes) -> str:
        """Return base64-encoded MD5, as required by S3 ``Content-MD5``."""
        return base64.b64encode(hashlib.md5(data).digest()).decode("ascii")

    @staticmethod
    def _get_obj_name(path: WaterButlerPath) -> str:
        """Convert a WaterButlerPath to an S3-compatible object key (no leading ``/``)."""
        return path.path.lstrip("/")
