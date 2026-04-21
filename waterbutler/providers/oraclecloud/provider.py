import base64
import hashlib
import logging
from http import HTTPStatus
from urllib.parse import quote, urlencode

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
from waterbutler.providers.oraclecloud.signing import OCISigner

logger = logging.getLogger(__name__)

BASE_OBJ_STORAGE_URL = "https://objectstorage.{region}.oraclecloud.com"

# Request all available metadata fields from ListObjects.
LIST_FIELDS = "name,size,etag,md5,timeCreated,timeModified,storageTier,archivalState"


class OracleCloudProvider(BaseProvider):
    """Provider for Oracle Cloud Infrastructure Object Storage.

    ``OracleCloudProvider`` uses the OCI native REST API with HTTP Signature authentication
    (RFC draft-cavage-http-signatures, RSA-SHA256).

    API docs: https://docs.oracle.com/en-us/iaas/api/#/en/objectstorage/latest/

    Quirks:

    * The official name of the service is "Object Storage" provided by "Oracle Cloud
      Infrastructure".  "OCI", "Oracle Cloud", and "OCI Object Storage" are used
      interchangeably in this provider.
    """

    NAME = "oraclecloud"

    def __init__(self, auth: dict, credentials: dict, settings: dict, **kwargs) -> None:

        # Here is an example of the settings for the ``OSFStorageProvider`` in OSF.
        #
        #     WATERBUTLER_CREDENTIALS = {
        #         'storage': {
        #             'oci_user': 'ocid1.user.oc1...',
        #             'oci_fingerprint': 'aa:bb:cc:...',
        #             'oci_tenancy': 'ocid1.tenancy.oc1...',
        #             'oci_region': 'us-ashburn-1',
        #             'oci_key_content': '-----BEGIN RSA PRIVATE KEY-----\n...',
        #         }
        #     }
        #
        #     WATERBUTLER_SETTINGS = {
        #         'storage': {
        #             'provider': 'oraclecloud',
        #             'bucket': 'my-bucket',
        #             'namespace': 'my-namespace',
        #         }
        #     }
        #
        #     WATERBUTLER_RESOURCE = 'bucket'

        super().__init__(auth, credentials, settings, **kwargs)

        self.bucket = settings.get("bucket")
        if not self.bucket:
            raise InvalidProviderConfigError(
                self.NAME,
                message="Missing Object Storage bucket name from OSF",
            )

        self.namespace = settings.get("namespace")
        if not self.namespace:
            raise InvalidProviderConfigError(
                self.NAME,
                message="Missing Object Storage namespace from OSF",
            )

        oci_creds = self._extract_oci_credentials(credentials)
        self.region = oci_creds["region"]
        self._base_url = BASE_OBJ_STORAGE_URL.format(region=self.region)

        self._signer = OCISigner(
            tenancy=oci_creds["tenancy"],
            user=oci_creds["user"],
            fingerprint=oci_creds["fingerprint"],
            private_key_content=oci_creds["key_content"],
        )

    @staticmethod
    def _extract_oci_credentials(credentials: dict) -> dict:
        """Build a normalized OCI config dict from WaterButler credentials."""
        required_keys = {
            "oci_user": "user",
            "oci_fingerprint": "fingerprint",
            "oci_tenancy": "tenancy",
            "oci_region": "region",
            "oci_key_content": "key_content",
        }
        oci_creds = {}
        for wb_key, oci_key in required_keys.items():
            value = credentials.get(wb_key)
            if not value:
                raise InvalidProviderConfigError(
                    "oraclecloud",
                    message=f"Missing required OCI credential: {wb_key}",
                )
            oci_creds[oci_key] = value
        return oci_creds

    def _build_object_url(self, obj_name: str) -> str:
        """``/n/{namespace}/b/{bucket}/o/{objectName}``"""
        return (
            f"{self._base_url}/n/{quote(self.namespace, safe='')}"
            f"/b/{quote(self.bucket, safe='')}"
            f"/o/{quote(obj_name, safe='/')}"
        )

    def _build_list_url(self, prefix: str = "", delimiter: str = "/") -> str:
        """``/n/{namespace}/b/{bucket}/o?prefix=...&delimiter=...&fields=...``"""
        base = (
            f"{self._base_url}/n/{quote(self.namespace, safe='')}"
            f"/b/{quote(self.bucket, safe='')}/o"
        )
        params: dict[str, str] = {"fields": LIST_FIELDS}
        if prefix:
            params["prefix"] = prefix
        if delimiter:
            params["delimiter"] = delimiter
        return f"{base}?{urlencode(params)}"

    def _sign_headers(self, method, url, body=None, extra_headers=None):
        headers = dict(extra_headers) if extra_headers else {}
        return self._signer.sign_request(method, url, headers, body=body)

    async def validate_v1_path(self, path: str, **kwargs) -> WaterButlerPath:
        return await self.validate_path(path)

    async def validate_path(self, path: str, **kwargs) -> WaterButlerPath:
        return WaterButlerPath(path)

    async def metadata(
        self, path: WaterButlerPath, **kwargs  # type: ignore
    ) -> OracleCloudFileMetadata | list[BaseOracleCloudMetadata]:
        if path.is_folder:
            return await self._metadata_folder(path)
        return await self._metadata_file(path)

    async def upload(
        self, stream: BaseStream, path: WaterButlerPath, *args, **kwargs
    ) -> tuple[OracleCloudFileMetadata, bool]:
        """Upload a file stream to OCI Object Storage via PutObject.

        API docs: https://docs.oracle.com/en-us/iaas/api/#/en/objectstorage/latest/Object/PutObject

        The body must be signed (``x-content-sha256``), so the stream is read into memory before
        the request.  After a successful upload, WB verifies the MD5 checksum returned in the
        ``opc-content-md5`` header.  An extra HEAD is required to retrieve full metadata since
        PutObject only returns headers.
        """

        created = not await self.exists(path)
        obj_name = self._get_obj_name(path)
        url = self._build_object_url(obj_name)

        data = await stream.read()
        headers = self._sign_headers("PUT", url, body=data)

        resp = await self.make_request(
            "PUT",
            url,
            data=data,
            headers=headers,
            expects=(HTTPStatus.OK,),
            throws=UploadError,
        )
        await resp.release()

        resp_md5 = resp.headers.get("opc-content-md5", None)
        if resp_md5:
            expected_md5 = base64.b64encode(hashlib.md5(data).digest()).decode()
            if resp_md5 != expected_md5:
                raise UploadChecksumMismatchError()

        metadata = await self._metadata_file(path)
        return metadata, created

    async def download(
        self,
        path: WaterButlerPath,
        accept_url=False,
        range=None,  # type: ignore
        **kwargs,
    ) -> ResponseStreamReader:
        """Download an object via GetObject, returning a streaming reader.

        API docs: https://docs.oracle.com/en-us/iaas/api/#/en/objectstorage/latest/Object/GetObject

        ``accept_url`` is not supported yet.  OCI pre-authenticated requests (PARs) require a
        server-side API call, unlike GCS signed URLs.
        """

        if path.is_folder:
            raise DownloadError("Cannot download folders", code=HTTPStatus.BAD_REQUEST)

        obj_name = self._get_obj_name(path)
        url = self._build_object_url(obj_name)
        headers = self._sign_headers("GET", url)

        resp = await self.make_request(
            "GET",
            url,
            headers=headers,
            range=range,
            expects=(HTTPStatus.OK, HTTPStatus.PARTIAL_CONTENT),
            throws=DownloadError,
        )

        return ResponseStreamReader(resp)

    async def delete(self, path: WaterButlerPath, *args, **kwargs) -> None:  # type: ignore
        r"""Delete a file object via DeleteObject.

        API docs: https://docs.oracle.com/en-us/iaas/api/#/en/objectstorage/latest/Object/DeleteObject

        Only supports file deletion.  ``OSFStorageProvider`` doesn't need folder delete.
        """

        if path.is_folder:
            raise DeleteError("This limited provider does not support folder deletion.")

        obj_name = self._get_obj_name(path)
        url = self._build_object_url(obj_name)
        headers = self._sign_headers("DELETE", url)

        resp = await self.make_request(
            "DELETE",
            url,
            headers=headers,
            expects=(HTTPStatus.OK, HTTPStatus.NO_CONTENT),
            throws=DeleteError,
        )
        await resp.release()

    def can_intra_copy(self, other: BaseProvider, path: WaterButlerPath = None) -> bool:
        return False

    def can_intra_move(self, other: BaseProvider, path: WaterButlerPath = None) -> bool:
        return False

    def can_duplicate_names(self):
        return True

    async def _metadata_file(self, path: WaterButlerPath) -> OracleCloudFileMetadata:
        """HeadObject to get file metadata without egress charges."""

        obj_name = self._get_obj_name(path)
        url = self._build_object_url(obj_name)
        headers = self._sign_headers("HEAD", url)

        resp = await self.make_request(
            "HEAD",
            url,
            headers=headers,
            expects=(HTTPStatus.OK,),
            throws=MetadataError,
        )

        return OracleCloudFileMetadata.new_from_head_response(obj_name, resp.headers)

    async def _metadata_folder(
        self, path: WaterButlerPath
    ) -> list[BaseOracleCloudMetadata]:
        """ListObjects with ``delimiter='/'`` for common-prefix folder listing."""

        prefix = self._get_obj_name(path) if not path.is_root else ""
        url = self._build_list_url(prefix=prefix)
        headers = self._sign_headers("GET", url)

        resp = await self.make_request(
            "GET",
            url,
            headers=headers,
            expects=(HTTPStatus.OK,),
            throws=MetadataError,
        )

        data = await resp.json()

        items: list[BaseOracleCloudMetadata] = []

        for folder_prefix in data.get("prefixes", []):
            items.append(
                OracleCloudFolderMetadata({"object_name": folder_prefix})
            )

        for obj_entry in data.get("objects", []):
            items.append(
                OracleCloudFileMetadata.new_from_list_entry(obj_entry)
            )

        return items

    @staticmethod
    def _get_obj_name(path: WaterButlerPath) -> str:
        """Object names in OCI don't start with ``/``."""
        return path.path.lstrip("/")
