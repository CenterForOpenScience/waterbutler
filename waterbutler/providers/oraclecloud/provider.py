import asyncio
import base64
import hashlib
import logging
from functools import partial
from http import HTTPStatus

import oci

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
from waterbutler.core.streams import BaseStream, StringStream
from waterbutler.providers.oraclecloud.metadata import (
    BaseOracleCloudMetadata,
    OracleCloudFileMetadata,
    OracleCloudFolderMetadata,
)

logger = logging.getLogger(__name__)


class OracleCloudProvider(BaseProvider):
    """Provider for Oracle Cloud Infrastructure Object Storage.

    ``OracleCloudProvider`` uses the OCI Python SDK (``ObjectStorageClient``) to interact with
    OCI Object Storage.  The synchronous SDK calls are wrapped with ``run_in_executor`` for
    async compatibility.

    General API docs: https://docs.oracle.com/en-us/iaas/Content/Object/Concepts/objectstorageoverview.htm

    SDK docs: https://docs.oracle.com/en-us/iaas/tools/python/latest/api/object_storage.html

    Quirks:

    * The official name of the service is "Object Storage" provided by "Oracle Cloud
      Infrastructure".  "OCI", "Oracle Cloud", and "OCI Object Storage" are used
      interchangeably in this provider.
    """

    NAME = "oraclecloud"

    def __init__(self, auth: dict, credentials: dict, settings: dict, **kwargs) -> None:
        """Initialize a provider instance with the given parameters.

        :param dict auth: the auth dictionary
        :param dict credentials: the credentials dictionary
        :param dict settings: the settings dictionary
        """

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

        oci_config = self._build_oci_config(credentials)
        try:
            oci.config.validate_config(oci_config)
        except (oci.exceptions.InvalidConfig, ValueError) as exc:
            raise InvalidProviderConfigError(
                self.NAME,
                message=f"Invalid OCI configuration: {exc}",
            )

        self._oci_config = oci_config
        self._client = oci.object_storage.ObjectStorageClient(oci_config)

    @staticmethod
    def _build_oci_config(credentials: dict) -> dict:
        """Build an OCI SDK config dict from WaterButler credentials.

        :param dict credentials: the credentials dict from OSF
        :rtype: dict
        """
        required_keys = {
            "oci_user": "user",
            "oci_fingerprint": "fingerprint",
            "oci_tenancy": "tenancy",
            "oci_region": "region",
            "oci_key_content": "key_content",
        }
        oci_config = {}
        for wb_key, oci_key in required_keys.items():
            value = credentials.get(wb_key)
            if not value:
                raise InvalidProviderConfigError(
                    "oraclecloud",
                    message=f"Missing required OCI credential: {wb_key}",
                )
            oci_config[oci_key] = value

        return oci_config

    async def validate_v1_path(self, path: str, **kwargs) -> WaterButlerPath:
        return await self.validate_path(path)

    async def validate_path(self, path: str, **kwargs) -> WaterButlerPath:
        return WaterButlerPath(path)

    async def metadata(
        self, path: WaterButlerPath, **kwargs  # type: ignore
    ) -> OracleCloudFileMetadata | list[BaseOracleCloudMetadata]:
        """Get the metadata about the object with the given WaterButlerPath.

        :param path: the WaterButlerPath to the file or folder
        :type path: :class:`.WaterButlerPath`
        :param dict kwargs: additional kwargs are ignored
        :rtype: :class:`.OracleCloudFileMetadata` (for file)
        :rtype: List<:class:`.BaseOracleCloudMetadata`> (for folder)
        """

        if path.is_folder:
            return await self._metadata_folder(path)
        return await self._metadata_file(path)

    async def upload(
        self, stream: BaseStream, path: WaterButlerPath, *args, **kwargs
    ) -> tuple[OracleCloudFileMetadata, bool]:
        """Upload a file stream to the given WaterButlerPath.

        API docs:

            PutObject: https://docs.oracle.com/en-us/iaas/api/#/en/objectstorage/latest/Object/PutObject

        The OCI SDK ``put_object`` handles the upload.  After a successful upload, WB verifies
        the MD5 checksum against the ``opc-content-md5`` header returned by OCI.  WB must make
        an extra metadata request after a successful upload since ``put_object`` returns headers
        only.

        :param stream: the stream to post
        :type stream: :class:`.streams.BaseStream`
        :param path: the WaterButlerPath of the file to upload
        :type path: :class:`.WaterButlerPath`
        :param list args: additional args are ignored
        :param dict kwargs: additional kwargs are ignored
        :rtype: :class:`.OracleCloudFileMetadata`
        :rtype: bool
        """

        created = not await self.exists(path)
        obj_name = self._get_obj_name(path)

        data = await stream.read()
        loop = asyncio.get_running_loop()

        try:
            resp = await loop.run_in_executor(
                None,
                partial(
                    self._client.put_object,
                    self.namespace,
                    self.bucket,
                    obj_name,
                    data,
                    content_length=stream.size,
                ),
            )
        except oci.exceptions.ServiceError as exc:
            raise UploadError(str(exc))

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
    ) -> StringStream:
        """Download the object with the given path.

        API docs:

            GetObject: https://docs.oracle.com/en-us/iaas/api/#/en/objectstorage/latest/Object/GetObject

        .. note::

            ``accept_url`` is not supported in this version.  OCI pre-authenticated requests
            (PARs) require a server-side API call, unlike GCS signed URLs which are generated
            locally.  For now all downloads go through WB.

        :param path: the WaterButlerPath for the object to download
        :type path: :class:`.WaterButlerPath`
        :param bool accept_url: ignored (not supported)
        :param tuple range: the Range HTTP request header
        :param dict kwargs: ``display_name`` is ignored in this version
        :rtype: :class:`.streams.StringStream`
        """

        if path.is_folder:
            raise DownloadError("Cannot download folders", code=HTTPStatus.BAD_REQUEST)

        obj_name = self._get_obj_name(path)
        loop = asyncio.get_running_loop()

        kwargs_oci = {}
        if range is not None:
            start, end = range
            range_header = f"bytes={start}-"
            if end is not None:
                range_header = f"bytes={start}-{end}"
            kwargs_oci["range"] = range_header

        try:
            resp = await loop.run_in_executor(
                None,
                partial(
                    self._client.get_object,
                    self.namespace,
                    self.bucket,
                    obj_name,
                    **kwargs_oci,
                ),
            )
        except oci.exceptions.ServiceError as exc:
            if exc.status == HTTPStatus.NOT_FOUND:
                raise DownloadError(
                    f"Object not found: {path}", code=HTTPStatus.NOT_FOUND
                )
            raise DownloadError(str(exc))

        # The synchronous OCI SDK reads the full response body into memory.
        return StringStream(resp.data.content)

    async def delete(self, path: WaterButlerPath, *args, **kwargs) -> None:  # type: ignore
        r"""Deletes the file object with the specified WaterButler path.

        API docs:

            DeleteObject: https://docs.oracle.com/en-us/iaas/api/#/en/objectstorage/latest/Object/DeleteObject

        .. note::

            This limited version only supports deletion for file objects.  The main reason is
            that ``OSFStorageProvider`` does not need it.

            *TODO [Phase 2]: If needed, iterate through all children and delete each of them.*

        :param path: the WaterButlerPath of the object to delete
        :type path: :class:`.WaterButlerPath`
        :param list args: additional args are ignored
        :param dict kwargs: additional kwargs are ignored
        :rtype: None
        """

        if path.is_folder:
            raise DeleteError("This limited provider does not support folder deletion.")

        obj_name = self._get_obj_name(path)
        loop = asyncio.get_running_loop()

        try:
            await loop.run_in_executor(
                None,
                partial(
                    self._client.delete_object,
                    self.namespace,
                    self.bucket,
                    obj_name,
                ),
            )
        except oci.exceptions.ServiceError as exc:
            if exc.status == HTTPStatus.NOT_FOUND:
                raise NotFoundError(str(path))
            raise DeleteError(str(exc))

    def can_intra_copy(self, other: BaseProvider, path: WaterButlerPath = None) -> bool:
        return False

    def can_intra_move(self, other: BaseProvider, path: WaterButlerPath = None) -> bool:
        return False

    def can_duplicate_names(self):
        return True

    async def _metadata_file(self, path: WaterButlerPath) -> OracleCloudFileMetadata:
        """Get the metadata about the file object with the given WaterButlerPath.  Uses
        ``head_object`` to avoid egress charges.

        :param path: the WaterButlerPath of the object
        :type path: :class:`.WaterButlerPath`
        :rtype: :class:`.OracleCloudFileMetadata`
        """

        obj_name = self._get_obj_name(path)
        loop = asyncio.get_running_loop()

        try:
            resp = await loop.run_in_executor(
                None,
                partial(
                    self._client.head_object,
                    self.namespace,
                    self.bucket,
                    obj_name,
                ),
            )
        except oci.exceptions.ServiceError as exc:
            if exc.status == HTTPStatus.NOT_FOUND:
                raise NotFoundError(str(path))
            raise MetadataError(str(exc))

        return OracleCloudFileMetadata.new_from_head_response(obj_name, resp)

    async def _metadata_folder(
        self, path: WaterButlerPath
    ) -> list[BaseOracleCloudMetadata]:
        """List the contents of the folder with the given WaterButlerPath using common-prefix
        listing (``list_objects`` with ``delimiter='/'``).

        :param path: the WaterButlerPath of the folder
        :type path: :class:`.WaterButlerPath`
        :rtype: list[:class:`.BaseOracleCloudMetadata`]
        """

        prefix = self._get_obj_name(path) if not path.is_root else ""
        loop = asyncio.get_running_loop()

        try:
            resp = await loop.run_in_executor(
                None,
                partial(
                    self._client.list_objects,
                    self.namespace,
                    self.bucket,
                    prefix=prefix,
                    delimiter="/",
                ),
            )
        except oci.exceptions.ServiceError as exc:
            raise MetadataError(str(exc))

        items: list[BaseOracleCloudMetadata] = []

        for folder_prefix in resp.data.prefixes or []:
            items.append(
                OracleCloudFolderMetadata(
                    {
                        "object_name": folder_prefix,
                    }
                )
            )

        for obj_summary in resp.data.objects or []:
            items.append(
                OracleCloudFileMetadata.new_from_oci_object_summary(obj_summary)
            )

        return items

    @staticmethod
    def _get_obj_name(path: WaterButlerPath) -> str:
        """Get the object name for the given WaterButlerPath.  Object names in OCI Object
        Storage do not start with ``/``.

        :param path: the WaterButlerPath of the object
        :type path: :class:`.WaterButlerPath`
        :rtype: str
        """

        return path.path.lstrip("/")
