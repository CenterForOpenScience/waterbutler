import re
import os
import abc
import typing
import logging

from waterbutler.core import metadata
from waterbutler.core.exceptions import MetadataError
from waterbutler.providers.googlecloud.utils import decode_and_hexlify_hashes

logger = logging.getLogger(__name__)


class BaseGoogleCloudMetadata(metadata.BaseMetadata, metaclass=abc.ABCMeta):
    """The ``BaseGoogleCloudMetadata`` object provides the base structure for both file and folder
    metadata on the Google Cloud Storage.  It is an abstract class and does not implements all
    abstract methods and properties in ``core_metadata.BaseMetadata``.
    """

    @property
    def provider(self) -> str:
        return 'googlecloud'

    @property
    def path(self) -> str:
        return self.build_path(self.raw.get('object_name', None))

    @staticmethod
    def get_metadata_from_resp_headers(obj_name: str, resp_headers: dict) -> dict:
        """Retrieve the metadata from HTTP response headers.

        Refer to the example JSON file "tests/googlecloud/fixtures/metadata/file-raw.json" and
        "tests/googlecloud/fixtures/metadata/folder-raw.json" for what metadata Google Cloud
        Storage provides via HTTP response headers.  Google sees both files and folders as objects.

        Google provides several customized headers that contains metadata WB needs:

            CRC32C and MD5: "x-goog-hash"
            https://cloud.google.com/storage/docs/xml-api/reference-headers#xgooghash
            "A request and response header for expressing an object's MD5 and/or CRC32C base64-
            encoded checksums.  Cloud Storage stores MD5 hashes for all non-composite objects.
            CRC32Cs are available for all objects."  Here are the three possible values returned:
                md5=<base64-encoded-md5>
                crc32c=<base64-encoded-crc32c>
                md5=<base64-encoded-md5>,crc32c=<base64-encoded-crc32c>

            SIZE: "x-goog-stored-content-length"
            https://cloud.google.com/storage/docs/xml-api/reference-headers#xgoogstoredcontentlength
            "A response header that indicates the content length (in bytes) of the object as stored
            in Cloud Storage, independent of any server-driven negotiation that might occur for
            individual requests for the object."  If for whatever reason this header does not exist,
            use value in "Content-Length" instead.

            Version: "x-goog-generation"
            https://cloud.google.com/storage/docs/xml-api/reference-headers#xgooggeneration
            A response header that indicates which version of the object data you are accessing.

        There are two pieces of information that are missing from the header: one for  "created_utc"
        and the other for "path".  Set "created_utc" to ``None`` and build the path from the first
        argument "object_name"

        :param obj_name: the "Object Name" of the object
        :param resp_headers: the response headers of the metadata HEAD request
        :rtype dict:
        """

        # HTTP Response Headers
        etag = resp_headers.get('etag', None)
        content_length = resp_headers.get('content-length', None)
        content_type = resp_headers.get('content-type', None)
        last_modified = resp_headers.get('last-modified', None)

        # Google's Customized Headers
        stored_content_length = resp_headers.get('x-goog-stored-content-length', None)
        generation = resp_headers.get('x-goog-generation', None)

        # TODO: verify the assumption that file objects always have both CRC32C and MD5
        google_hash = resp_headers.get('x-goog-hash', None)
        pattern = r'((crc32c)=(.*==),(md5)=(.*==))|((md5)=(.*==),(crc32c)=(.*==))'
        match = re.match(pattern, google_hash)

        if not match:
            raise MetadataError('Fail to parse HTTP response header: "x-goog-hash"')
        start_index = 2 if match.group(1) else 7 if match.group(6) else None
        if not start_index:
            raise MetadataError('Fail to parse HTTP response header: "x-goog-hash"')
        hashes = {
            match.group(start_index): decode_and_hexlify_hashes(match.group(start_index + 1)),
            match.group(start_index + 2): decode_and_hexlify_hashes(match.group(start_index + 3)),
        }

        return {
            'object_name': obj_name,
            'content_type': content_type,
            'last_modified': last_modified,
            'size': stored_content_length or content_length,
            'etag': etag,
            'extra': {
                'generation': generation,
                'hashes': hashes,
            }
        }


class GoogleCloudFileMetadata(BaseGoogleCloudMetadata, metadata.BaseFileMetadata):
    """The ``GoogleCloudFileMetadata`` object provides the full structure for files on the Google
    Cloud Storage Provider.  It inherits two non-abstract classes: ``BaseGoogleCloudMetadata`` and
    ``core_metadata.BaseFileMetadata``.
    """

    @property
    def name(self) -> str:
        return os.path.split(self.path)[1]

    @property
    def content_type(self) -> typing.Union[str, None]:
        return self.raw.get('content_type', None)

    @property
    def modified(self) -> typing.Union[str, None]:
        return self.raw.get('last_modified', None)

    @property
    def created_utc(self) -> typing.Union[str, None]:
        # Google's Cloud Storage does not provide creation time through XML API.
        return None

    @property
    def size(self) -> typing.Union[str, None]:
        size = self.raw.get('size', None)
        return int(size) if size else None

    @property
    def etag(self) -> typing.Union[str, None]:
        return self.raw.get('etag', None)

    @property
    def extra(self) -> dict:
        return self.raw.get('extra', None)


class GoogleCloudFolderMetadata(BaseGoogleCloudMetadata, metadata.BaseFolderMetadata):
    """The ``GoogleCloudFolderMetadata`` object provides the full structure for folders on Google
    Cloud Storage Provider.  It inherits two non-abstract classes: ``BaseGoogleCloudMetadata`` and
    ``core_metadata.BaseFolderMetadata``.
    """

    @property
    def name(self) -> str:
        return os.path.split(self.path.rstrip('/'))[1]
