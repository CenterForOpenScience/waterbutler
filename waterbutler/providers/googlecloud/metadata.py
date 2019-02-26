import os
import re
import abc
import typing
import logging

from multidict import MultiDict, MultiDictProxy

from waterbutler.core import metadata
from waterbutler.core.exceptions import MetadataError
from waterbutler.providers.googlecloud.utils import decode_and_hexlify_hashes

logger = logging.getLogger(__name__)


class BaseGoogleCloudMetadata(metadata.BaseMetadata, metaclass=abc.ABCMeta):
    """This class provides the base structure of both files and folders metadata for the
    :class:`.GoogleCloudProvider`.  It is an abstract class and does not implement all abstract
    methods and properties in :class:`.BaseMetadata`.

    Quirks:

    * Google sees both files and folders as objects.
    """

    @property
    def provider(self) -> str:
        return 'googlecloud'

    @property
    def path(self) -> str:
        return self.build_path(self.raw.get('object_name', None))

    @classmethod
    def get_metadata_from_resp_headers(cls, obj_name: str, resp_headers: MultiDictProxy) -> dict:
        """Retrieve the metadata from HTTP response headers.

        Quirks - Google Cloud Customized Headers:

            Google provides several customized headers that contain what WB needs for metadata.

            **CRC32C and MD5: "x-goog-hash"**

            See: https://cloud.google.com/storage/docs/xml-api/reference-headers#xgooghash

            "A request and response header for expressing an object's MD5 and/or CRC32C base64-
            encoded checksums.  Cloud Storage stores MD5 hashes for all non-composite objects.
            CRC32Cs are available for all objects."

            According to the RFC: http://www.rfc-editor.org/rfc/rfc4648.txt, there are two Base 64
            Alphabets: 1. The Standard Base 64 Alphabet: [A-Za-z0-9+/=] and 2.  The "URL and File-
            name safe" Base 64 Alphabet: [A-Za-z0-9-_=].  Google Cloud uses the standard one.

            **SIZE: "x-goog-stored-content-length"**

            https://cloud.google.com/storage/docs/xml-api/reference-headers#xgoogstoredcontentlength

            "A response header that indicates the content length (in bytes) of the object as stored
            in Cloud Storage, independent of any server-driven negotiation that might occur for
            individual requests for the object." Do not use the "Content-Length" header, it is the
            length of the response body, not the size of the object.

            **Version: "x-goog-generation"**

            https://cloud.google.com/storage/docs/xml-api/reference-headers#xgooggeneration

            "A response header that indicates which version of the object data you are accessing."

            There are two pieces of information that are missing from the header: one for ``path``
            and the other for ``created_utc``.  Set ``created_utc`` to ``None`` and build the path
            from the ``obj_name``.

        Quirks - `aiohttp` Response Headers:

            `aiohttp` is able to parse the raw hash header, retrieve both hashes and store them in
            a dictionary where one key can have multiple values. This ``resp_headers`` is of type
            :class:`aiohttp.CIMultiDictProxy`, which is immutable. WB calls its ``.getall(key)``
            method to return a list of all values that matches the key.

            The raw hash google header ``x-goog-hash: crc32c=Tf8tmw==,md5=mkaUfJxiLXeSEl2OpExGOA==``
            becomes ``{"x-goog-hash": "crc32c=Tf8tmw==", "md5": "mkaUfJxiLXeSEl2OpExGOA=="}``

        :param obj_name: the "Object Name" of the file or folder
        :param resp_headers: the response headers of the metadata request
        :rtype: dict
        """

        # HTTP Response Headers
        etag = resp_headers.get('etag', None).strip('"')
        content_type = resp_headers.get('content-type', None)
        last_modified = resp_headers.get('last-modified', None)

        # Google's Customized Headers
        stored_content_length = resp_headers.get('x-goog-stored-content-length', None)
        generation = resp_headers.get('x-goog-generation', None)

        # Obtain the CRC32C and MD5 hashes
        google_hash_list = resp_headers.getall('x-goog-hash', None)
        if not google_hash_list:
            raise MetadataError('Missing header "x-goog-hash"')

        # Parse and convert the hashes
        pattern = r'(crc32c|md5)=([A-Za-z0-9+/=]+)'
        google_hashes = {}
        for google_hash in google_hash_list:
            match = re.match(pattern, google_hash)
            if not match:
                raise MetadataError('Fail to parse HTTP response header: "x-goog-hash"')
            google_hashes.update({match.group(1): decode_and_hexlify_hashes(match.group(2))})

        # Return a Python ``dict`` that can be used directly for metadata initialization
        return {
            'object_name': obj_name,
            'content_type': content_type,
            'last_modified': last_modified,
            'size': stored_content_length,
            'etag': etag,
            'extra': {
                'generation': generation,
                'hashes': google_hashes,
            }
        }


class GoogleCloudFileMetadata(BaseGoogleCloudMetadata, metadata.BaseFileMetadata):
    """This class provides a full structure of the files for the :class:`.GoogleCloudProvider`.  It
    inherits two concrete classes: :class:`.BaseGoogleCloudMetadata` and :class:`.BaseFileMetadata`.

    Refer to the file ``tests/providers/googlecloud/fixtures/metadata/file-raw.json`` for an
    example of the metadata Google Cloud Storage XML API returns via HTTP response headers.
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
    def size(self) -> typing.Union[str, None]:  # type: ignore
        size = self.raw.get('size', None)
        return int(size) if size else None  # type: ignore

    @property
    def etag(self) -> typing.Union[str, None]:
        return self.raw.get('etag', None)

    @property
    def extra(self) -> dict:
        return self.raw.get('extra', None)

    @classmethod
    def new_from_resp_headers(cls, obj_name: str,
                              resp_headers: typing.Union[MultiDict, MultiDictProxy]):
        """Construct an instance of :class:`.GoogleCloudFileMetadata` from the response headers
        returned.

        :param str obj_name: the object name
        :param resp_headers: the response headers
        :type resp_headers: :class:`aiohttp.MultiDict` or :class:`MultiDictProxy`
        :rtype: :class:`.GoogleCloudFileMetadata`
        """

        if not obj_name:
            raise MetadataError('Metadata init failed: missing object name')

        if not resp_headers:
            raise MetadataError('Metadata init failed: missing response headers.')

        if not (isinstance(resp_headers, MultiDict) or isinstance(resp_headers, MultiDictProxy)):
            raise MetadataError('Metadata init failed: invalid response headers.')

        parsed_resp_headers = cls.get_metadata_from_resp_headers(obj_name, resp_headers)

        return GoogleCloudFileMetadata(parsed_resp_headers)


class GoogleCloudFolderMetadata(BaseGoogleCloudMetadata, metadata.BaseFolderMetadata):
    """This class provides the full structure of the folders for the :class:`.GoogleCloudProvider`.
    It inherits two concrete classes: :class:`.BaseGoogleCloudMetadata` and
    :class:`.BaseFolderMetadata`.

    Refer to the file ``tests/providers/googlecloud/fixtures/metadata/folder-raw.json`` for an
    example of the metadata Google Cloud Storage XML API returns via HTTP response headers.
    """

    @property
    def name(self) -> str:
        return os.path.split(self.path.rstrip('/'))[1]

    @classmethod
    def new_from_resp_headers(cls, obj_name: str,
                              resp_headers: typing.Union[MultiDict, MultiDictProxy]):
        """Construct an instance of :class:`.GoogleCloudFolderMetadata` from the response headers
        returned.

        :param str obj_name: the object name
        :param resp_headers: the response headers
        :type resp_headers: :class:`aiohttp.MultiDict` or :class:`MultiDictProxy`
        :rtype: :class:`.GoogleCloudFolderMetadata`
        """

        if not obj_name:
            raise MetadataError('Metadata init failed: missing object name')

        if not resp_headers:
            raise MetadataError('Metadata init failed: missing response headers.')

        if not (isinstance(resp_headers, MultiDict) or isinstance(resp_headers, MultiDictProxy)):
            raise MetadataError('Metadata init failed: invalid response headers.')

        parsed_resp_headers = cls.get_metadata_from_resp_headers(obj_name, resp_headers)

        return GoogleCloudFolderMetadata(parsed_resp_headers)
