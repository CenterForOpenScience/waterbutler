import os
import abc
import base64
import logging
import binascii

from waterbutler.core import utils
from waterbutler.core import metadata

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
        return self.build_path(self.raw.get('name', None))


class GoogleCloudFileMetadata(BaseGoogleCloudMetadata, metadata.BaseFileMetadata):
    """The ``GoogleCloudFileMetadata`` object provides the full structure for files on the Google
    Cloud Storage Provider.  It inherits two non-abstract classes: ``BaseGoogleCloudMetadata`` and
    ``core_metadata.BaseFileMetadata``.

    Please refer to the example JSON files "tests/googlecloud/fixtures/metadata/file-itself.json"
    for what metadata Google Cloud Storage provides.
    """

    @property
    def name(self) -> str:
        return os.path.split(self.path)[1]

    @property
    def content_type(self) -> str:
        return self.raw.get('contentType', None)

    @property
    def modified(self) -> str:
        return self.raw.get('updated', None)

    @property
    def created_utc(self) -> str:
        time_created = self.raw.get('timeCreated', None)
        return utils.normalize_datetime(time_created) if time_created else None

    @property
    def size(self) -> int:
        size = self.raw.get('size', None)
        return int(size) if size else None

    @property
    def etag(self) -> str:
        return self.raw.get('etag', None)

    @property
    def extra(self) -> dict:

        # Convert the base64 encoded MD5 hash to hex digest representation ``.encode()`` is called
        # to convert str to bytes and ``.decode()`` is called to convert bytes to str
        md5_hex_digest = None
        md5_base64_encoded = self.raw.get('md5Hash', None)
        if md5_base64_encoded:
            md5 = base64.b64decode(md5_base64_encoded.encode())
            md5_hex_digest = binascii.hexlify(md5).decode()

        return {
            'id': self.raw.get('id', None),
            'bucket': self.raw.get('bucket', None),
            'generation': self.raw.get('generation', None),
            'hashes': {
                'md5': md5_hex_digest,
                'crc32c': self.raw.get('crc32c', None),
            },
        }


class GoogleCloudFolderMetadata(BaseGoogleCloudMetadata, metadata.BaseFolderMetadata):
    """The ``GoogleCloudFolderMetadata`` object provides the full structure for folders on Google
    Cloud Storage Provider.  It inherits two non-abstract classes: ``BaseGoogleCloudMetadata`` and
    ``core_metadata.BaseFolderMetadata``.

    Please refer to the example JSON files "tests/googlecloud/fixtures/metadata/folder-itself.json"
    for what metadata Google Cloud Storage provides.
    """

    @property
    def name(self) -> str:
        return os.path.split(self.path.rstrip('/'))[1]
