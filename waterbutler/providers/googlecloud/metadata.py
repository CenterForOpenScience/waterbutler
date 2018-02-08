import os
import abc
import logging

from waterbutler.core import utils
from waterbutler.core import metadata
from waterbutler.providers.googlecloud import settings

logger = logging.getLogger(__name__)


class BaseGoogleCloudMetadata(metadata.BaseMetadata, metaclass=abc.ABCMeta):
    """The ``BaseGoogleCloudMetadata`` object provides the base structure for both file and folder
    metadata on the Google Cloud Storage.  It is an abstract class and does not implements all
    abstract methods and properties in ``core_metadata.BaseMetadata``.
    """

    @property
    def provider(self) -> str:
        return settings.NAME

    @property
    def path(self) -> str:
        return self.build_path(self.raw.get('name', None))


class GoogleCloudFileMetadata(BaseGoogleCloudMetadata, metadata.BaseFileMetadata):
    """The ``GoogleCloudFileMetadata`` object provides the full structure for files on the Google
    Cloud Storage Provider.  It inherits two non-abstract classes: ``BaseGoogleCloudMetadata`` and
    ``core_metadata.BaseFileMetadata``.

    TODO: what should we do if expected properties do not exist?

    Please refer to the example JSON files "tests/googlecloud/fixtures/metadata/file-itself.json"
    for what metadata Google Cloud Storage provides.
    """

    @property
    def name(self) -> str:
        return os.path.split(self.path)[1]

    @property
    def content_type(self) -> str:
        return self.raw.get('contentType', '')

    @property
    def modified(self) -> str:
        return self.raw.get('updated', '')

    @property
    def created_utc(self) -> str:
        time_created = self.raw.get('timeCreated', None)
        return utils.normalize_datetime(time_created) if time_created else ''

    @property
    def size(self) -> int:
        size = self.raw.get('size', None)
        return int(self.raw.get('size', None)) if size else -1

    @property
    def etag(self) -> str:
        return self.raw.get('etag', '')

    @property
    def extra(self) -> dict:

        # TODO: store the (base64) decoded the md5Hash

        return {
            'id': self.raw.get('id', ''),
            'bucket': self.raw.get('bucket', ''),
            'generation': self.raw.get('generation', ''),
            'hashes': {
                'md5': self.raw.get('md5Hash', '')
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
