import abc
import os
import logging

from waterbutler.core import utils as core_utils
from waterbutler.core import metadata as core_metadata
from waterbutler.providers.googlecloud import settings as pd_settings

logger = logging.getLogger(__name__)


class BaseGoogleCloudMetadata(core_metadata.BaseMetadata, metaclass=abc.ABCMeta):
    """The ``BaseGoogleCloudMetadata`` object provides the base structure for both file and folder
    metadata on the Google Cloud Storage.  It is an abstract class and does not implements all
    abstract methods and properties in ``core_metadata.BaseMetadata``.
    """

    @property
    def provider(self) -> str:
        return pd_settings.NAME

    @property
    def path(self) -> str:
        return self.build_path(self.raw.get('name', None))

    @property
    def etag(self) -> str:
        return self.raw['etag']

    @property
    def extra(self) -> dict:
        return {
            'id': self.raw.get('id', None),
            'bucket': self.raw.get('bucket', None),
            'generation': self.raw.get('generation', None),
            'hashes': {
                'md5': self.raw.get('md5Hash', None)
            },
        }


class GoogleCloudFileMetadata(BaseGoogleCloudMetadata, core_metadata.BaseFileMetadata):
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
    def created(self) -> str:
        return self.raw.get('timeCreated', None)

    @property
    def created_utc(self) -> str:
        return core_utils.normalize_datetime(self.created)

    @property
    def size(self) -> int:
        return int(self.raw.get('size', None))


class GoogleCloudFolderMetadata(BaseGoogleCloudMetadata, core_metadata.BaseFolderMetadata):
    """The ``GoogleCloudFolderMetadata`` object provides the full structure for folders on Google
    Cloud Storage Provider.  It inherits two non-abstract classes: ``BaseGoogleCloudMetadata`` and
    ``core_metadata.BaseFolderMetadata``.

    Please refer to the example JSON files "tests/googlecloud/fixtures/metadata/folder-itself.json"
    for what metadata Google Cloud Storage provides.
    """

    @property
    def name(self) -> str:
        return os.path.split(self.path.rstrip('/'))[1]
