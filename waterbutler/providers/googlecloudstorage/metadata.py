import abc
import os
import logging

from waterbutler.core import utils as core_utils
from waterbutler.core import metadata as core_metadata
from waterbutler.providers.googlecloudstorage import settings as pd_settings

logger = logging.getLogger(__name__)


class GoogleCloudStorageMetaData(core_metadata.BaseMetadata, metaclass=abc.ABCMeta):
    """The GoogleCloudStorage object provides the base structure for both file and folder metadata
    on the Google Cloud Storage.  It is an abstract class and does not implements all abstract
    methods and properties in ``core_metadata.BaseMetadata``.
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
            'kind': self.raw.get('kind', None),
            'id': self.raw.get('id', None),
            'bucket': self.raw.get('bucket', None),
            'hashes': {
                'md5': self.raw.get('md5Hash', None)
            },
            'crc32c': self.raw.get('crc32c', None)
        }


class GoogleCloudStorageFileMetadata(GoogleCloudStorageMetaData, core_metadata.BaseFileMetadata):

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


class GoogleCloudStorageFolderMetadata(GoogleCloudStorageMetaData, core_metadata.BaseFolderMetadata):

    @property
    def name(self) -> str:
        return os.path.split(self.path.rstrip('/'))[1]
