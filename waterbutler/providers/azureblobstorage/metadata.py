import os

from waterbutler.core import metadata


class AzureBlobStorageMetadata(metadata.BaseMetadata):

    @property
    def provider(self):
        return 'azureblobstorage'

    @property
    def name(self):
        return os.path.split(self.path)[1]

    @property
    def created_utc(self):
        return None


class AzureBlobStorageFileMetadataHeaders(AzureBlobStorageMetadata, metadata.BaseFileMetadata):

    def __init__(self, path, headers):
        self._path = path
        # Cast to dict to clone as the headers will
        # be destroyed when the request leaves scope
        super().__init__(headers)

    @property
    def path(self):
        return '/' + self._path

    @property
    def size(self):
        return self.raw.properties.content_length

    @property
    def content_type(self):
        return self.raw.properties.content_settings.content_type

    @property
    def modified(self):
        return self.raw.properties.last_modified.strftime('%Y-%m-%d %H:%M:%S')

    @property
    def etag(self):
        return self.raw.properties.content_settings.content_md5

    @property
    def extra(self):
        return {
            'md5': self.raw.properties.content_settings.content_md5
        }


class AzureBlobStorageFileMetadata(AzureBlobStorageMetadata, metadata.BaseFileMetadata):

    @property
    def path(self):
        return '/' + self.raw.name

    @property
    def size(self):
        return int(self.raw.properties.content_length)

    @property
    def modified(self):
        return self.raw.properties.last_modified.strftime('%Y-%m-%d %H:%M:%S')

    @property
    def content_type(self):
        return self.raw.properties.content_settings.content_type

    @property
    def etag(self):
        return self.raw.properties.content_settings.content_md5

    @property
    def extra(self):
        return {
            'md5': self.raw.properties.content_settings.content_md5
        }


class AzureBlobStorageFolderMetadata(AzureBlobStorageMetadata, metadata.BaseFolderMetadata):

    @property
    def name(self):
        return self.raw['prefix'].split('/')[-2]

    @property
    def path(self):
        return '/' + self.raw['prefix']
