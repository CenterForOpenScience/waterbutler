import os

from waterbutler.core import metadata


class BaseCloudFilesMetadata(metadata.BaseMetadata):

    @property
    def provider(self):
        return 'cloudfiles'


class CloudFilesFileMetadata(BaseCloudFilesMetadata, metadata.BaseFileMetadata):

    @property
    def name(self):
        return os.path.split(self.raw['name'])[1]

    @property
    def path(self):
        return self.build_path(self.raw['name'])

    @property
    def size(self):
        return self.raw['bytes']

    @property
    def modified(self):
        return self.raw['last_modified']

    @property
    def created_utc(self):
        return None

    @property
    def content_type(self):
        return self.raw['content_type']

    @property
    def etag(self):
        return self.raw['hash']

    @property
    def extra(self):
        return {
            'hashes': {
                'md5': self.raw['hash'],
            },
        }


class CloudFilesHeaderMetadata(BaseCloudFilesMetadata, metadata.BaseFileMetadata):

    def __init__(self, raw, path):
        super().__init__(raw)
        self._path = path

    def to_revision(self):
        revison_dict = {'bytes': self.size,
                        'name': self.name,
                        'last_modified': self.modified,
                        'content_type': self.content_type}

        return CloudFilesRevisonMetadata(revison_dict)

    @property
    def kind(self):
        return 'folder' if self._path.is_dir else 'file'

    @property
    def name(self):
        return self._path.name

    @property
    def path(self):
        return self._path.materialized_path

    @property
    def size(self):
        return int(self.raw['CONTENT-LENGTH'])

    @property
    def modified(self):
        return self.raw['LAST-MODIFIED']

    @property
    def created_utc(self):
        return None

    @property
    def content_type(self):
        return self.raw['CONTENT-TYPE']

    @property
    def etag(self):
        return self.raw['ETAG'].replace('"', '')

    @property
    def extra(self):
        return {
            'hashes': {
                'md5': self.raw['ETAG'].replace('"', '')
            },
        }


class CloudFilesFolderMetadata(BaseCloudFilesMetadata, metadata.BaseFolderMetadata):

    @property
    def name(self):
        return os.path.split(self.raw['subdir'].rstrip('/'))[1]

    @property
    def path(self):
        return self.build_path(self.raw['subdir'])


class CloudFilesRevisonMetadata(metadata.BaseFileRevisionMetadata):

    @property
    def version_identifier(self):
        return 'revision'

    @property
    def version(self):
        return self.raw['name']

    @property
    def modified(self):
        return self.raw['last_modified']

    @property
    def size(self):
        return self.raw['bytes']

    @property
    def name(self):
        return self.raw['name']

    @property
    def content_type(self):
        return self.raw['content_type']
