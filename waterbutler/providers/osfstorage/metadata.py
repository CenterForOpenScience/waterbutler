from waterbutler.core import metadata


class BaseOsfStorageMetadata:
    @property
    def provider(self):
        return 'osfstorage'


class BaseOsfStorageItemMetadata(BaseOsfStorageMetadata):

    def __init__(self, raw, materialized):
        super().__init__(raw)
        self._materialized = materialized

    @property
    def name(self):
        return self.raw['name']

    @property
    def path(self):
        return self.raw['path']

    @property
    def materialized_path(self):
        return self._materialized


class OsfStorageFileMetadata(BaseOsfStorageItemMetadata, metadata.BaseFileMetadata):

    @property
    def modified(self):
        return self.raw['modified']

    @property
    def size(self):
        return self.raw['size']

    @property
    def content_type(self):
        return self.raw.get('contentType')

    @property
    def etag(self):
        return '{}::{}'.format(self.raw['version'], self.path)

    @property
    def extra(self):
        return {
            'version': self.raw['version'],
            'downloads': self.raw['downloads'],
            'checkout_user': self.raw['checkout_user'],
            'hashes': {
                'md5': self.raw['md5'],
                'sha256': self.raw['sha256']
            },
        }


class OsfStorageFolderMetadata(BaseOsfStorageItemMetadata, metadata.BaseFolderMetadata):
    pass


class OsfStorageRevisionMetadata(BaseOsfStorageMetadata, metadata.BaseFileRevisionMetadata):

    @property
    def modified(self):
        return self.raw['date']

    @property
    def version_identifier(self):
        return 'version'

    @property
    def version(self):
        return str(self.raw['index'])

    @property
    def extra(self):
        return {
            'user': self.raw['user'],
            'downloads': self.raw['downloads'],
            'hashes': {
                'md5': self.raw['md5'],
                'sha256': self.raw['sha256']
            },
        }
