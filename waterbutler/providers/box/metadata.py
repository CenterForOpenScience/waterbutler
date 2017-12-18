from waterbutler.core import metadata, utils


class BaseBoxMetadata(metadata.BaseMetadata):

    def __init__(self, raw, path_obj):
        super().__init__(raw)
        self._path_obj = path_obj

    @property
    def provider(self):
        return 'box'

    @property
    def materialized_path(self):
        return str(self._path_obj)


class BoxFolderMetadata(BaseBoxMetadata, metadata.BaseFolderMetadata):

    @property
    def name(self):
        return self.raw['name']

    @property
    def path(self):
        return '/{}/'.format(self.raw['id'])


class BoxFileMetadata(BaseBoxMetadata, metadata.BaseFileMetadata):

    @property
    def name(self):
        return self.raw['name']

    @property
    def path(self):
        return '/{0}'.format(self.raw['id'])

    @property
    def size(self):
        return self.raw.get('size')

    @property
    def modified(self):
        return self.raw.get('modified_at')

    @property
    def created_utc(self):
        return utils.normalize_datetime(self.raw.get('created_at'))

    @property
    def content_type(self):
        return None

    @property
    def extra(self):
        return {
            'etag': self.raw.get('etag'),
            'hashes': {
                'sha1': self.raw.get('sha1')
            }
        }

    @property
    def etag(self):
        return '{}::{}'.format(self.raw.get('etag', ''), self.raw['id'])


class BoxRevision(metadata.BaseFileRevisionMetadata):

    @property
    def version(self):
        return self.raw['id']

    @property
    def version_identifier(self):
        return 'revision'

    @property
    def modified(self):
        return self.raw['modified_at']
