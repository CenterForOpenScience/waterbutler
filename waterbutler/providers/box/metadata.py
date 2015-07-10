from waterbutler.core import metadata


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

    def __init__(self, raw, path_obj, view_url=None):
        super().__init__(raw, path_obj)
        self.view_url = view_url

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
    def content_type(self):
        return None

    @property
    def extra(self):
        return {
            'etag': self.raw.get('etag'),
            'viewUrl': self.view_url
        }

    @property
    def etag(self):
        return '{}::{}'.format(self.raw.get('etag', ''), self.raw['id'])


class BoxRevision(metadata.BaseFileRevisionMetadata):

    @property
    def version(self):
        try:
            return self.raw['id']
        except KeyError:
            return self.raw['path'].split('/')[1]

    @property
    def version_identifier(self):
        return 'revision'

    @property
    def path(self):
        try:
            return '/{0}/{1}'.format(self.raw['id'], self.raw['name'])
        except KeyError:
            return self.raw.get('path')

    @property
    def modified(self):
        try:
            return self.raw['modified_at']
        except KeyError:
            return self.raw.get('modified')
