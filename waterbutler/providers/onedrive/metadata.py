from waterbutler.core import metadata


class BaseOneDriveMetadata(metadata.BaseMetadata):

    def __init__(self, raw, path_obj):
        print('++++++++++++++++++++++++++++++ BaseOneDriveMetadata.__init__')
        print('+++++ raw: {}'.format(raw))
        print('+++++ path_obj: {}'.format(path_obj))
        super().__init__(raw)
        self._path_obj = path_obj

    @property
    def provider(self):
        return 'onedrive'

    @property
    def materialized_path(self):
        return '/{}/{}'.format(
            self.raw['parentReference']['path'].replace('/drive/root:/', ''),
            self.raw['name']
        )

    @property
    def extra(self):
        return {
            'id': self.raw['id'],
            'parentReference': self.raw['parentReference']['path']
        }

    @property
    def created_utc(self):
        return None


class OneDriveFolderMetadata(BaseOneDriveMetadata, metadata.BaseFolderMetadata):

    @property
    def name(self):
        return self.raw['name']

    @property
    def path(self):
        return '/{}/'.format(self.raw['id'])

    @property
    def etag(self):
        return self.raw.get('eTag')


class OneDriveFileMetadata(BaseOneDriveMetadata, metadata.BaseFileMetadata):

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
        return self.raw.get('lastModifiedDateTime')

    @property
    def content_type(self):
        if 'file' in self.raw.keys():
            return self.raw['file'].get('mimeType')
        return 'application/octet-stream'

    @property
    def extra(self):
        return {
            'id': self.raw.get('id'),
            'etag': self.raw.get('eTag'),
            'webView': self.raw.get('webUrl'),
        }

    @property
    def etag(self):
        return self.raw['eTag']


class OneDriveRevisionMetadata(metadata.BaseFileRevisionMetadata):

    @property
    def version_identifier(self):
        return 'revision'

    @property
    def version(self):
        return self.raw['eTag']

    @property
    def modified(self):
        return self.raw['lastModifiedDateTime']
