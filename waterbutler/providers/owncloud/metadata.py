from waterbutler.core import metadata


class BaseOwnCloudMetadata(metadata.BaseMetadata):

    def __init__(self, href, folder, attributes=None):
        super().__init__(None)
        self.attributes = attributes or {}
        self._folder = folder
        self._href = href

    def _dehydrate(self):
        payload = super()._dehydrate()
        payload['attributes'] = self.attributes
        payload['_folder'] = self._folder
        payload['_href'] = self._href
        return payload

    @classmethod
    def _rehydrate(cls, payload):
        args = super()._rehydrate(payload)
        args.append(payload['_href'], payload['_folder'], attributes=payload['attributes'])
        return args

    @property
    def provider(self):
        return 'owncloud'

    @property
    def name(self):
        return self._href.strip('/').split('/')[-1]

    @property
    def path(self):
        path = self._href[len(self._folder) - 1:]
        return path

    @property
    def size(self):
        if '{DAV:}getcontentlength' in self.attributes:
            return str(int(self.attributes['{DAV:}getcontentlength']))
        return None

    @property
    def etag(self):
        return str(self.attributes['{DAV:}getetag'])

    @property
    def modified(self):
        return self.attributes['{DAV:}getlastmodified']

    @property
    def created_utc(self):
        return None


class OwnCloudFileMetadata(BaseOwnCloudMetadata, metadata.BaseFileMetadata):

    @property
    def content_type(self):
        if '{DAV:}getcontenttype' in self.attributes:
            return str(self.attributes['{DAV:}getcontenttype'])
        return None


class OwnCloudFolderMetadata(BaseOwnCloudMetadata, metadata.BaseFolderMetadata):

    @property
    def content_type(self):
        if '{DAV:}getcontenttype' in self.attributes:
            return str(self.attributes['{DAV:}getcontenttype'])
        return 'httpd/unix-directory'


class OwnCloudFileRevisionMetadata(metadata.BaseFileRevisionMetadata):

    def __init__(self, modified):
        self._modified = modified
        super().__init__({'modified': modified})

    def _dehydrate(self):
        payload = super()._dehydrate()
        payload['_modified'] = self._modified
        return payload

    @staticmethod
    def _rehydrate(cls, payload):
        args = super()._rehydrate(payload)
        args.append(payload['_modified'])
        return args

    @classmethod
    def from_metadata(cls, file_metadata_object):
        return OwnCloudFileRevisionMetadata(modified=file_metadata_object.modified)

    @property
    def version_identifier(self):
        return 'revision'

    @property
    def version(self):
        return 'latest'

    @property
    def modified(self):
        return self._modified
