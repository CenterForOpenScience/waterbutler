from waterbutler.core import metadata


class BaseNextcloudMetadata(metadata.BaseMetadata):

    def __init__(self, href, folder, attributes=None):
        super(BaseNextcloudMetadata, self).__init__(None)
        self.attributes = attributes or {}
        self._folder = folder
        self._href = href

    @property
    def provider(self):
        return 'nextcloud'

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


class NextcloudFileMetadata(BaseNextcloudMetadata, metadata.BaseFileMetadata):

    @property
    def content_type(self):
        if '{DAV:}getcontenttype' in self.attributes:
            return str(self.attributes['{DAV:}getcontenttype'])
        return None


class NextcloudFolderMetadata(BaseNextcloudMetadata, metadata.BaseFolderMetadata):

    @property
    def content_type(self):
        if '{DAV:}getcontenttype' in self.attributes:
            return str(self.attributes['{DAV:}getcontenttype'])
        return 'httpd/unix-directory'


class NextcloudFileRevisionMetadata(metadata.BaseFileRevisionMetadata):

    def __init__(self, modified):
        self._modified = modified

    @classmethod
    def from_metadata(cls, metadata):
        return NextcloudFileRevisionMetadata(modified=metadata.modified)

    @property
    def version_identifier(self):
        return 'revision'

    @property
    def version(self):
        return 'latest'

    @property
    def modified(self):
        return self._modified
