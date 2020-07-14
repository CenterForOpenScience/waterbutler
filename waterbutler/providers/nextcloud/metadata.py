from waterbutler.core import metadata


class BaseNextcloudMetadata(metadata.BaseMetadata):

    def __init__(self, href, folder, provider, attributes=None):
        super(BaseNextcloudMetadata, self).__init__(None)
        self.attributes = attributes or {}
        self._folder = folder
        self._href = href
        self._provider = provider

    @property
    def provider(self):
        return self._provider

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
        if '{DAV:}getetag' in self.attributes:
            return str(self.attributes['{DAV:}getetag'])
        return None

    @property
    def modified(self):
        if '{DAV:}getlastmodified' in self.attributes:
            return self.attributes['{DAV:}getlastmodified']
        return None

    @property
    def created_utc(self):
        return None


class NextcloudFileMetadata(BaseNextcloudMetadata, metadata.BaseFileMetadata):

    @property
    def content_type(self):
        if '{DAV:}getcontenttype' in self.attributes:
            return str(self.attributes['{DAV:}getcontenttype'])
        return None

    @property
    def fileid(self):
        if '{http://owncloud.org/ns}fileid' in self.attributes:
            return str(self.attributes['{http://owncloud.org/ns}fileid'])
        return None


class NextcloudFolderMetadata(BaseNextcloudMetadata, metadata.BaseFolderMetadata):

    @property
    def content_type(self):
        if '{DAV:}getcontenttype' in self.attributes:
            return str(self.attributes['{DAV:}getcontenttype'])
        return 'httpd/unix-directory'


class NextcloudFileRevisionMetadata(metadata.BaseFileRevisionMetadata):

    def __init__(self, version, metadata):
        self._metadata = metadata
        self._version = version
        self._modified = self._metadata.modified
        self._md5 = ''
        self._sha256 = ''

    @classmethod
    def from_metadata(cls, revision, metadata):
        return NextcloudFileRevisionMetadata(revision, metadata)

    @property
    def version_identifier(self):
        return 'revision'

    @property
    def version(self):
        return self._version

    @property
    def modified(self):
        return self._modified

    @property
    def extra(self):
        return {
            'hashes': {
                'md5': self._md5,
                'sha256': self._sha256
            },
        }
