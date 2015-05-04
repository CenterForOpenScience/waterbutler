import os

from waterbutler.core import metadata


class OwnCloudMetadata(metadata.BaseMetadata):

    @property
    def provider(self):
        return 'owncloud'

    @property
    def name(self):
        return os.path.basename(self.path)


class OwnCloudFileMetadata(OwnCloudMetadata, metadata.BaseFileMetadata):

    def __init__(self, path, file_attrs):
        super().__init__(file_attrs)
        self._path = path

    @property
    def path(self):
        return self.build_path(self._path)

    @property
    def size(self):
        if '{DAV:}getcontentlength' in self.raw:
            return int(self.raw['{DAV:}getcontentlength'])
        return None

    @property
    def modified(self):
        if '{DAV:}getlastmodified' in self.raw:
            return self.raw['{DAV:}getlastmodified'],
        return None

    @property
    def content_type(self):
        if '{DAV:}getcontenttype' in self.raw:
            return self.raw['{DAV:}getcontenttype']
        return None

    @property
    def extra(self):
        return {
            'etag': self.raw['{DAV:}getetag']
        }
