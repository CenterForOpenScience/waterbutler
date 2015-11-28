import os

from waterbutler.core import metadata


class BaseOneDriveMetadata(metadata.BaseMetadata):

    def __init__(self, raw, folder):
        super().__init__(raw)
        self._folder = folder

    @property
    def provider(self):
        return 'onedrive'

    def build_path(self, path):
        # TODO write a test for this
        if path.lower().startswith(self._folder.lower()):
            path = path[len(self._folder):]
        return super().build_path(path)

    @property
    def extra(self):
        return {
            'revisionId': self.raw['cTag'] #TODO: rev?
        }


class OneDriveFolderMetadata(BaseOneDriveMetadata, metadata.BaseFolderMetadata):

    @property
    def name(self):
        return os.path.split(self.raw['name'])[1]

    @property
    def path(self):
        return self.build_path(self.raw['id'])


class OneDriveFileMetadata(BaseOneDriveMetadata, metadata.BaseFileMetadata):

    @property
    def name(self):
        return os.path.split(self.raw['name'])[1]

    @property
    def path(self):
        return self.build_path(self.raw['id'])

    @property
    def size(self):
        return self.raw['size']

    @property
    def modified(self):
        return self.raw['lastModifiedDateTime']

    @property
    def content_type(self):
        return 'foo-app'
        return self.raw['file']['mimeType'] #TODO: pull from file['mimetype'] - https://dev.onedrive.com/facets/file_facet.htm

    @property
    def etag(self):
        return self.raw['eTag']


# TODO dates!
class OneDriveRevision(metadata.BaseFileRevisionMetadata):

    @property
    def version_identifier(self):
        return 'revision'

    @property
    def version(self):
        return self.raw['eTag']

    @property
    def modified(self):
        return self.raw['lastModifiedDateTime']
