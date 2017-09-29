import os

from waterbutler.core import metadata


class BaseDropboxMetadata(metadata.BaseMetadata):

    def __init__(self, raw, folder):
        super().__init__(raw)
        self._folder = folder

    @property
    def provider(self):
        return 'dropbox'

    def build_path(self, path):
        # TODO write a test for this
        if path.lower().startswith(self._folder.lower()):
            path = path[len(self._folder):]
        return super().build_path(path)


class DropboxFolderMetadata(BaseDropboxMetadata, metadata.BaseFolderMetadata):

    @property
    def name(self):
        return os.path.split(self.raw['path_display'])[1]

    @property
    def path(self):
        return self.build_path(self.raw['path_display'])


class DropboxFileMetadata(BaseDropboxMetadata, metadata.BaseFileMetadata):

    @property
    def name(self):
        return os.path.split(self.raw['path_display'])[1]

    @property
    def path(self):
        return self.build_path(self.raw['path_display'])

    @property
    def size(self):
        return self.raw['size']

    @property
    def modified(self):
        return self.raw['server_modified']

    @property
    def created_utc(self):
        return None

    @property
    def content_type(self):
        return None

    @property
    def etag(self):
        return self.raw['rev']

    @property
    def extra(self):
        return {
            'revisionId': self.raw['rev'],
            'id': self.raw['id'],
            'hashes': {
                'dropbox': self.raw['content_hash'],
            },
        }


# TODO dates!
class DropboxRevision(metadata.BaseFileRevisionMetadata):

    @property
    def version_identifier(self):
        return 'revision'

    @property
    def version(self):
        return self.raw['rev']

    @property
    def modified(self):
        return self.raw['server_modified']

    @property
    def extra(self):
        return {
            'revisionId': self.raw['rev'],
            'id': self.raw['id']
        }
