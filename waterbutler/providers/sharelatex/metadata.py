import os

from waterbutler.core import metadata


class BaseShareLatexMetadata(metadata.BaseMetadata):

    @property
    def provider(self):
        return 'sharelatex'


class ShareLatexFileMetadata(BaseShareLatexMetadata, metadata.BaseFileMetadata):

    @property
    def name(self):
        return os.path.split(self.raw['path'])[1]

    @property
    def path(self):
        return self.build_path(self.raw['path'])

    @property
    def size(self):
        return self.raw['size']

    @property
    def modified(self):
        return self.raw['modified']

    @property
    def content_type(self):
        return self.raw['mimetype']

    @property
    def extra(self):
        return {
            'status': 'ok',
        }


class ShareLatexProjectMetadata(BaseShareLatexMetadata, metadata.BaseFolderMetadata):

    @property
    def name(self):
        return os.path.split(self.raw['path'])[1]

    @property
    def path(self):
        return self.build_path(self.raw['path'])
