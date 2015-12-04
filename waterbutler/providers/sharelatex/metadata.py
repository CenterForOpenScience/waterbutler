from waterbutler.core import metadata


class BaseShareLatexMetadata(metadata.BaseMetadata):

    @property
    def provider(self):
        return 'sharelatex'


class ShareLatexFileMetadata(BaseShareLatexMetadata, metadata.BaseFileMetadata):

    @property
    def name(self):
        return self.raw['name']

    @property
    def path(self):
        return self.raw['path']

    @property
    def size(self):
        return self.raw['size']

    @property
    def modified(self):
        return None

    @property
    def content_type(self):
        return self.raw['mimetype']

    @property
    def extra(self):
        return {
            'status': 'ok',
        }


class ShareLatexFolderMetadata(BaseShareLatexMetadata, metadata.BaseFolderMetadata):

    @property
    def name(self):
        return self.raw['name']

    @property
    def path(self):
        return self.raw['path']
