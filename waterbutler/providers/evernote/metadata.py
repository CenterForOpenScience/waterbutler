from waterbutler.core import metadata


class EvernoteFileMetadata(metadata.BaseFileMetadata):

    def __init__(self, raw):
        print('EvernoteFileMetadata.__init__ raw:', raw)
        super().__init__(raw)

    @property
    def id(self):
        return self.raw['guid']

    @property
    def content_type(self):
        return 'text/html'

    @property
    def modified(self):
        return self.raw['updated']

    @property
    def created_utc(self):
        return None

    @property
    def name(self):
        return self.raw['title'] + ".enml"

    @property
    def provider(self):
        return 'evernote'

    @property
    def size(self):
        return self.raw['length']

    @property
    def extra(self):
        return super(EvernoteFileMetadata, self).extra.update({
        })

    @property
    def path(self):
        return "/" + self.raw["guid"]

    @property
    def etag(self):
        _etag = '{}::{}'.format(self.raw["guid"], self.raw.get('updateSequenceNum', ''))
        return _etag

    @property
    def export_name(self):
        return self.raw['title'] + ".html"


class EvernoteFileRevisionMetadata(metadata.BaseFileRevisionMetadata):

    def __init__(self, modified):
        self._modified = modified

    @classmethod
    def from_metadata(cls, metadata):
        return EvernoteFileRevisionMetadata(modified=metadata.modified)

    @property
    def version_identifier(self):
        return 'revision'

    @property
    def version(self):
        return 'latest'

    @property
    def modified(self):
        return self._modified
