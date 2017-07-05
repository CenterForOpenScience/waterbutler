from waterbutler.core import metadata


class EvernoteFileMetadata(metadata.BaseFileMetadata):

    def __init__(self, raw):

        # print('++++++++++++++++++++++++++++++ EvernoteFileMetadata.__init__')
        # print('+++++ raw: {}'.format(raw))
        super().__init__(raw)

    @property
    def content_type(self):
        # TO DO: Implement
        return 'text/html'

    @property
    def modified(self):
        """ Date the file was last modified, as reported by the provider, in
        the format used by the provider. """

        return self.raw['updated']

    @property
    def created_utc(self):
        return None

    @property
    def name(self):

        # print("EvernoteFileMetadata.name: self.raw['title']", self.raw['title'])
        return self.raw['title'] + ".enml"

    @property
    def provider(self):
        return 'evernote'

    @property
    def size(self):
        # TO DO

        return self.raw['length']

    @property
    def extra(self):
        return super(EvernoteFileMetadata, self).extra.update({

        })

    @property
    def path(self):
        print('EvernoteFileMetadata.path: self.raw', self.raw)
        return "/" + self.raw["guid"]

    @property
    def etag(self):
        # TO DO: implement
        return "[ETAG]"


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
