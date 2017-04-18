from waterbutler.core import metadata


class DmptoolFileMetadata(metadata.BaseFileMetadata):

    def __init__(self, raw):
        # metadata.BaseFileMetadata.__init__(self, raw)

        print('++++++++++++++++++++++++++++++ DmptoolFileMetadata.__init__')
        print('+++++ raw: {}'.format(raw))
        super().__init__(raw)

    @property
    def content_type(self):
        return "application/pdf"

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

        # TO DO: change back to pdf --> md for now
        return self.raw['title'] + ".pdf"

    @property
    def provider(self):
        return 'dmptool'

    @property
    def size(self):

        return self.raw['length']

    @property
    def extra(self):

        return {
            'webView': 'https://dmptool.org/plans/{}/details'.format(self.raw.get('guid')),
        }

    @property
    def path(self):
        return "/" + self.raw["guid"]

    @property
    def etag(self):
        # TO DO: implement
        return "[ETAG]"


class DmptoolFileRevisionMetadata(metadata.BaseFileRevisionMetadata):

    def __init__(self, modified):
        self._modified = modified

    @classmethod
    def from_metadata(cls, metadata):
        return DmptoolFileRevisionMetadata(modified=metadata.modified)

    @property
    def version_identifier(self):
        return 'revision'

    @property
    def version(self):
        return 'latest'

    @property
    def modified(self):
        return self._modified
