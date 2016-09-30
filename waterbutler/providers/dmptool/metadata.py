from waterbutler.core import metadata


class DmptoolFileMetadata(metadata.BaseFileMetadata):

    def __init__(self, raw):
        print("DmptoolFileMetadata.__init__:raw ", raw)
        metadata.BaseFileMetadata.__init__(self, raw)

    @property
    def content_type(self):
        # TO DO: Implement
        return 'application/pdf'

    @property
    def modified(self):
        """ Date the file was last modified, as reported by the provider, in
        the format used by the provider. """

        return self.raw['updated']

    @property
    def name(self):

        print("DmptoolFileMetadata.name: self.raw['title']", self.raw['title'])
        return self.raw['title'] + ".pdf"

    @property
    def provider(self):
        return 'dmptool'

    @property
    def size(self):
        # TO DO

        return self.raw['length']

    @property
    def extra(self):
        return super(DmptoolFileMetadata, self).extra.update({

        })

    @property
    def path(self):
        return "/" + self.raw["guid"]

    @property
    def etag(self):
        # TO DO: implement
        return "[ETAG]"
