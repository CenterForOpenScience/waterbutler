from waterbutler.core import metadata

class EvernoteFileMetadata(metadata.BaseFileMetadata):
  
    def __init__(self, raw):
        metadata.BaseFileMetadata.__init__(self, raw)

    @property
    def content_type(self):
        # TO DO: Implement
        return 'text/enml'

    @property
    def modified(self):
        """ Date the file was last modified, as reported by the provider, in
        the format used by the provider. """

        return self.raw['updated']

    @property
    def name(self):
        
        return self.raw['title']

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
        return "/" + self.raw["guid"]

    @property
    def _json_api_links(self, resource):
        return {}
    
    @property
    def etag(self):
        # TO DO: implement
        return "[ETAG]"




