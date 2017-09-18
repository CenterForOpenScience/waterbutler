from waterbutler.core import metadata


class BaseWEKOMetadata(metadata.BaseMetadata):

    @property
    def provider(self):
        return 'weko'

    @property
    def created_utc(self):
        return None


class WEKOItemMetadata(BaseWEKOMetadata, metadata.BaseFileMetadata):
    index = None
    all_indices = None

    def __init__(self, raw, index, all_indices):
        super().__init__(raw)
        self.index = index
        self.all_indices = all_indices

    @property
    def file_id(self):
        return str(self.raw.file_id)

    @property
    def name(self):
        return self.raw.title

    @property
    def content_type(self):
        return None

    @property
    def path(self):
        target = self.index
        path = target.identifier + '/'
        while target.parentIdentifier is not None:
            target = [i for i in self.all_indices
                        if i.identifier == target.parentIdentifier][0]
            path = target.identifier + '/' + path
        return '/' + path + self.raw.file_id

    @property
    def size(self):
        return None

    @property
    def modified(self):
        return None

    @property
    def etag(self):
        return self.raw.file_id

    @property
    def extra(self):
        return {
            'fileId': self.raw.file_id,
        }


class WEKOIndexMetadata(BaseWEKOMetadata, metadata.BaseFolderMetadata):
    all_indices = None

    def __init__(self, raw, all_indices):
        super().__init__(raw)
        self.all_indices = all_indices

    @property
    def name(self):
        return self.raw.title

    @property
    def path(self):
        target = self.raw
        path = target.identifier + '/'
        while target.parentIdentifier is not None:
            target = [i for i in self.all_indices
                        if i.identifier == target.parentIdentifier][0]
            path = target.identifier + '/' + path
        return '/' + path
