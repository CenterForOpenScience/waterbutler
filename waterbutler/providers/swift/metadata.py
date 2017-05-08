import os

from waterbutler.core import metadata
from swiftclient import parse_header_string


def resp_headers(headers):
    return dict(map(lambda item: (parse_header_string(item[0]).lower(),
                                  parse_header_string(item[1])),
                    headers.items()))


class SwiftMetadata(metadata.BaseMetadata):

    @property
    def provider(self):
        return 'swift'

    @property
    def name(self):
        return os.path.split(self.path)[1]

    @property
    def created_utc(self):
        return None


class SwiftFileMetadataHeaders(SwiftMetadata, metadata.BaseFileMetadata):

    def __init__(self, path, headers):
        self._path = path
        # Cast to dict to clone as the headers will
        # be destroyed when the request leaves scope
        super().__init__(resp_headers(headers))

    @property
    def path(self):
        return '/' + self._path

    @property
    def size(self):
        return self.raw['content-length']

    @property
    def content_type(self):
        return self.raw['content-type']

    @property
    def modified(self):
        return self.raw['last-modified']

    @property
    def etag(self):
        return self.raw['etag']

    @property
    def extra(self):
        return {
            'md5': self.raw['etag']
        }


class SwiftFileMetadata(SwiftMetadata, metadata.BaseFileMetadata):

    @property
    def path(self):
        return '/' + self.raw['name']

    @property
    def size(self):
        return int(self.raw['bytes'])

    @property
    def modified(self):
        return self.raw['last_modified']

    @property
    def content_type(self):
        return self.raw['content_type']

    @property
    def etag(self):
        return self.raw['hash']

    @property
    def extra(self):
        return {
            'md5': self.raw['hash']
        }


class SwiftFolderMetadata(SwiftMetadata, metadata.BaseFolderMetadata):

    @property
    def name(self):
        return self.raw['prefix'].split('/')[-2]

    @property
    def path(self):
        return '/' + self.raw['prefix']
