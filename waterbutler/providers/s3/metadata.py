import os

from waterbutler.core import metadata
from waterbutler.core import utils


class S3Metadata(metadata.BaseMetadata):

    @property
    def provider(self):
        return 's3'

    @property
    def name(self):
        return os.path.split(self.path)[1]


class S3FileMetadataHeaders(S3Metadata, metadata.BaseFileMetadata):

    def __init__(self, path, headers):
        self._path = path
        self.obj = headers
        self._etag = None
        # Cast to dict to clone as the headers will
        # be destroyed when the request leaves scope
        super().__init__(headers)

    @property
    def path(self):
        return '/' + self._path

    @property
    def size(self):
        return self.obj.content_length

    @property
    def content_type(self):
        return self.obj.content_type

    @property
    def modified(self):
        return utils.normalize_datetime(self.obj.last_modified)

    @property
    def created_utc(self):
        return None

    @property
    def etag(self):
        if self._etag:
            return self._etag
        else:
            self._etag = self.obj.e_tag.replace('"', '')
            return self._etag

    @property
    def extra(self):
        return {
            'md5': self.etag,
            'encryption': self.obj.server_side_encryption,
            'hashes': {
                'md5': self.etag,
            },
        }


class S3FileMetadata(S3Metadata, metadata.BaseFileMetadata):

    @property
    def path(self):
        return '/' + self.raw['Key']

    @property
    def size(self):
        return int(self.raw['Size'])

    @property
    def modified(self):
        return self.raw['LastModified'].isoformat()

    @property
    def created_utc(self):
        return None

    @property
    def content_type(self):
        return None  # TODO

    @property
    def etag(self):
        return self.raw['ETag'].replace('"', '')

    @property
    def extra(self):
        md5 = self.raw['ETag'].replace('"', '')
        return {
            'md5': md5,
            'hashes': {
                'md5': md5,
            },
        }


class S3FolderKeyMetadata(S3Metadata, metadata.BaseFolderMetadata):

    @property
    def name(self):
        return self.raw['Key'].split('/')[-2]

    @property
    def path(self):
        return '/' + self.raw['Key']


class S3FolderMetadata(S3Metadata, metadata.BaseFolderMetadata):

    @property
    def name(self):
        return self.raw['Prefix'].split('/')[-2]

    @property
    def path(self):
        return '/' + self.raw['Prefix']


# TODO dates!
class S3Revision(metadata.BaseFileRevisionMetadata):

    @property
    def version_identifier(self):
        return 'version'

    @property
    def version(self):
        if self.raw['IsLatest'] == 'true':
            return 'Latest'
        return self.raw['VersionId']

    @property
    def modified(self):
        return utils.normalize_datetime(self.raw['LastModified'])

    @property
    def extra(self):
        return {
            'md5': self.raw['ETag'].replace('"', '')
        }
