import os

from waterbutler.core import metadata


class S3CompatMetadata(metadata.BaseMetadata):

    @property
    def provider(self):
        return 's3compat'

    @property
    def name(self):
        return os.path.split(self.path)[1]


class S3CompatFileMetadataHeaders(S3CompatMetadata, metadata.BaseFileMetadata):

    def __init__(self, path, headers):
        self._path = path
        # Cast to dict to clone as the headers will
        # be destroyed when the request leaves scope
        super().__init__(dict(headers))

    @property
    def path(self):
        return '/' + self._path

    @property
    def size(self):
        return self.raw['CONTENT-LENGTH']

    @property
    def content_type(self):
        return self.raw['CONTENT-TYPE']

    @property
    def modified(self):
        return self.raw['LAST-MODIFIED']

    @property
    def created_utc(self):
        return None

    @property
    def etag(self):
        return self.raw['ETAG'].replace('"', '')

    @property
    def extra(self):
        return {
            'md5': self.raw['ETAG'].replace('"', ''),
            'encryption': self.raw.get('X-AMZ-SERVER-SIDE-ENCRYPTION', '')
        }


class S3CompatFileMetadata(S3CompatMetadata, metadata.BaseFileMetadata):

    @property
    def path(self):
        return '/' + self.raw['Key']

    @property
    def size(self):
        return int(self.raw['Size'])

    @property
    def modified(self):
        return self.raw['LastModified']

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
        return {
            'md5': self.raw['ETag'].replace('"', '')
        }


class S3CompatFolderKeyMetadata(S3CompatMetadata, metadata.BaseFolderMetadata):

    @property
    def name(self):
        return self.raw['Key'].split('/')[-2]

    @property
    def path(self):
        return '/' + self.raw['Key']


class S3CompatFolderMetadata(S3CompatMetadata, metadata.BaseFolderMetadata):

    @property
    def name(self):
        return self.raw['Prefix'].split('/')[-2]

    @property
    def path(self):
        return '/' + self.raw['Prefix']


# TODO dates!
class S3CompatRevision(metadata.BaseFileRevisionMetadata):

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
        return self.raw['LastModified']

    @property
    def extra(self):
        return {
            'md5': self.raw['ETag'].replace('"', '')
        }
