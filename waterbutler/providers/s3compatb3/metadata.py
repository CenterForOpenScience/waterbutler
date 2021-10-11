import os

from waterbutler.core import metadata


class S3CompatB3Metadata(metadata.BaseMetadata):

    @property
    def provider(self):
        return self.raw['provider']

    @property
    def name(self):
        return os.path.split(self.path)[1]

    @staticmethod
    def convert_prefix(provider, raw, key):
        raw['provider'] = provider.NAME
        raw[key] = raw[key][len(provider.prefix):].lstrip('/')


class S3CompatB3FileMetadataHeaders(S3CompatB3Metadata, metadata.BaseFileMetadata):

    def __init__(self, provider, path, headers):
        # Cast to dict to clone as the headers will
        # be destroyed when the request leaves scope
        new_headers = dict(headers)
        new_headers['Key'] = path
        self.convert_prefix(provider, new_headers, 'Key')
        super().__init__(new_headers)

    @property
    def path(self):
        return '/' + self.raw['Key'].lstrip('/')

    @property
    def size(self):
        return self.raw['ContentLength']

    @property
    def content_type(self):
        return self.raw['ContentType']

    @property
    def modified(self):
        return str(self.raw['LastModified'])

    @property
    def created_utc(self):
        return None

    @property
    def etag(self):
        return self.raw['ETag'].replace('"', '')

    @property
    def extra(self):
        return {
            'md5': self.raw['ETag'].replace('"', ''),
            'encryption': self.raw.get('ServerSideEncryption', '')
        }


class S3CompatB3FileMetadata(S3CompatB3Metadata, metadata.BaseFileMetadata):

    def __init__(self, provider, raw):
        new_raw = dict(raw)
        self.convert_prefix(provider, new_raw, 'Key')
        super().__init__(new_raw)

    @property
    def path(self):
        return '/' + self.raw['Key'].lstrip('/')

    @property
    def size(self):
        return int(self.raw['Size'])

    @property
    def modified(self):
        return str(self.raw['LastModified'])

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
            'md5': self.raw['ETag'].replace('"', ''),
            'encryption': self.raw.get('ServerSideEncryption', '')
        }


class S3CompatB3FolderKeyMetadata(S3CompatB3Metadata, metadata.BaseFolderMetadata):

    def __init__(self, provider, raw):
        new_raw = dict(raw)
        self.convert_prefix(provider, new_raw, 'Key')
        super().__init__(new_raw)

    @property
    def name(self):
        return self.raw['Key'].split('/')[-2]

    @property
    def path(self):
        return '/' + self.raw['Key'].lstrip('/')


class S3CompatB3FolderMetadata(S3CompatB3Metadata, metadata.BaseFolderMetadata):

    def __init__(self, provider, raw):
        new_raw = dict(raw)
        self.convert_prefix(provider, new_raw, 'Prefix')
        super().__init__(new_raw)

    @property
    def name(self):
        return self.raw['Prefix'].split('/')[-2]

    @property
    def path(self):
        return '/' + self.raw['Prefix'].lstrip('/')


# TODO dates!
class S3CompatB3Revision(metadata.BaseFileRevisionMetadata):

    @property
    def version_identifier(self):
        return 'version'

    @property
    def version(self):
        if self.raw['IsLatest'] is True:
            return 'Latest'
        return self.raw['VersionId']

    @property
    def modified(self):
        return str(self.raw['LastModified'])

    @property
    def extra(self):
        return {
            'md5': self.raw['ETag'].replace('"', '')
        }
