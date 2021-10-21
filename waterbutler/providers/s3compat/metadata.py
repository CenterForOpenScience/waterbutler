import os

from waterbutler.core import metadata


class S3CompatMetadata(metadata.BaseMetadata):

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


class S3CompatFileMetadataHeaders(S3CompatMetadata, metadata.BaseFileMetadata):

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
        return self.raw['Content-Length']

    @property
    def content_type(self):
        return self.raw['Content-Type']

    @property
    def modified(self):
        return self.raw['Last-Modified']

    @property
    def created_utc(self):
        return None

    @property
    def etag(self):
        return self.raw['Etag'].replace('"', '')

    @property
    def extra(self):
        return {
            'md5': self.raw['Etag'].replace('"', ''),
            'encryption': self.raw.get('x-amz-server-side-encryption', '')
        }


class S3CompatFileMetadata(S3CompatMetadata, metadata.BaseFileMetadata):

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


class S3CompatFolderMetadata(S3CompatMetadata, metadata.BaseFolderMetadata):

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
