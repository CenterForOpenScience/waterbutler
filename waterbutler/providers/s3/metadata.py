import os

from waterbutler.core import metadata, utils


def strip_char(string, chars):
    if string.startswith(chars):
        return string[len(chars):]
    return string


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
        # Cast to dict to clone as the headers will
        # be destroyed when the request leaves scope
        super().__init__(dict(headers))

    @property
    def path(self):
        return '/' + strip_char(self._path, self.raw.get('base_folder', ''))

    @property
    def size(self):
        if 'ContentLength' in self.raw:
            return self.raw['ContentLength']
        elif 'Content-Length' in self.raw:
            return self.raw['Content-Length']
        return None

    @property
    def content_type(self):
        if 'ContentType' in self.raw:
            return self.raw['ContentType']
        elif 'Content-Type' in self.raw:
            return self.raw['Content-Type']
        return ''

    @property
    def modified(self):
        if 'LastModified' in self.raw:
            return str(self.raw['LastModified'])
        elif 'Last-Modified' in self.raw:
            return str(self.raw['Last-Modified'])
        return None

    @property
    def created_utc(self):
        return None

    @property
    def modified_utc(self) -> str:
        """ Date the file was last modified, as reported by the provider,
        converted to UTC, in format (YYYY-MM-DDTHH:MM:SS+00:00). """
        last_modified = self.modified
        return utils.normalize_datetime(str(last_modified)) if last_modified else last_modified

    @property
    def etag(self):
        # ETag is used in boto3/aiobotocore, Etag is used in boto
        if 'ETag' in self.raw:
            return self.raw['ETag']
        elif 'Etag' in self.raw:
            return self.raw['Etag']
        return ''

    @property
    def extra(self):
        md5 = ''
        if 'ETag' in self.raw:
            md5 = self.raw['ETag']
        elif 'Etag' in self.raw:
            md5 = self.raw['Etag']
        return {
            'md5': md5,
            'encryption': self.raw.get('x-amz-server-side-encryption', ''),
            'hashes': {
                'md5': md5,
            },
        }


class S3FileMetadata(S3Metadata, metadata.BaseFileMetadata):

    @property
    def path(self):
        return '/' + strip_char(self.raw['Key'], self.raw.get('base_folder', ''))

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
        return '/' + strip_char(self.raw['Key'], self.raw.get('base_folder', ''))


class S3FolderMetadata(S3Metadata, metadata.BaseFolderMetadata):

    @property
    def name(self):
        return self.raw['Prefix'].split('/')[-2]

    @property
    def path(self):
        if self.raw.get('base_folder', ''):
            return '/' + strip_char(self.raw['Prefix'], self.raw.get('base_folder', ''))
        return '/' + self.raw['Prefix']


# TODO dates!
class S3Revision(metadata.BaseFileRevisionMetadata):

    @property
    def version_identifier(self):
        return 'version'

    @property
    def version(self):
        is_latest = self.raw['IsLatest']
        if is_latest == True or is_latest == 'true':
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
