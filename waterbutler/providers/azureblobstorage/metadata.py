import os

from waterbutler.core import utils
from waterbutler.core import metadata


def strip_char(str, chars):
    if str.startswith(chars):
        return str[len(chars):]
    return str


class AzureBlobStorageMetadata(metadata.BaseMetadata):

    @property
    def provider(self):
        return 'azureblobstorage'

    @property
    def name(self):
        return os.path.split(self.path)[1]

    @property
    def created_utc(self):
        return None


class AzureBlobStorageFileMetadataHeaders(AzureBlobStorageMetadata, metadata.BaseFileMetadata):

    def __init__(self, headers, path):
        self._path = path
        super().__init__(dict(headers))

    def _dehydrate(self):
        payload = super()._dehydrate()
        payload['_path'] = self._path
        return payload

    @classmethod
    def _rehydrate(cls, payload):
        args = super()._rehydrate(payload)
        args.append(payload['_path'])
        return args

    @property
    def path(self):
        return '/' + strip_char(self._path, self.raw.get('base_folder', ''))

    @property
    def size(self):
        return int(self.raw['Content-Length'])

    @property
    def content_type(self):
        return self.raw['Content-Type']

    @property
    def created_utc(self):
        creation_time = self.raw.get('Creation-Time')
        return utils.normalize_datetime(creation_time)

    @property
    def modified(self):
        return self.raw.get('Last-Modified')

    @property
    def etag(self):
        return self.raw.get('ETag', '').strip('"')

    @property
    def extra(self):
        return {
            'md5': self.raw.get('Content-MD5', ''),
            'etag': self.raw.get('ETag', '').strip('"')
        }


class AzureBlobStorageFileMetadata(AzureBlobStorageMetadata, metadata.BaseFileMetadata):

    @property
    def path(self):
        return '/' + strip_char(self.raw['Name'], self.raw.get('base_folder', ''))

    @property
    def size(self):
        return int(self.raw['Properties']['Content-Length'])

    @property
    def content_type(self):
        return self.raw['Properties']['Content-Type']

    @property
    def created_utc(self):
        creation_time = self.raw['Properties'].get('CreationTime')
        return utils.normalize_datetime(creation_time)

    @property
    def modified(self):
        return self.raw['Properties'].get('Last-Modified')

    @property
    def etag(self):
        return self.raw['Properties'].get('Etag', '').strip('"')

    @property
    def extra(self):
        return {
            'md5': self.raw['Properties'].get('Content-MD5', ''),
            'etag': self.raw['Properties'].get('Etag', '').strip('"'),
            'blob_type': self.raw['Properties'].get('BlobType', ''),
            'access_tier': self.raw['Properties'].get('AccessTier', ''),
            'creation_time': self.raw['Properties'].get('CreationTime', ''),
            'lease_status': self.raw['Properties'].get('LeaseStatus', ''),
            'lease_state': self.raw['Properties'].get('LeaseState', ''),
            'server_encrypted': self.raw['Properties'].get('ServerEncrypted', '')
        }


class AzureBlobStorageFolderMetadata(AzureBlobStorageMetadata, metadata.BaseFolderMetadata):

    @property
    def name(self):
        if not self.raw['Name'] or self.raw['Name'] == '/':
            return ''
        name_parts = self.raw['Name'].rstrip('/').split('/')
        return name_parts[-1] if name_parts else ''

    @property
    def path(self):
        if self.raw.get('base_folder', ''):
            return '/' + strip_char(self.raw['Name'], self.raw.get('base_folder', ''))
        return '/' + self.raw['Name']
