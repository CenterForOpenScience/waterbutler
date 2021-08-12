from waterbutler.core import utils
from waterbutler.core import metadata


class BaseOneDriveMetadata(metadata.BaseMetadata):

    def __init__(self, raw, path_obj):
        """Figuring out the materialized path for a OneDrive entity can be a bit tricky. If the
        base folder is not the provider root, we need to make sure to scrub out everything up to
        and including the base folder.  All this has been done already in building the
        OneDrivePath object, so we'll just pass that in and save ourselves some trouble."""
        super().__init__(raw)
        self._path_obj = path_obj

    @property
    def provider(self):
        return 'onedrive'

    @property
    def materialized_path(self):
        return str(self._path_obj)

    @property
    def extra(self):
        return {
            'id': self.raw.get('id'),
            'etag': self.raw.get('eTag'),
            'webView': self.raw.get('webUrl'),
        }

    def _json_api_links(self, resource) -> dict:
        """Update JSON-API links to remove mutation actions"""
        links = super()._json_api_links(resource)
        for action in ['delete', 'upload', 'new_folder']:
            if action in links:
                links[action] = None
        return links


class OneDriveFolderMetadata(BaseOneDriveMetadata, metadata.BaseFolderMetadata):

    @property
    def name(self):
        return self.raw['name']

    @property
    def path(self):
        return '/{}/'.format(self.raw['id'])

    @property
    def etag(self):
        return self.raw.get('eTag')

    @property
    def extra(self):
        """OneDrive provides modified and creation times for folders.  Most providers do not
        so we'll stuff this into the ``extra`` properties."""

        return dict(super().extra, **{
            'modified_utc': utils.normalize_datetime(self.raw.get('lastModifiedDateTime')),
            'created_utc': utils.normalize_datetime(self.raw.get('createdDateTime')),
        })


class OneDriveFileMetadata(BaseOneDriveMetadata, metadata.BaseFileMetadata):

    @property
    def name(self):
        return self.raw['name']

    @property
    def path(self):
        return '/{0}'.format(self.raw['id'])

    @property
    def size(self):
        return int(self.raw.get('size'))

    @property
    def modified(self):
        return self.raw.get('lastModifiedDateTime')

    @property
    def content_type(self):
        if not self.raw.get('file'):
            return 'application/octet-stream'

        return self.raw['file'].get('mimeType', 'application/octet-stream')

    @property
    def etag(self):
        return self.raw['eTag']

    @property
    def created_utc(self):
        return utils.normalize_datetime(self.raw.get('createdDateTime'))


class OneDriveRevisionMetadata(metadata.BaseFileRevisionMetadata):

    @property
    def version_identifier(self):
        return 'revision'

    @property
    def version(self):
        return self.raw['id']

    @property
    def modified(self):
        return self.raw['lastModifiedDateTime']
