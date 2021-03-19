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

        modified = self.raw.get('lastModifiedDateTime', None)
        if modified is not None:
            modified = utils.normalize_datetime(modified)

        created = self.raw.get('createdDateTime', None)
        if created is not None:
            created = utils.normalize_datetime(created)

        return dict(super().extra, **{
            'modified_utc': modified,
            'created_utc': created,
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
        if 'file' in self.raw.keys():
            return self.raw['file'].get('mimeType')
        return 'application/octet-stream'

    @property
    def etag(self):
        return self.raw['eTag']

    @property
    def created_utc(self):
        created = self.raw.get('createdDateTime', None)
        if created is not None:
            created = utils.normalize_datetime(created)
        return created

    def download_url(self):
        return self.raw.get('@microsoft.graph.downloadUrl', None)

    def package_type(self):
        if 'package' in self.raw:
            if 'type' in self.raw['package']:
                return self.raw['package']['type']
        return None


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
