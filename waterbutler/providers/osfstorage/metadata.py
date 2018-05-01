import pytz
import dateutil.parser

from waterbutler.core import metadata


class BaseOsfStorageMetadata:
    @property
    def provider(self):
        return 'osfstorage'


class BaseOsfStorageItemMetadata(BaseOsfStorageMetadata):

    def __init__(self, raw, materialized):
        super().__init__(raw)
        self._materialized = materialized

    @property
    def name(self):
        return self.raw['name']

    @property
    def path(self):
        return self.raw['path']

    @property
    def materialized_path(self):
        return self._materialized


class OsfStorageFileMetadata(BaseOsfStorageItemMetadata, metadata.BaseFileMetadata):

    @property
    def modified(self):
        return self.raw['modified']

    @property
    def modified_utc(self):
        try:
            return self.raw['modified_utc']
        except KeyError:
            if self.raw['modified'] is None:
                return None

            # Kludge for OSF, whose modified attribute does not include
            # tzinfo but is assumed to be UTC.
            parsed_datetime = dateutil.parser.parse(self.raw['modified'])
            if not parsed_datetime.tzinfo:
                parsed_datetime = parsed_datetime.replace(tzinfo=pytz.UTC)
            return parsed_datetime.isoformat()

    @property
    def created_utc(self):
        try:
            return self.raw['created_utc']
        except KeyError:
            if self.raw['created'] is None:
                return None

            # Kludge for OSF, whose created attribute does not include
            # tzinfo but is assumed to be UTC.
            parsed_datetime = dateutil.parser.parse(self.raw['created'])
            if not parsed_datetime.tzinfo:
                parsed_datetime = parsed_datetime.replace(tzinfo=pytz.UTC)
            return parsed_datetime.isoformat()

    @property
    def size(self):
        return self.raw['size']

    @property
    def content_type(self):
        return self.raw.get('contentType')

    @property
    def etag(self):
        return '{}::{}'.format(self.raw['version'], self.path)

    @property
    def extra(self):
        """osfstorage-specific metadata for files.

        * ``guid``: Always `None`.  Added in anticipation of OSF-side support, which was then
          abandoned after technical consideration.  Left in to avoid breaking clients that expect
          the key to be present.

        * ``version``: The version number of the *most recent* version, not the requested version.

        * ``downloads``: Number of times the file has been downloaded.

        * ``checkout``: Whether this file has been checked-out and is therefore read-only to all
          but the user who has checked it out.

        * ``latestVersionSeen``: Whether the requesting user has seen the most recent version of
          the file.  `True` if so.  `False` if a newer version exists that the user has not yet
          seen.  `None` if the user has not seen *any* version of the file.

        """
        return {
            'guid': self.raw.get('guid', None),
            'version': self.raw['version'],
            'downloads': self.raw['downloads'],
            'checkout': self.raw['checkout'],
            'latestVersionSeen': self.raw.get('latestVersionSeen', None),
            'hashes': {
                'md5': self.raw['md5'],
                'sha256': self.raw['sha256']
            },
        }


class OsfStorageFolderMetadata(BaseOsfStorageItemMetadata, metadata.BaseFolderMetadata):
    pass


class OsfStorageRevisionMetadata(BaseOsfStorageMetadata, metadata.BaseFileRevisionMetadata):

    @property
    def modified(self):
        return self.raw['date']

    @property
    def version_identifier(self):
        return 'version'

    @property
    def version(self):
        return str(self.raw['index'])

    @property
    def extra(self):
        return {
            'user': self.raw['user'],
            'downloads': self.raw['downloads'],
            'hashes': {
                'md5': self.raw['md5'],
                'sha256': self.raw['sha256']
            },
        }
