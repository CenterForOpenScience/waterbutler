from waterbutler.core import metadata
import waterbutler.core.utils as core_utils

from waterbutler.providers.googledrive import utils


class BaseGoogleDriveMetadata(metadata.BaseMetadata):

    def __init__(self, raw, path):
        super().__init__(raw)
        self._path = path

    @property
    def provider(self):
        return 'googledrive'

    @property
    def extra(self):
        """NB: The Google Drive provider frequently fudges this value in the raw data before passing
        it to the GoogleDrive*Metadata constructor.  The ``version`` field provided by the GDrive
        API does not actually identify any previous versions, so we must look it up another way.
        GDrive does not support revisions for read-only files, so those are assigned artificial
        values to help the provider to recognize them.  See the **Revisions** section of the
        GoogleDriveProvider docstring."""

        return {'revisionId': self.raw['version']}


class GoogleDriveFolderMetadata(BaseGoogleDriveMetadata, metadata.BaseFolderMetadata):

    def __init__(self, raw, path):
        super().__init__(raw, path)
        self._path._is_folder = True

    @property
    def id(self):
        return self.raw['id']

    @property
    def name(self):
        return self.raw['title']

    @property
    def path(self):
        return '/' + self._path.raw_path

    @property
    def materialized_path(self):
        return str(self._path)

    @property
    def export_name(self):
        return self.name


class GoogleDriveFileMetadata(BaseGoogleDriveMetadata, metadata.BaseFileMetadata):
    """The metadata for a single file on Google Drive.  This class expects a the ``raw``
    property to be the response[1] from the GDrive v2 file metadata endpoint[2].

    [1] https://developers.google.com/drive/v2/reference/files
    [2] https://developers.google.com/drive/v2/reference/files/get
    """

    @property
    def id(self):
        return self.raw['id']

    @property
    def name(self):
        title = self._file_title
        if self.is_google_doc:
            ext = utils.get_extension(self.raw)
            title += ext
        return title

    @property
    def path(self):
        path = '/' + self._path.raw_path
        if self.is_google_doc:
            ext = utils.get_extension(self.raw)
            path += ext
        return path

    @property
    def materialized_path(self):
        materialized = str(self._path)
        if self.is_google_doc:
            ext = utils.get_extension(self.raw)
            materialized += ext
        return materialized

    @property
    def size(self):
        # Google docs(Docs,sheets, slides, etc)  don't have file size before they are exported
        return self.raw.get('fileSize')

    @property
    def modified(self):
        return self.raw['modifiedDate']

    @property
    def created_utc(self):
        return core_utils.normalize_datetime(self.raw['createdDate'])

    @property
    def content_type(self):
        return self.raw['mimeType']

    @property
    def etag(self):
        return self.raw['version']

    @property
    def extra(self):
        ret = super().extra
        ret['webView'] = self.raw.get('alternateLink')

        if self.is_google_doc:
            ret['downloadExt'] = utils.get_download_extension(self.raw)
        else:
            if not hasattr(ret, 'hashes'):
                ret['hashes'] = {}
            ret['hashes']['md5'] = self.raw.get('md5Checksum')  # no md5 for non-exportable file

        return ret

    @property
    def is_google_doc(self):
        return utils.is_docs_file(self.raw) is not None

    @property
    def export_name(self):
        title = self._file_title
        if self.is_google_doc:
            ext = utils.get_download_extension(self.raw)
            title += ext
        return title

    @property
    def _file_title(self):
        return self.raw['title']


class GoogleDriveFileRevisionMetadata(GoogleDriveFileMetadata):
    """The metadata for a single file at a particular revision on Google Drive.  This class expects
    the ``raw`` property to be the response[1] from the GDrive v2 revision metadata endpoint[2].
    This response is similar to the one from the file metadata endpoint, but lacks a created date
    and version field.  It also stores the file name of non-GDoc files in the ``originalFilename``
    field instead of the ``title`` field.  GDocs do not include the file name at all, and must
    derive it from the `GoogleDrivePath` object.

    [1] https://developers.google.com/drive/v2/reference/revisions
    [2] https://developers.google.com/drive/v2/reference/revisions/get
    """

    @property
    def created_utc(self):
        return None

    @property
    def etag(self):
        return self.raw['etag']

    @property
    def extra(self):
        """The metadata for a file revision doesn't contain a webView link, and a revisionId isn't
        appropriate.  GDocs don't have an md5, non-GDocs don't need a downloadExt.
        """
        if self.is_google_doc:
            return {'downloadExt': utils.get_download_extension(self.raw)}
        return {'md5': self.raw['md5Checksum']}

    @property
    def _file_title(self):
        return self.raw.get('originalFilename', self._path.name)


class GoogleDriveRevision(metadata.BaseFileRevisionMetadata):

    @property
    def version_identifier(self):
        return 'revision'

    @property
    def version(self):
        return self.raw['id']

    @property
    def modified(self):
        return self.raw['modifiedDate']
