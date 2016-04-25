from waterbutler.core import metadata

from waterbutler.providers.googledrive import utils


class BaseGoogleDriveMetadata(metadata.BaseMetadata):

    def __init__(self, raw, path):
        super().__init__(raw)
        self._path = path

    @property
    def provider(self):
        return 'googledrive'

    @property
    def path(self):
        return '/' + self._path.raw_path

    @property
    def materialized_path(self):
        return str(self._path)

    @property
    def extra(self):
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
    def export_name(self):
        return self.name


class GoogleDriveFileMetadata(BaseGoogleDriveMetadata, metadata.BaseFileMetadata):

    @property
    def id(self):
        return self.raw['id']

    @property
    def name(self):
        title = self.raw['title']
        if self.is_google_doc:
            ext = utils.get_extension(self.raw)
            title += ext
        return title

    @property
    def size(self):
        # Google docs(Docs,sheets, slides, etc)  don't have file size before they are exported
        return self.raw.get('fileSize')

    @property
    def modified(self):
        return self.raw['modifiedDate']

    @property
    def content_type(self):
        return self.raw['mimeType']

    @property
    def etag(self):
        return self.raw['version']

    @property
    def extra(self):
        ret = super().extra
        if self.is_google_doc:
            ret['downloadExt'] = utils.get_download_extension(self.raw)
        ret['webView'] = self.raw.get('alternateLink')
        return ret

    @property
    def is_google_doc(self):
        return utils.is_docs_file(self.raw)

    @property
    def export_name(self):
        title = self.raw['title']
        if self.is_google_doc:
            ext = utils.get_download_extension(self.raw)
            title += ext
        return title


class GoogleDriveFileRevisionMetadata(GoogleDriveFileMetadata):
    @property
    def id(self):
        return self.raw['id']

    @property
    def name(self):
        title = self.raw.get('originalFilename', self._path.name)
        if self.is_google_doc:
            ext = utils.get_extension(self.raw)
            title += ext
        return title

    @property
    def size(self):
        # Google docs(Docs,sheets, slides, etc)  don't have file size before they are exported
        return self.raw.get('fileSize')

    @property
    def modified(self):
        return self.raw['modifiedDate']

    @property
    def content_type(self):
        return self.raw['mimeType']

    @property
    def etag(self):
        return self.raw['etag']

    @property
    def extra(self):
        if self.is_google_doc:
            return {'downloadExt': utils.get_download_extension(self.raw)}
        return {'md5': self.raw['md5Checksum']}

    @property
    def export_name(self):
        title = self.raw.get('originalFilename', self._path.name)
        if self.is_google_doc:
            ext = utils.get_download_extension(self.raw)
            title += ext
        return title


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
