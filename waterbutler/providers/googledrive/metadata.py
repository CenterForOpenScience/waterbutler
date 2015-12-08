from waterbutler.core import metadata

from waterbutler.providers.googledrive import utils


class BaseGoogleDriveMetadata(metadata.BaseMetadata):

    def __init__(self, raw, path_obj):
        super().__init__(raw)
        self._path_obj = path_obj

    @property
    def provider(self):
        return 'googledrive'

    # @property
    # def path(self):
    #     return '/' + self._path.raw_path

    @property
    def materialized_path(self):
        return str(self._path_obj)

    @property
    def extra(self):
        return {'revisionId': self.raw['version']}


class GoogleDriveFolderMetadata(BaseGoogleDriveMetadata, metadata.BaseFolderMetadata):

    def __init__(self, raw, path_obj):
        super().__init__(raw, path_obj)
        self._path_obj._is_folder = True

    # @property
    # def id(self):
    #     return self.raw['id']

    @property
    def name(self):
        return self.raw['title']

    @property
    def path(self):
        return '/{}/'.format(self.raw['id'])


class GoogleDriveFileMetadata(BaseGoogleDriveMetadata, metadata.BaseFileMetadata):

    # @property
    # def id(self):
    #     return self.raw['id']

    @property
    def name(self):
        title = self.raw['title']
        if utils.is_docs_file(self.raw):
            ext = utils.get_extension(self.raw)
            title += ext
        return title

    @property
    def path(self):
        return '/{0}'.format(self.raw['id'])

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
        if utils.is_docs_file(self.raw):
            ret['downloadExt'] = utils.get_download_extension(self.raw)
        ret['webView'] = self.raw.get('alternateLink')
        return ret


class GoogleDriveFileRevisionMetadata(GoogleDriveFileMetadata):
    @property
    def id(self):
        return self.raw['id']

    @property
    def name(self):
        title = self.raw.get('originalFilename', self._path_obj.name)
        if utils.is_docs_file(self.raw):
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
        if utils.is_docs_file(self.raw):
            return {'downloadExt': utils.get_download_extension(self.raw)}
        return {'md5': self.raw['md5Checksum']}


class GoogleDriveRevision(metadata.BaseFileRevisionMetadata):

    @property
    def version_identifier(self):
        return 'revision'

    @property
    def version(self):
        return self.raw['id']

    @property
    def path(self):
        try:
            return '/{0}/{1}'.format(self.raw['id'], self.raw['name'])
        except KeyError:
            return self.raw.get('path')

    @property
    def modified(self):
        return self.raw['modifiedDate']
