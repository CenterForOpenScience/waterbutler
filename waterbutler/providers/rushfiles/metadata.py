import typing
from waterbutler.core import metadata


class BaseRushFilesMetadata(metadata.BaseMetadata):

    def __init__(self, raw, path):
        super().__init__(raw)
        self._path = path

    @property
    def provider(self):
        return 'rushfiles'


class RushFilesFolderMetadata(BaseRushFilesMetadata, metadata.BaseFolderMetadata):
    def __init__(self, raw, path):
        super().__init__(raw, path)
        self._path._is_folder = True

    @property
    def name(self) -> str:
        raise NotImplementedError

    @property
    def path(self) -> str:
        raise NotImplementedError


class RushFilesFileMetadata(BaseRushFilesMetadata, metadata.BaseFileMetadata):
    @property
    def name(self) -> str:
        raise NotImplementedError

    @property
    def path(self) -> str:
        raise NotImplementedError

    @property
    def size(self) -> typing.Union[int, str]:
        raise NotImplementedError

    @property
    def modified(self) -> str:
        return NotImplementedError

    @property
    def created_utc(self) -> str:
        raise NotImplementedError

    @property
    def content_type(self) -> typing.Union[str, None]:
        return None

    @property
    def etag(self) -> typing.Union[str, None]:
        #TODO Can we return something? Remove if not
        raise NotImplementedError



# TODO Remove if not necessary
class RushFilesFileRevisionMetadata(RushFilesFileMetadata):
    pass


class RushFilesRevision(metadata.BaseFileRevisionMetadata):

    @property
    def version_identifier(self):
        raise NotImplementedError

    @property
    def version(self):
        raise NotImplementedError

    @property
    def modified(self):
        raise NotImplementedError
