import functools
import typing
from urllib import parse

from waterbutler.core import utils
from waterbutler.core import metadata
from waterbutler.core.path import WaterButlerPath, WaterButlerPathPart


class RushFilesPathPart(WaterButlerPathPart):
    DECODE = parse.unquote
    ENCODE = functools.partial(parse.quote, safe='')  # type: ignore


class RushFilesPath(WaterButlerPath):
    PART_CLASS = RushFilesPathPart


class BaseRushFilesMetadata(metadata.BaseMetadata):

    def __init__(self, raw, path):
        super().__init__(raw)
        self._path = path

    @property
    def provider(self):
        return 'rushfiles'

    @property
    def name(self) -> str:
        return self.raw['PublicName']

    @property
    def path(self) -> str:
        return '/' + self._path.raw_path

    @property
    def internal_name(self) -> str:
        return self.raw['InternalName']

    @property
    def path_obj(self) -> RushFilesPath:
        return self._path


class RushFilesFolderMetadata(BaseRushFilesMetadata, metadata.BaseFolderMetadata):
    def __init__(self, raw, path):
        super().__init__(raw, path)
        self._path._is_folder = True

    @property
    def materialized_path(self):
        return str(self._path)

    @property
    def extra(self):
        return {
            'internalName': self.raw['InternalName'],
            'shareId': self.raw['ShareId'],
            'parentId': self.raw['ParrentId'],
        }


class RushFilesFileMetadata(BaseRushFilesMetadata, metadata.BaseFileMetadata):
    @property
    def size(self) -> typing.Union[int, str]:
        return self.raw['EndOfFile']

    @property
    def modified(self) -> str:
        return self.raw['LastWriteTime']

    @property
    def etag(self) -> str:
        return self.raw['InternalName'] + '-' + str(self.raw['Tick'])

    @property
    def created_utc(self) -> str:
        return utils.normalize_datetime(self.raw['CreationTime'])

    @property
    def content_type(self) -> typing.Union[str, None]:
        return None

    @property
    def upload_name(self) -> str:
        return self.raw['UploadName']

    @property
    def extra(self):
        return {
            'internalName': self.raw['InternalName'],
            'shareId': self.raw['ShareId'],
            'parentId': self.raw['ParrentId'],
        }


class RushFilesRevision(metadata.BaseFileRevisionMetadata):

    @property
    def version_identifier(self):
        return 'revision'

    @property
    def version(self):
        return str(self.raw['Tick'])

    @property
    def modified(self):
        return self.raw['LastWriteTime']

    @property
    def upload_name(self):
        return self.raw['UploadName']
