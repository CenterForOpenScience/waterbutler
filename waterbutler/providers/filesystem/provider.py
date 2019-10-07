import os
import shutil
import logging
import datetime
import mimetypes
from typing import Tuple, Union

from waterbutler.core import exceptions, provider
from waterbutler.core.path import WaterButlerPath
from waterbutler.core.streams import FileStreamReader, PartialFileStreamReader

from waterbutler.providers.filesystem import settings as pd_settings
from waterbutler.providers.filesystem.metadata import (FileSystemFileMetadata,
                                                       FileSystemFolderMetadata, )

logger = logging.getLogger(__name__)


class FileSystemProvider(provider.BaseProvider):
    """Provider using the local filesystem as a backend-store

    This provider is used for local testing.  Files are stored by hash, preserving
    case-sensitivity on case-insensitive host filesystems.
    """
    NAME = 'filesystem'

    def __init__(self, auth, credentials, settings, **kwargs):
        super().__init__(auth, credentials, settings, **kwargs)
        self.folder = self.settings['folder']
        os.makedirs(self.folder, exist_ok=True)

    async def validate_v1_path(self, path, **kwargs):
        if not os.path.exists(self.folder + path):
            raise exceptions.NotFoundError(str(path))

        implicit_folder = path.endswith('/')
        explicit_folder = os.path.isdir(self.folder + path)
        if implicit_folder != explicit_folder:
            raise exceptions.NotFoundError(str(path))

        return WaterButlerPath(path, prepend=self.folder)

    async def validate_path(self, path, **kwargs):
        return WaterButlerPath(path, prepend=self.folder)

    def can_duplicate_names(self):
        return False

    async def intra_copy(self, dest_provider, src_path, dest_path):
        exists = await self.exists(dest_path)
        shutil.copy(src_path.full_path, dest_path.full_path)
        return (await dest_provider.metadata(dest_path)), not exists

    async def intra_move(self, dest_provider, src_path, dest_path):
        exists = await self.exists(dest_path)
        shutil.move(src_path.full_path, dest_path.full_path)
        return (await dest_provider.metadata(dest_path)), not exists

    async def download(self, path: WaterButlerPath, range: Tuple[int, int]=None,   # type: ignore
                       **kwargs) -> Union[FileStreamReader, PartialFileStreamReader]:
        if not os.path.exists(path.full_path):
            raise exceptions.DownloadError('Could not retrieve file \'{0}\''.format(path), code=404)
        file_pointer = open(path.full_path, 'rb')
        logger.debug('requested-range:: {}'.format(range))
        if range is not None and range[1] is not None:
            return PartialFileStreamReader(file_pointer, range)
        return FileStreamReader(file_pointer)

    async def upload(self, stream, path, **kwargs):
        created = not (await self.exists(path))

        os.makedirs(os.path.split(path.full_path)[0], exist_ok=True)

        with open(path.full_path, 'wb') as file_pointer:
            chunk = await stream.read(pd_settings.CHUNK_SIZE)
            while chunk:
                file_pointer.write(chunk)
                chunk = await stream.read(pd_settings.CHUNK_SIZE)

        metadata = await self.metadata(path)
        return metadata, created

    async def delete(self, path, **kwargs):
        if path.is_file:
            os.remove(path.full_path)
        else:
            shutil.rmtree(path.full_path)
            if path.is_root:
                os.makedirs(self.folder, exist_ok=True)

    async def metadata(self, path, **kwargs):
        if path.is_dir:
            if not os.path.exists(path.full_path) or not os.path.isdir(path.full_path):
                raise exceptions.MetadataError(
                    'Could not retrieve folder \'{0}\''.format(path),
                    code=404,
                )

            ret = []
            for item in os.listdir(path.full_path):
                if os.path.isdir(os.path.join(path.full_path, item)):
                    metadata = self._metadata_folder(path, item)
                    ret.append(FileSystemFolderMetadata(metadata, self.folder))
                else:
                    metadata = self._metadata_file(path, item)
                    ret.append(FileSystemFileMetadata(metadata, self.folder))
            return ret
        else:
            if not os.path.exists(path.full_path) or os.path.isdir(path.full_path):
                raise exceptions.MetadataError(
                    'Could not retrieve file \'{0}\''.format(path),
                    code=404,
                )

            metadata = self._metadata_file(path)
            return FileSystemFileMetadata(metadata, self.folder)

    def _metadata_file(self, path, file_name=''):
        full_path = path.full_path if file_name == '' else os.path.join(path.full_path, file_name)
        modified = datetime.datetime.utcfromtimestamp(os.path.getmtime(full_path)).replace(tzinfo=datetime.timezone.utc)
        return {
            'path': full_path,
            'size': os.path.getsize(full_path),
            'modified': modified.strftime('%a, %d %b %Y %H:%M:%S %z'),
            'modified_utc': modified.isoformat(),
            'mime_type': mimetypes.guess_type(full_path)[0],
        }

    def _metadata_folder(self, path, folder_name):
        return {
            'path': os.path.join(path.path, folder_name),
        }

    def can_intra_copy(self, dest_provider, path=None):
        return type(self) == type(dest_provider)

    def can_intra_move(self, dest_provider, path=None):
        return self.can_intra_copy(dest_provider)
