import asyncio
import datetime
import os

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.sharelatex import settings
from waterbutler.providers.sharelatex.metadata import ShareLatexFileMetadata
from waterbutler.providers.sharelatex.metadata import ShareLatexProjectMetadata


class ShareLatexProvider(provider.BaseProvider):
    NAME = 'sharelatex'
    BASE_URL = settings.BASE_URL

    def __init__(self, auth, credentials, settings):
        super().__init__(auth, credentials, settings)
        self.project_id = settings.get('project')
        self.auth_token = credentials.get('access_key')

    @asyncio.coroutine
    def validate_path(self, path, **kwargs):
        return WaterButlerPath(path)

    def build_url(self, *segments, **query):
        query['auth_token'] = self.auth_token
        return provider.build_url(*segments, **query)

    @asyncio.coroutine
    def download(self, path, **kwargs):
        url = self.build_url('/project/download/file/', path.path)

        resp = yield from self.make_request(
            'GET',
            url,
            expects=(200, 206),
            throws=exceptions.DownloadError,
        )

        return streams.ResponseStreamReader(resp)

    @asyncio.coroutine
    def upload(self, stream, path, conflict='replace', **kwargs):
        path, exists = yield from self.handle_name_conflict(path, conflict=conflict)
        url = self._build_project_url(path.path)

        resp = yield from self.make_request(
            'PUT',
            url,
            data=stream,
            headers={'Content-Length': str(stream.size)},
            expects=(200, 201, ),
            throws=exceptions.UploadError,
        )

        data = yield from resp.json()
        return ShareLatexFileMetadata(data), not exists

    @asyncio.coroutine
    def delete(self, path, **kwargs):
        url = self._build_project_url(path.path)
        yield from self.make_request(
            'DELETE',
            url,
            expects=(200, 204, ),
            throws=exceptions.DeleteError,
        )

    @asyncio.coroutine
    def metadata(self, path, **kwargs):
        url = self.build_url('project', self.project_id, 'docs')

        resp = yield from self.make_request(
            'GET', url,
            expects=(200, ),
            headers={
                'Content-Type': 'application/json'
            },
            throws=exceptions.MetadataError
        )

        data = yield from resp.json()

        if not data:
            raise exceptions.NotFoundError(str(path))

        ret = []
        if str(path) is '/':

            for doc in data['rootFolder'][0]['docs']:
                metadata = self._metadata_file(path, doc['name'])
                ret.append(ShareLatexFileMetadata(metadata))
            for fil in data['rootFolder'][0]['fileRefs']:
                metadata = self._metadata_file(path, fil['name'])
                ret.append(ShareLatexFileMetadata(metadata))
            for fol in data['rootFolder'][0]['folders']:
                metadata = self._metadata_folder(path, fol['name'])
                ret.append(ShareLatexProjectMetadata(metadata))

        else:
            folders_old = []
            folders = data['rootFolder'][0]['folders']
            path_exploded = str(path).strip('/').split('/')

            for p in path_exploded:
                folders_old = folders
                folders = self._search_folders(p, folders)

            for f in folders_old:
                for doc in f['docs']:
                    metadata = self._metadata_file(path, doc['name'])
                    ret.append(ShareLatexFileMetadata(metadata))

                for filename in f['fileRefs']:
                    metadata = self._metadata_file(path, filename['name'])
                    ret.append(ShareLatexFileMetadata(metadata))

            for f in folders:
                metadata = self._metadata_folder(path, f['name'])
                ret.append(ShareLatexProjectMetadata(metadata))

        return ret

    def _search_folders(self, name, folders):
        for f in folders:
            if (name == f['name']):
                return f['folders']
        raise exceptions.NotFoundError(str(folders))

    def _metadata_file(self, path, file_name=''):
        full_path = path.full_path if file_name == '' else os.path.join(path.full_path, file_name)
        modified = datetime.datetime.fromtimestamp(1445967864)
        return {
            'path': full_path,
            'size': 123,
            'modified': modified.strftime('%a, %d %b %Y %H:%M:%S %z'),
            'mime_type': 'application/json',
        }

    def _metadata_folder(self, path, folder_name):
        return {
            'path': os.path.join(path.path, folder_name),
        }

    @asyncio.coroutine
    def revisions(self, path, **kwargs):
        raise exceptions.ProviderError({'message': 'ShareLaTeX does not support file revisions.'}, code=405)
