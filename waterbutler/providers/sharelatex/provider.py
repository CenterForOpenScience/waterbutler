import asyncio
import datetime
import os

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.sharelatex.metadata import ShareLatexFileMetadata
from waterbutler.providers.sharelatex.metadata import ShareLatexProjectMetadata


class ShareLatexProvider(provider.BaseProvider):
    NAME = 'sharelatex'

    def __init__(self, auth, credentials, settings):
        super().__init__(auth, credentials, settings)
        self.project_id = settings.get('project')
        self.auth_token = credentials.get('auth_token')
        self.sharelatex_url = credentials.get('sharelatex_url')

    @asyncio.coroutine
    def validate_v1_path(self, path, **kwargs):
        return self.validate_path(path, **kwargs)

    @asyncio.coroutine
    def validate_path(self, path, **kwargs):
        return WaterButlerPath(path)

    def build_url(self, *segments, **query):
        query['auth_token'] = self.auth_token
        return provider.build_url(self.sharelatex_url, 'api', 'v1', *segments, **query)

    @asyncio.coroutine
    def upload(self, stream, path, conflict='replace', **kwargs):
        pass

    @asyncio.coroutine
    def delete(self, path, **kwargs):
        pass

    @asyncio.coroutine
    def download(self, path, accept_url=False, range=None, **kwargs):
        url = self.build_url('project', self.project_id, 'file', path.path)

        if accept_url:
            return url

        resp = yield from self.make_request(
            'GET',
            url,
            range=range,
            expects=(200, 206),
            throws=exceptions.DownloadError,
        )

        return streams.ResponseStreamReader(resp, None, None, True)

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

        if path.is_file:
            return self._metadata_file(path, str(path))

        ret = []
        if str(path) is '/':

            for doc in data['rootFolder'][0]['docs']:
                ret.append(self._metadata_doc(path, doc['name']))
            for fil in data['rootFolder'][0]['fileRefs']:
                ret.append(self._metadata_file(path, fil['name']))
            for fol in data['rootFolder'][0]['folders']:
                ret.append(self._metadata_folder(path, fol['name']))

        else:
            folders_old = []
            folders = data['rootFolder'][0]['folders']
            path_exploded = str(path).strip('/').split('/')

            for p in path_exploded:
                folders_old = folders
                folders = self._search_folders(p, folders)

            for f in folders_old:
                for doc in f['docs']:
                    ret.append(self._metadata_doc(path, doc['name']))

                for filename in f['fileRefs']:
                    ret.append(self._metadata_file(path, filename['name']))

            for f in folders:
                ret.append(self._metadata_folder(path, f['name']))

        return ret

    def _search_folders(self, name, folders):
        for f in folders:
            if (name == f['name']):
                return (f['folders'])

    def _metadata_file(self, path, file_name=''):
        full_path = path.full_path if file_name == '' else os.path.join(path.full_path, file_name)
        modified = datetime.datetime.fromtimestamp(1445967864)
        metadata = {
            'path': full_path,
            'size': 123,
            'modified': modified.strftime('%a, %d %b %Y %H:%M:%S %z'),
            'mimetype': 'text/plain'  # TODO
        }
        return ShareLatexFileMetadata(metadata)

    def _metadata_folder(self, path, folder_name):
        return ShareLatexProjectMetadata({'path': os.path.join(path.path, folder_name)})

    def _metadata_doc(self, path, file_name=''):
        full_path = path.full_path if file_name == '' else os.path.join(path.full_path, file_name)
        modified = datetime.datetime.fromtimestamp(1445967864)  # TODO
        metadata = {
            'path': full_path,
            'size': 123,  # TODO
            'modified': modified.strftime('%a, %d %b %Y %H:%M:%S %z'),
            'mimetype': 'application/x-tex'
        }
        return ShareLatexFileMetadata(metadata)
