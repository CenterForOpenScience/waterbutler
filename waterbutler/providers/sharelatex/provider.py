import asyncio

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
        self.project_id = settings.get('project_id')
        self.auth_token = credentials.get('auth_token')

    @asyncio.coroutine
    def validate_path(self, path, **kwargs):
        return WaterButlerPath(path)

    def _build_project_url(self, *segments, **query):
        project_url = '/' + self.project_id + segments
        query['auth_token'] = self.auth_token
        return provider.build_url(project_url, **query)

    @asyncio.coroutine
    def download(self, path, **kwargs):
        url = self._build_project_url(path.path)

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
        url = self._build_project_url(path.path)

        resp = yield from self.make_request(
            'GET', url,
            expects=(200, ),
            throws=exceptions.MetadataError
        )

        data = yield from resp.json()

        if path.is_dir:
            ret = []
            for item in data['contents']:
                if item['is_dir']:
                    ret.append(ShareLatexProjectMetadata(item))
                else:
                    ret.append(ShareLatexFileMetadata(item))
            return ret

        return ShareLatexFileMetadata(data)

    @asyncio.coroutine
    def revisions(self, path, **kwargs):
        raise exceptions.ProviderError({'message': 'sharelatex does not support file revisions.'}, code=405)
