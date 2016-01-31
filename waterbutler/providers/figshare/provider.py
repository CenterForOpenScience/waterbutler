import http
import json
import asyncio

import aiohttp
import oauthlib.oauth1

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.figshare import metadata
from waterbutler.providers.figshare import settings
from waterbutler.providers.figshare import utils as figshare_utils


class FigshareProvider:

    def __new__(cls, auth, credentials, settings):
        if settings['container_type'] == 'project':
            return FigshareProjectProvider(auth, credentials, dict(settings, project_id=settings['container_id']))
        if settings['container_type'] in ('article', 'fileset'):
            return FigshareArticleProvider(auth, credentials, dict(settings, article_id=settings['container_id']))
        raise exceptions.ProviderError('Invalid "container_type" {0}'.format(settings['container_type']))


class BaseFigshareProvider(provider.BaseProvider):
    NAME = 'figshare'
    BASE_URL = settings.BASE_URL

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = oauthlib.oauth1.Client(
            self.credentials['client_token'],
            client_secret=self.credentials['client_secret'],
            resource_owner_key=self.credentials['owner_token'],
            resource_owner_secret=self.credentials['owner_secret'],
        )

    async def make_request(self, method, uri, *args, **kwargs):
        signed_uri, signed_headers, _ = self.client.sign(uri, method)
        signed_headers.update(kwargs.pop('headers', {}))
        kwargs['headers'] = signed_headers
        return (await super().make_request(method, signed_uri, *args, **kwargs))

    async def revalidate_path(self, base, path, folder=False):
        wbpath = base
        assert base.is_dir
        path = path.strip('/')

        for entry in (await self.metadata(base)):
            if entry.name == path:
                # base may when refering to a file will have a article id as well
                # This handles that case so the resulting path is actually correct
                names, ids = map(lambda x: getattr(entry, x).strip('/').split('/'), ('materialized_path', 'path'))
                while names and ids:
                    wbpath = wbpath.child(names.pop(0), _id=ids.pop(0))
                wbpath._is_folder = entry.kind == 'folder'
                return wbpath

        return base.child(path, folder=False)

    def can_duplicate_names(self):
        return True


class FigshareProjectProvider(BaseFigshareProvider):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.project_id = self.settings['project_id']

    async def validate_v1_path(self, path, **kwargs):
        return self.validate_path(path, **kwargs)

    async def validate_path(self, path, **kwargs):
        split = path.rstrip('/').split('/')[1:]
        wbpath = WaterButlerPath('/', _ids=(self.settings['project_id'], ), folder=True)

        if split:
            name_or_id = split.pop(0)
            try:
                article = await self._assert_contains_article(name_or_id)
            except ValueError:
                return wbpath.child(name_or_id, folder=False)
            except exceptions.ProviderError as e:
                if e.code not in (404, 401):
                    raise
                return wbpath.child(name_or_id, folder=False)

            wbpath = wbpath.child(article['title'], article['id'], folder=True)

        if split:
            provider = await self._make_article_provider(article['id'], check_parent=False)
            try:
                return (await provider.validate_path('/'.join([''] + split), parent=wbpath))
            except exceptions.ProviderError as e:
                if e.code not in (404, 401):
                    raise
                return wbpath.child(split.pop(0), folder=False)

        return wbpath

    async def _assert_contains_article(self, article_id):
        articles_json = await self._list_articles()
        try:
            return next(
                each for each in articles_json
                if each['id'] == int(article_id)
            )
        except StopIteration:
            raise exceptions.ProviderError(
                'Article {0} not found'.format(article_id),
                code=http.client.NOT_FOUND,
            )

    async def _make_article_provider(self, article_id, check_parent=True):
        article_id = str(article_id)
        if check_parent:
            await self._assert_contains_article(article_id)
        settings = {'article_id': article_id}
        return FigshareArticleProvider(self.auth, self.credentials, settings, child=True)

    async def _get_project_metadata(self):
        response = await self.make_request(
            'GET',
            self.build_url('projects', self.project_id),
            expects=(200, ),
        )
        data = await response.json()
        return data
        return metadata.FigshareProjectMetadata(data)

    async def _list_articles(self):
        response = await self.make_request(
            'GET',
            self.build_url('projects', self.project_id, 'articles'),
            expects=(200, ),
        )
        return (await response.json())

    async def _get_article_metadata(self, article_id):
        provider = await self._make_article_provider(article_id, check_parent=False)
        return (await provider.about())

    async def _project_metadata_contents(self):
        articles_json = await self._list_articles()
        contents = await asyncio.gather(*[
            self._get_article_metadata(each['id'])
            for each in articles_json
        ])
        return [each for each in contents if each]

    async def _create_article(self, name):
        response = await self.make_request(
            'POST',
            self.build_url('articles'),
            data=json.dumps({
                'title': name,
                'defined_type': 'dataset',
            }),
            headers={'Content-Type': 'application/json'},
            expects=(200, ),
        )
        return (await response.json())

    async def download(self, path, **kwargs):
        if path.identifier is None:
            raise exceptions.NotFoundError(str(path))

        provider = await self._make_article_provider(path.parts[1].identifier)
        return (await provider.download(path, **kwargs))

    async def upload(self, stream, path, **kwargs):
        if not path.parent.is_root:
            provider = await self._make_article_provider(path.parent.identifier)
        else:
            article_json = await self._create_article(path.name)
            provider = await self._make_article_provider(article_json['article_id'], check_parent=False)
            await provider._add_to_project(self.project_id)

        return (await provider.upload(stream, path, **kwargs))

    async def delete(self, path, **kwargs):
        provider = await self._make_article_provider(path.parts[1].identifier)

        if len(path.parts) == 3:
            await provider.delete(path, **kwargs)
        else:
            await provider._remove_from_project(self.project_id)

    async def metadata(self, path, **kwargs):
        if path.is_root:
            return (await self._project_metadata_contents())

        if path.identifier is None:
            raise exceptions.NotFoundError(str(path))

        provider = await self._make_article_provider(path.parts[1].identifier)
        return (await provider.metadata(path, **kwargs))

    async def revisions(self, path, **kwargs):
        raise exceptions.ProviderError({'message': 'figshare does not support file revisions.'}, code=405)


class FigshareArticleProvider(BaseFigshareProvider):

    def __init__(self, auth, credentials, settings, child=False):
        super().__init__(auth, credentials, settings)
        self.article_id = self.settings['article_id']
        self.child = child

    async def validate_v1_path(self, path, **kwargs):
        return await self.validate_path(path, **kwargs)

    async def validate_path(self, path, parent=None, **kwargs):
        split = path.rstrip('/').split('/')[1:]
        wbpath = parent or WaterButlerPath('/', _ids=(self.article_id, ), folder=True)

        if split:
            name = split.pop(0)

            try:
                fid = int(name)
            except ValueError:
                fid = name

            article_json = await self._get_article_json()
            try:
                wbpath = wbpath.child(**next(
                    {
                        '_id': x['id'],
                        'name': x['name'],
                    } for x in article_json['files']
                    if x['id'] == fid
                ))
            except StopIteration:
                wbpath = wbpath.child(name)

        return wbpath

    async def _get_article_json(self):
        response = await self.make_request(
            'GET',
            self.build_url('articles', self.article_id),
            expects=(200, ),
        )
        data = await response.json()
        return data['items'][0]

    async def _add_to_project(self, project_id):
        resp = await self.make_request(
            'PUT',
            self.build_url('projects', project_id, 'articles'),
            data=json.dumps({'article_id': int(self.article_id)}),
            headers={'Content-Type': 'application/json'},
            expects=(200, ),
        )
        return (await resp.json())

    async def _remove_from_project(self, project_id):
        resp = await self.make_request(
            'DELETE',
            self.build_url('projects', project_id, 'articles'),
            data=json.dumps({'article_id': int(self.article_id)}),
            headers={'Content-Type': 'application/json'},
            expects=(200, ),
        )
        return (await resp.json())

    def _serialize_item(self, item, parent):
        defined_type = item.get('defined_type')
        files = item.get('files')
        if defined_type == 'fileset':
            metadata_class = metadata.FigshareArticleMetadata
            metadata_kwargs = {}
        elif defined_type and not files:
            # Hide single-file articles with no contents
            return None
        else:
            metadata_class = metadata.FigshareFileMetadata
            metadata_kwargs = {'parent': parent, 'child': self.child}
            if defined_type:
                item = item['files'][0]
        return metadata_class(item, **metadata_kwargs)

    async def about(self):
        article_json = await self._get_article_json()
        return self._serialize_item(article_json, article_json)

    async def download(self, path, **kwargs):
        """Download a file. Note: Although Figshare may return a download URL,
        the `accept_url` parameter is ignored here, since Figshare does not
        support HTTPS for downloads.

        :param str path: Path to the key you want to download
        :rtype ResponseWrapper:
        """
        if path.identifier is None:
            raise exceptions.NotFoundError(str(path))

        file_metadata = await self.metadata(path)
        download_url = file_metadata.extra['downloadUrl']
        if download_url is None:
            raise exceptions.DownloadError(
                'Cannot download private files',
                code=http.client.FORBIDDEN,
            )
        resp = await aiohttp.request('GET', download_url)
        return streams.ResponseStreamReader(resp)

    async def delete(self, path, **kwargs):
        resp = await self.make_request(
            'DELETE',
            self.build_url('articles', str(self.article_id), 'files', str(path.identifier)),
            expects=(200, ),
            throws=exceptions.DeleteError,
        )
        await resp.release()

    async def upload(self, stream, path, **kwargs):
        article_json = await self._get_article_json()

        stream = streams.FormDataStream(
            filedata=(stream, path.name)
        )

        response = await self.make_request(
            'PUT',
            self.build_url('articles', self.article_id, 'files'),
            data=stream,
            expects=(200, ),
            headers=stream.headers,
        )

        data = await response.json()
        return metadata.FigshareFileMetadata(data, parent=article_json, child=self.child), True

    async def metadata(self, path, **kwargs):
        if path.identifier is None:
            raise exceptions.NotFoundError(str(path))

        article_json = await self._get_article_json()

        if path.is_root or str(path.identifier) == self.article_id:
            return [x for x in [
                self._serialize_item(item, parent=article_json)
                for item in article_json['files']
            ] if x]

        file_json = figshare_utils.file_or_error(article_json, path.identifier)
        return self._serialize_item(file_json, parent=article_json)

    async def revisions(self, path, **kwargs):
        raise exceptions.ProviderError({'message': 'figshare does not support file revisions.'}, code=405)
