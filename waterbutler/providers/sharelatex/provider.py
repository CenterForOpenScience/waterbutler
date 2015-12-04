import asyncio

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.sharelatex.metadata import ShareLatexFileMetadata
from waterbutler.providers.sharelatex.metadata import ShareLatexFolderMetadata


class ShareLatexProvider(provider.BaseProvider):
    """Provider for ShareLaTeX"""

    NAME = 'sharelatex'

    def __init__(self, auth, credentials, settings):
        """
        :param dict auth: Not used
        :param dict credentials: Contains `auth_token` and `sharelatex_url`
        :param dict settings: Contains `project` the project_id
        """
        super().__init__(auth, credentials, settings)
        self.project_id = settings.get('project')
        self.auth_token = credentials.get('auth_token')
        self.sharelatex_url = credentials.get('sharelatex_url')

    @asyncio.coroutine
    def validate_v1_path(self, path, **kwargs):
        return self.validate_path(path, **kwargs)

    @asyncio.coroutine
    def validate_path(self, path, **kwargs):
        return WaterButlerPath(path, prepend='/')

    def can_duplicate_names(self):
        return False

    def build_url(self, *segments, **query):
        """Reimplementation of build_url to add the auth token on query
        and specify the api version

        :param dict \*segments: Other segments to be append on url
        :param dict \*\*query: Additional query arguments
        :returns: the `url` created
        :rtype: :class: `string`
        """
        query['auth_token'] = self.auth_token
        return provider.build_url(self.sharelatex_url, *segments, **query)

    @asyncio.coroutine
    def upload(self, stream, path, conflict='replace', **kwargs):
        """Not implemented on ShareLaTeX
        """
        pass

    @asyncio.coroutine
    def delete(self, path, **kwargs):
        """Not implemented on ShareLaTeX
        """
        pass

    @asyncio.coroutine
    def download(self, path, accept_url=False, range=None, **kwargs):
        """Returns the url when `accept_url` otherwise returns a ResponseWrapper
        (Stream) for the specified path and raises exception if the status from
        ShareLaTeX is not 200

        :param str path: Path to the file you want to download
        :param dict \*\*kwargs: Additional arguments that are ignored
        :returns: the `file stream`
        :rtype: :class:`waterbutler.core.streams.ResponseStreamReader`
        :returns: the `url`
        :rtype: :class:`string`
        :raises: :class:`waterbutler.core.exceptions.DownloadError`
        """
        segments = ('api', 'v1', 'project', self.project_id, 'file', path.path)
        url = self.build_url(*segments)

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
        """Get metdata about the specified resource from this provider.
        Will be a :class:`list` if the resource is a directory otherwise an instance of :class:`waterbutler.providers.sharelatex.metadata.ShareLatexFileMetadata`

        :param str path: The path to a project, file or tex document
        :param dict \*\*kwargs: Arguments to be parsed by child classes
        :rtype: :class:`waterbutler.providers.sharelatex.metadata.ShareLatexFileMetadata`
        :rtype: :class:`list` of :class:`ShareLatexFileMetadata` and :class:`ShareLatexFolderMetadata`
        :raises: :class:`waterbutler.core.exceptions.NotFoundError`
        :raises: :class:`waterbutler.core.exceptions.MetadataError`
        """
        segments = ('api', 'v1', 'project', self.project_id, 'docs')
        url = self.build_url(*segments)

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

        metadata = self._read_metadata_from_json(data, path)

        if path.is_file:
            found = self._search_file(path, metadata)
            if not found:
                raise exceptions.NotFoundError(str(path))
            return found

        return metadata

    def _read_metadata_from_json(self, data, path):
        ret = []
        metadata, base_path = self._read_path_metadata(data, path)
        docs = metadata['docs']
        files = metadata['fileRefs']
        folders = metadata['folders']

        for doc in docs:
            new_doc = self._metadata_doc(base_path, doc['name'])
            ret.append(new_doc)
        for fil in files:
            new_file = self._metadata_file(base_path, fil['name'], fil['mimetype'])
            ret.append(new_file)
        for fol in folders:
            ret.append(self._metadata_folder(base_path, fol['name']))

        return ret

    def _read_path_metadata(self, data, path):
        last_dir = data['rootFolder'][0]
        base_dir = ''
        path_exploded = path.path.strip('/').split('/')
        if path.is_file:
            path_exploded.pop()
        for p in path_exploded:
            folders = last_dir['folders']
            for folder in self._search_folders(p, folders):
                last_dir = folder
        if not path.is_root:
            path_exploded.insert(0, base_dir)
            base_dir = '/'.join(path_exploded)
        return last_dir, base_dir

    def _search_folders(self, name, folders):
        return [x for x in folders if x['name'] == name]

    def _only_files(self, metadata):
        return [x for x in metadata if x.kind == 'file']

    def _search_file(self, path, metadata):
        only_files = self._only_files(metadata)
        for i in only_files:
            if i.path == path.full_path:
                return i
        return None

    def _metadata_file(self, base_path, file_name='', mimetype='text/plain'):
        path = '/'.join([base_path, file_name])
        metadata = {
            'path': path,
            'name': file_name,
            'size': 123,  # TODO
            'mimetype': mimetype
        }
        return ShareLatexFileMetadata(metadata)

    def _metadata_folder(self, path, folder_name):
        metadata = {
            'path': path,
            'name': folder_name
        }
        return ShareLatexFolderMetadata(metadata)

    def _metadata_doc(self, base_path, file_name=''):
        return self._metadata_file(base_path, file_name, 'application/x-tex')
