import json
import base64
import aiohttp
import mimetypes

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions

from waterbutler.providers.gitlab.path import GitLabPath
from waterbutler.providers.gitlab.metadata import GitLabRevision
from waterbutler.providers.gitlab.metadata import GitLabFileMetadata
from waterbutler.providers.gitlab.metadata import GitLabFolderMetadata


class GitLabProvider(provider.BaseProvider):
    """Provider for GitLab repositories.

    API docs: https://docs.gitlab.com/ce/api/

    Quirks:

    * Metadata for files will change depending on the path used to fetch it.  If the file metadata
      comes from a listing of the parent folder, the ``size`` property will be `None`.

    * The GitLab provider cannot determine the ``modified``, ``modified_utc``, or ``created_utc``
      for metadata properties for any files.

    * GitLab does not do content-type detection, so the ``contentType`` property is inferred in WB
      from the file extension.

    * If a path is given with ``commit_sha``, ``branch_name``, and ``revision`` parameters, then
      ``revision`` will overwrite whichever of the other two it is determined to be.
    """
    NAME = 'gitlab'

    MAX_PAGE_SIZE = 100

    def __init__(self, auth, credentials, settings):
        super().__init__(auth, credentials, settings)
        self.name = self.auth.get('name', None)
        self.email = self.auth.get('email', None)
        self.token = self.credentials['token']
        self.owner = self.settings['owner']
        self.repo = self.settings['repo']
        self.repo_id = self.settings['repo_id']
        self.BASE_URL = self.settings['host'] + '/api/v4'
        self.VIEW_URL = self.settings['host']

    @property
    def default_headers(self):
        """ Headers to be included with every request.

        :rtype: :class:`dict` with `PRIVATE-TOKEN` token
        """
        return {'PRIVATE-TOKEN': str(self.token)}

    async def validate_v1_path(self, path, **kwargs):
        """Ensure path is in Waterbutler v1 format.

        :param str path: The path to a file/folder
        :rtype: GitLabPath
        :raises: :class:`waterbutler.core.exceptions.NotFoundError`
        """

        gl_path = await self.validate_path(path, **kwargs)
        if gl_path.is_root:
            return gl_path

        data = await self._fetch_tree_contents(gl_path.parent)

        type_needed = 'tree' if gl_path.is_dir else 'blob'
        found = [x for x in data if x['type'] == type_needed and x['name'] == gl_path.name]

        if not found:
            raise exceptions.NotFoundError(str(gl_path))

        return gl_path

    async def validate_path(self, path, **kwargs):
        """Ensure path is in Waterbutler format.

        :param str path: The path to a file
        :rtype: GitLabPath
        """
        commit_sha = kwargs.get('commitSha')
        branch_name = kwargs.get('branch')

        # revision query param could be commit sha OR branch
        # take a guess which one it will be.
        revision = kwargs.get('revision', None)
        if revision is not None:
            try:
                int(revision, 16)  # is revision valid hex?
            except (TypeError, ValueError):
                branch_name = revision
            else:
                commit_sha = revision

        if not commit_sha and not branch_name:
            branch_name = await self._fetch_default_branch()

        if path == '/':
            return GitLabPath(path, _ids=[(commit_sha, branch_name)])

        gl_path = GitLabPath(path)
        for part in gl_path.parts:
            part._id = (commit_sha, branch_name)

        return gl_path

    def path_from_metadata(self,  # type: ignore
                           parent_path: GitLabPath,
                           metadata) -> GitLabPath:
        return parent_path.child(metadata.name, folder=metadata.is_folder)

    async def metadata(self, path, **kwargs):
        """Get Metadata about the requested file or folder.

        :param GitLabPath path: The path to a file or folder
        :rtype: :class:`GitLabFileMetadata`
        :rtype: :class:`list` of :class:`GitLabFileMetadata` or :class:`GitLabFolderMetadata`
        """
        if path.is_dir:
            return (await self._metadata_folder(path, **kwargs))
        else:
            return (await self._metadata_file(path, **kwargs))

    async def revisions(self, path, sha=None, **kwargs):
        """Get past versions of the request file.

        :param str path: The user specified path
        :param str sha: The sha of the revision
        :param dict kwargs: Ignored
        :rtype: :class:`list` of :class:`GitLabRevision`
        :raises: :class:`waterbutler.core.exceptions.RevisionsError`
        """
        url = self._build_repo_url('repository', 'commits', path=path.path,
                                   ref_name=path.branch_name)
        resp = await self.make_request(
            'GET',
            url,
            expects=(200,),
            throws=exceptions.RevisionsError
        )

        return [GitLabRevision(item) for item in (await resp.json())]

    async def download(self, path, **kwargs):
        """Get the stream to the specified file on gitlab.

        There is an endpoint for downloading the raw file directly, but we cannot use it because
        GitLab requires periods in the file path to be encoded.  Python and aiohttp make this
        difficult, though their behavior is arguably correct. See
        https://gitlab.com/gitlab-org/gitlab-ce/issues/31470 for details.

        :param str path: The path to the file on gitlab
        :param dict kwargs: Ignored
        :raises: :class:`waterbutler.core.exceptions.DownloadError`
        """

        ref = path.commit_sha or path.branch_name
        url = self._build_repo_url('repository', 'files', path.full_path, ref=ref)
        resp = await self.make_request(
            'GET',
            url,
            expects=(200,),
            throws=exceptions.DownloadError,
        )

        data = await resp.json()
        raw = base64.b64decode(data['content'])

        mdict_options = {}
        mimetype = mimetypes.guess_type(path.full_path)[0]
        if mimetype is not None:
            mdict_options['CONTENT-TYPE'] = mimetype

        mdict = aiohttp.multidict.MultiDict(resp.headers)
        mdict.update(mdict_options)
        resp.headers = mdict
        resp.content = streams.StringStream(raw)

        return streams.ResponseStreamReader(resp, len(raw))

    def can_duplicate_names(self):
        return False

    def can_intra_move(self, other, path=None):
        return False

    def can_intra_copy(self, other, path=None):
        return False

    async def upload(self, *args, **kwargs):
        raise exceptions.ReadOnlyProviderError(self.NAME)

    async def create_folder(self, *args, **kwargs):
        raise exceptions.ReadOnlyProviderError(self.NAME)

    async def delete(self, *args, **kwargs):
        raise exceptions.ReadOnlyProviderError(self.NAME)

    async def move(self, *args, **kwargs):
        raise exceptions.ReadOnlyProviderError(self.NAME)

    # copy is okay if source is gitlab and destination is not
    async def copy(self, dest_provider, *args, **kwargs):
        if dest_provider.NAME == self.NAME:
            raise exceptions.ReadOnlyProviderError(self.NAME)
        return await super().copy(dest_provider, *args, **kwargs)

    def _build_repo_url(self, *segments, **query):
        """Build the repository url with the params, returning the complete repository url.

        :param list segments: The list of child paths
        :param dict query: The query used to append the parameters on url
        :rtype: str
        """
        segments = ('projects', self.repo_id) + segments
        return self.build_url(*segments, **query)

    async def _metadata_folder(self, path, **kwargs):
        data = await self._fetch_tree_contents(path)

        ret = []
        for item in data:
            name = item['name']
            if item['type'] == 'tree':
                folder_path = path.child(name, folder=True)
                ret.append(GitLabFolderMetadata(item, folder_path))
            else:
                file_path = path.child(name, folder=False)
                item['mime_type'] = mimetypes.guess_type(name)[0]
                ret.append(GitLabFileMetadata(item, file_path, host=self.VIEW_URL,
                                              owner=self.owner, repo=self.repo))

        return ret

    async def _metadata_file(self, path, **kwargs):
        file_contents = await self._fetch_file_contents(path)

        file_name = file_contents['file_name']
        data = {'name': file_name, 'id': file_contents['blob_id'],
                'path': file_contents['file_path'], 'size': file_contents['size']}

        data['mime_type'] = mimetypes.guess_type(file_name)[0]

        return GitLabFileMetadata(data, path, host=self.VIEW_URL, owner=self.owner, repo=self.repo)

    async def _fetch_file_contents(self, path):
        """

        Modified date is available by looking up `last_commit_id`.

        Created date is not available.

        API docs: https://docs.gitlab.com/ce/api/repository_files.html#get-file-from-repository

        """
        url = self._build_repo_url('repository', 'files', path.raw_path, ref=path.branch_name)
        resp = await self.make_request(
            'GET',
            url,
            expects=(200,),
            throws=exceptions.NotFoundError,
        )
        raw_data = (await resp.read()).decode("utf-8")
        data = None
        try:
            data = json.loads(raw_data)
        except json.decoder.JSONDecodeError:
            # GitLab API sometimes returns ruby hashes instead of json
            # see: https://gitlab.com/gitlab-org/gitlab-ce/issues/31790
            data = self._convert_ruby_hash_to_dict(raw_data)

        return data

    async def _fetch_tree_contents(self, path):
        """

        API docs: https://docs.gitlab.com/ce/api/repositories.html#list-repository-tree

        Pagination: https://docs.gitlab.com/ce/api/README.html#pagination

        """

        data, page_nbr = [], 1
        while page_nbr:
            path_args = ['repository', 'tree']
            path_kwargs = {'ref': path.branch_name, 'page': page_nbr,
                           'per_page': self.MAX_PAGE_SIZE}
            if not path.is_root:
                path_kwargs['path'] = path.full_path

            url = self._build_repo_url(*path_args, **path_kwargs)
            resp = await self.make_request(
                'GET',
                url,
                expects=(200, 404),
                throws=exceptions.NotFoundError,
            )
            data_page = await resp.json()

            if isinstance(data_page, dict):
                if data_page['message'] == '404 Tree Not Found':  # Empty Project
                    break
                elif resp.status == 404:  # True Not Found
                    raise exceptions.NotFoundError(path.full_path)

            # GitLab currently returns 200 OK for nonexistent directories
            # See: https://gitlab.com/gitlab-org/gitlab-ce/issues/34016
            # Fallback: empty directories shouldn't exist in git,
            if page_nbr == 1 and len(data_page) == 0:
                raise exceptions.NotFoundError(path.full_path)

            data.extend(data_page)
            page_nbr = resp.headers.get('X-Next-Page', None)

        return data

    async def _fetch_default_branch(self):
        """

        Docs: https://docs.gitlab.com/ce/api/projects.html#get-single-project

        """
        url = self._build_repo_url()
        resp = await self.make_request(
            'GET',
            url,
            expects=(200,),
            throws=exceptions.NotFoundError,
        )
        data = await resp.json()

        if 'default_branch' not in data:
            raise exceptions.NotFoundError

        return data['default_branch']

    def _convert_ruby_hash_to_dict(self, ruby_hash):
        """Adopted from: https://stackoverflow.com/a/19322785"""
        dict_str = ruby_hash.replace(":", '"')     # Remove the ruby object key prefix
        dict_str = dict_str.replace("=>", '" : ')  # swap the k => v notation, and close any unshut quotes
        dict_str = dict_str.replace('""', '"')     # strip back any double quotes we created to sinlges
        return json.loads(dict_str)
