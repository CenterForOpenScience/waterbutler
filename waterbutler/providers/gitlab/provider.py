import json
import base64
import typing
import aiohttp
import mimetypes

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions

from waterbutler.providers.gitlab.path import GitLabPath
from waterbutler.providers.gitlab.metadata import (BaseGitLabMetadata,
                                                   GitLabRevision,
                                                   GitLabFileMetadata,
                                                   GitLabFolderMetadata)


class GitLabProvider(provider.BaseProvider):
    """Provider for GitLab repositories.  GitLab is an open-source GitHub clone that can be hosted
    personally or externally. This provider consumes the v4 GitLab API and uses Personal Access
    Tokens for auth.  A valid host must support those two features to be useable.

    API docs: https://docs.gitlab.com/ce/api/

    Personal access tokens: https://docs.gitlab.com/ee/user/profile/personal_access_tokens.html

    Quirks:

    * This provider is currently read-only, meaning it supports metadata, download, download-as-zip,
      revision, and copy-to-external-provider requests.  Write support is possible and will be added
      at a later date.

    * Metadata for files will change depending on the path used to fetch it.  If the file metadata
      comes from a listing of the parent folder, the ``size``, ``modified``, ``modified_utc``, and
      ``created_utc`` properties will be `None`.

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
    def default_headers(self) -> dict:
        return {'PRIVATE-TOKEN': str(self.token)}

    async def validate_v1_path(self, path: str, **kwargs) -> GitLabPath:
        """Turns the string ``path`` into a `GitLabPath` object. See `validate_path` for details.
        This method does much the same as `validate_path`, but does two extra validation steps.
        First it checks to see if the object identified by ``path`` already exists in the repo,
        throwing a 404 if not.  It then checks to make sure the v1 file/folder semantics are
        respected.

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

    async def validate_path(self, path: str, **kwargs) -> GitLabPath:
        """Turn the string ``path`` into a `GitLabPath` object. Will infer the branch/commit
        information from the query params or from the default branch for the repo if those are
        not provided.  Does no validation to ensure that the entity described by ``path`` actually
        exists.

        Valid kwargs are ``commitSha``, ``branch``, and ``revision``.  If ``revision`` is given,
        its value will be assigned to the commit SHA if it is a valid base-16 number, or branch
        otherwise.  ``revision`` will override ``commitSha`` or ``branch``.  If both a commit SHA
        and branch name are given, both will be associated with the new GitLabPath object.  No
        effort is made to ensure that they point to the same thing.  `GitLabPath` objects default
        to commit SHAs over branch names when building API calls, as a commit SHA is more specific.

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
        """Build a GitLabPath for a the child of ``parent_path`` described by ``metadata``."""
        return parent_path.child(metadata.name, folder=metadata.is_folder)

    async def metadata(self,  # type: ignore
                       path: GitLabPath, **kwargs):
        """Returns file metadata if ``path`` is a file, or a list of metadata objects of the
        children of ``path`` if it is a folder.

        :param GitLabPath path: The path to a file or folder
        :rtype: :class:`GitLabFileMetadata`
        :rtype: :class:`list` of :class:`GitLabFileMetadata` or :class:`GitLabFolderMetadata`
        """
        if path.is_dir:
            return await self._metadata_folder(path)
        else:
            return await self._metadata_file(path)

    async def revisions(self,  # type: ignore
                        path: GitLabPath, **kwargs) -> typing.List[GitLabRevision]:
        """Get the revision history for the file at ``path``.  Returns a list of `GitLabRevision`
        objects representing each version of the file where the file was modified.

        API docs: https://docs.gitlab.com/ce/api/commits.html#list-repository-commits

        Note: ``path`` is not a documented parameter of the above GL endpoint, but seems to work.

        :param GitLabPath path: The file to fetch revision history for
        :param dict \*\*kwargs: ignored
        :rtype: `list` of :class:`GitLabRevision`
        :raises: :class:`waterbutler.core.exceptions.RevisionsError`
        """
        url = self._build_repo_url('repository', 'commits', path=path.path,
                                   ref_name=path.ref)
        resp = await self.make_request(
            'GET',
            url,
            expects=(200,),
            throws=exceptions.RevisionsError
        )
        data = await resp.json()
        if len(data) == 0:
            raise exceptions.RevisionsError('No revisions found', code=404)

        return [GitLabRevision(item) for item in data]

    async def download(self,  # type: ignore
                       path: GitLabPath, **kwargs):
        """Return a stream to the specified file on GitLab.

        There is an endpoint for downloading the raw file directly, but we cannot use it because
        GitLab requires periods in the file path to be encoded.  Python and aiohttp make this
        difficult, though their behavior is arguably correct. See
        https://gitlab.com/gitlab-org/gitlab-ce/issues/31470 for details.

        API docs: https://docs.gitlab.com/ce/api/repository_files.html#get-file-from-repository

        This uses the same endpoint as `_fetch_file_contents`, but relies on the response headers,
        which are not returned by that method.  It may also be replaced when the above bug is
        fixed.

        :param str path: The path to the file on GitLab
        :param dict \*\*kwargs: Ignored
        :raises: :class:`waterbutler.core.exceptions.DownloadError`
        """

        url = self._build_repo_url('repository', 'files', path.full_path, ref=path.ref)
        resp = await self.make_request(
            'GET',
            url,
            expects=(200,),
            throws=exceptions.DownloadError,
        )

        raw_data = (await resp.read()).decode("utf-8")
        data = None
        try:
            data = json.loads(raw_data)
        except json.decoder.JSONDecodeError:
            # GitLab API sometimes returns ruby hashes instead of json
            # see: https://gitlab.com/gitlab-org/gitlab-ce/issues/31790
            data = self._convert_ruby_hash_to_dict(raw_data)

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

        :param list \*segments: a list of child paths
        :param dict \*\*query: query parameters to append to the url
        :rtype: str
        """
        segments = ('projects', self.repo_id) + segments
        return self.build_url(*segments, **query)

    async def _metadata_folder(self, path: GitLabPath) -> typing.List[BaseGitLabMetadata]:
        """Fetch metadata for the contents of the folder at ``path`` and return a `list` of
        `GitLabFileMetadata` and `GitLabFolderMetadata` objects.

        :param GitLabPath path: `GitLabPath` representing a folder
        :rtype: `list`
        """
        data = await self._fetch_tree_contents(path)

        ret = []  # type: typing.List[BaseGitLabMetadata]
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

    async def _metadata_file(self, path: GitLabPath) -> GitLabFileMetadata:
        """Fetch metadata for the file at ``path`` and build a `GitLabFileMetadata` object for it.

        :param GitLabPath path: the file to get metadata for
        :rtype: `GitLabFileMetadata`
        """
        file_contents = await self._fetch_file_contents(path)

        # go to commit history to get modified and created dates
        last_commit, first_commit, page_nbr = None, None, 1
        while page_nbr:
            url = self._build_repo_url('repository', 'commits', path=path.path,
                                       ref_name=path.ref, page=page_nbr,
                                       per_page=self.MAX_PAGE_SIZE)
            resp = await self.make_request(
                'GET',
                url,
                expects=(200, 404),
                throws=exceptions.NotFoundError,
            )
            if resp.status == 404:
                raise exceptions.NotFoundError(path.full_path)

            data_page = await resp.json()

            # GitLab currently returns 200 OK for nonexistent directories
            # See: https://gitlab.com/gitlab-org/gitlab-ce/issues/34016
            # Fallback: empty directories shouldn't exist in git,
            if page_nbr == 1 and len(data_page) == 0:
                raise exceptions.NotFoundError(path.full_path)

            if page_nbr == 1:
                last_commit = data_page[0]

            first_commit = data_page[-1]
            page_nbr = resp.headers.get('X-Next-Page', None)

        file_name = file_contents['file_name']
        data = {'name': file_name, 'id': file_contents['blob_id'],
                'path': file_contents['file_path'], 'size': file_contents['size'],
                'mime_type': mimetypes.guess_type(file_name)[0],
                'modified': last_commit['committed_date'],
                'created': first_commit['committed_date'], }

        return GitLabFileMetadata(data, path, host=self.VIEW_URL, owner=self.owner, repo=self.repo)

    async def _fetch_file_contents(self, path: GitLabPath) -> dict:
        """Fetch and return the metadata for the file represented by ``path``.  Metadata returned
        includes the file name, size, and base64-encoded content.

        API docs: https://docs.gitlab.com/ce/api/repository_files.html#get-file-from-repository

        :param GitLabPath path: the file to get metadata for
        :rtype: `dict`
        :return: file metadata from the GitLab endpoint
        """
        url = self._build_repo_url('repository', 'files', path.raw_path, ref=path.ref)
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

    async def _fetch_tree_contents(self, path: GitLabPath) -> list:
        """Looks up the contents of the folder represented by ``path``.  The GitLab API is
        paginated and all pages will be fetched and returned.  Each entry in the list is a simple
        `dict` containing ``id``, ``name``, ``type``, ``path``, and ``mode``.

        API docs: https://docs.gitlab.com/ce/api/repositories.html#list-repository-tree

        Pagination: https://docs.gitlab.com/ce/api/README.html#pagination

        :param GitLabPath path: the tree whose contents should be returned
        :rtype: `list`
        :return: list of `dict`s representing the tree's children
        """
        data, page_nbr = [], 1  # type: ignore
        while page_nbr:
            path_args = ['repository', 'tree']
            path_kwargs = {'ref': path.ref, 'page': page_nbr,
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
            if resp.status == 404:
                raise exceptions.NotFoundError(path.full_path)

            data_page = await resp.json()

            # GitLab currently returns 200 OK for nonexistent directories
            # See: https://gitlab.com/gitlab-org/gitlab-ce/issues/34016
            # Fallback: empty directories shouldn't exist in git,
            if page_nbr == 1 and len(data_page) == 0:
                raise exceptions.NotFoundError(path.full_path)

            data.extend(data_page)
            page_nbr = resp.headers.get('X-Next-Page', None)

        return data

    async def _fetch_default_branch(self) -> str:
        """Get the default branch configured for the repository.  Uninitialized repos do not have
        this property and throw an `UninitializedRepositoryError` if encountered.

        API docs: https://docs.gitlab.com/ce/api/projects.html#get-single-project

        :rtype: `str`
        :return: the name of the default branch for the repository.
        """
        url = self._build_repo_url()
        resp = await self.make_request(
            'GET',
            url,
            expects=(200,),
            throws=exceptions.NotFoundError,
        )
        data = await resp.json()

        if data['default_branch'] is None:
            raise exceptions.UninitializedRepositoryError('{}/{}'.format(self.owner, self.repo))

        return data['default_branch']

    def _convert_ruby_hash_to_dict(self, ruby_hash: str) -> dict:
        """Adopted from https://stackoverflow.com/a/19322785 as a workaround for
        https://gitlab.com/gitlab-org/gitlab-ce/issues/34016.

        :param str ruby_hash: serialized Ruby hash
        :rtype: `dict`
        :return: the data structure represented by the hash
        """
        dict_str = ruby_hash.replace(":", '"')     # Remove the ruby object key prefix
        dict_str = dict_str.replace("=>", '" : ')  # swap the k => v notation, and close any unshut quotes
        dict_str = dict_str.replace('""', '"')     # strip back any double quotes we created to sinlges
        return json.loads(dict_str)
