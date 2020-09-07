import json
import typing
import logging
import mimetypes

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions

from waterbutler.providers.gitlab.path import GitLabPath
from waterbutler.providers.gitlab.metadata import (BaseGitLabMetadata,
                                                   GitLabRevision,
                                                   GitLabFileMetadata,
                                                   GitLabFolderMetadata)


logger = logging.getLogger(__name__)


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

    * There are three query parameters supported to identify the ref of a path, ``revision``,
      ``commitSha``, and ``branch``.  ``commitSha`` and ``branch`` are explicit and preferred.  If
      both are given, ``commitSha`` will take precedence, as it is more precise.  If ``revision`` is
      given, the provider will guess if it is a commit SHA or branch name and overwrite the
      appropriate parameter.

    * If an explicit ``commitSha`` is not provided the provider will look it up and set it on the
      `GitLabPath` object, so that it will be available in the returned metadata.
    """
    NAME = 'gitlab'

    MAX_PAGE_SIZE = 100

    def __init__(self, auth, credentials, settings, **kwargs):
        super().__init__(auth, credentials, settings, **kwargs)
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

        if gl_path.commit_sha is None:
            commit_sha = await self._get_commit_sha_for_branch(gl_path.branch_name)
            gl_path.set_commit_sha(commit_sha)

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
        r"""Get the revision history for the file at ``path``.  Returns a list of `GitLabRevision`
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
            expects=(200, 500),
            throws=exceptions.RevisionsError
        )
        if resp.status == 500:
            # GitLab API is buggy for unicode filenames. Affected files will still work, but
            # but will have empty created/modified dates. See docstring for _metadata_file
            await resp.release()
            return []  # temporary work around for uncommon bug

        data = await resp.json()
        if len(data) == 0:
            raise exceptions.RevisionsError('No revisions found', code=404)

        return [GitLabRevision(item) for item in data]

    async def download(self, path: GitLabPath, **kwargs):  # type: ignore
        r"""Return a stream to the specified file on GitLab.

        API Docs: https://docs.gitlab.com/ce/api/repository_files.html#get-raw-file-from-repository

        Historically this method was implemented using a different endpoint which returned the file
        data as a base-64 encoded string.  We used this endpoint because the one listed above was
        buggy (see: https://gitlab.com/gitlab-org/gitlab-ce/issues/31470).  That issue has since
        been fixed in GL.  We removed the workaround since it required slurping the file contents
        into memory.  As a side-effect, the Gitlab download() method no longer supports the Range
        header.  It had been manually implemented by array slicing the slurped data.  The raw file
        endpoint does not currently respect it.

        :param str path: The path to the file on GitLab
        :param dict \*\*kwargs: Ignored
        :raises: :class:`waterbutler.core.exceptions.DownloadError`
        """

        logger.debug('requested-range:: {}'.format(range))

        url = self._build_file_url(path, raw=True)
        resp = await self.make_request(
            'GET',
            url,
            expects=(200, 206, ),
            throws=exceptions.DownloadError,
        )

        logger.debug('download-headers:: {}'.format([(x, resp.headers[x]) for x in resp.headers]))

        # get size from X-Gitlab-Size header, since some responses don't set Content-Length
        return streams.ResponseStreamReader(resp, size=int(resp.headers['X-Gitlab-Size']))

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
        r"""Build the repository url with the params, returning the complete repository url.

        :param list \*segments: a list of child paths
        :param dict \*\*query: query parameters to append to the url
        :rtype: str
        """
        segments = ('projects', self.repo_id) + segments
        return self.build_url(*segments, **query)

    def _build_file_url(self, path: GitLabPath, raw: bool=False) -> str:
        """Build a url to GitLab's files endpoint.

        Quirk 1:

        This is done separately because the files endpoint requires unusual quoting of the path.
        GL requires that the directory-separating slashes in the full path of the file be url
        encoded.  Ex. a file called ``foo/bar/baz`` would be encoded as ``foo%2Fbar%2Fbaz``.  WB's
        default url-building methods would split the path, encode each segment, then rejoin them
        with literal slashes.  If we were to try to pre-encode the path, any encoded characters
        will be double-encoded

        Quirk 2:

        GitLab CE File API takes care of file operations.  Most of them share the same endpoint /
        URL format, and the specific action is determined by the HTTP method.

            CRUD: ``POST | GET | PUT | DELETE /projects/:id/repository/files/:file_path?``

        However, one issue with ``GET`` (i.e. read / download) is that the request returns a JSON
        response containing both the file metadata and the Base64 encoded file content.  Replacing
        ``GET`` with ``HEAD`` just returns the file metadata via response headers.

        Alternatively, WB now uses the dedicated endpoint for downloading the raw content of a file
        directly at ``GET /projects/:id/repository/files/:file_path/raw?``.  Set the optional param
        ``raw`` to ``True`` to use this endpoint in download.

        API docs:

        * https://docs.gitlab.com/ce/api/repository_files.html#get-file-from-repository

        * https://docs.gitlab.com/ce/api/repository_files.html#get-raw-file-from-repository

        * https://docs.gitlab.com/ce/api/README.html#namespaced-path-encoding

        :param GitLabPath path: path to a file
        :param bool raw: get raw file content
        :rtype: str
        :return: url to the GitLab files endpoint for the given file
        """
        file_base = self._build_repo_url('repository', 'files')
        suffix = '/raw' if raw else ''
        return '{}/{}{}?ref={}'.format(file_base, path.raw_path.replace('/', '%2F'), suffix,
                                       path.ref)

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

        The commits endpoint used here will 500 when given a unicode filename.  Bug reported here:
        https://gitlab.com/gitlab-org/gitlab-ce/issues/40776  The `revisions` method uses the same
        endpoint and will probably encounter the same issue.

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
            logger.debug('file metadata commit history url: {}'.format(url))
            resp = await self.make_request(
                'GET',
                url,
                expects=(200, 404, 500),
                throws=exceptions.NotFoundError,
            )
            if resp.status == 404:
                await resp.release()
                raise exceptions.NotFoundError(path.full_path)
            if resp.status == 500:
                # GitLab API is buggy for unicode filenames. Affected files will still work, but
                # but will have empty created/modified dates. See method docstring
                await resp.release()
                break

            data_page = await resp.json()

            # GitLab currently returns 200 OK for nonexistent directories
            # See: https://gitlab.com/gitlab-org/gitlab-ce/issues/34016
            # Fallback: empty directories shouldn't exist in git, unless it's the root
            if page_nbr == 1 and len(data_page) == 0 and not path.is_root:
                raise exceptions.NotFoundError(path.full_path)

            if page_nbr == 1:
                last_commit = data_page[0]

            first_commit = data_page[-1]
            page_nbr = resp.headers.get('X-Next-Page', None)

        file_name = file_contents['file_name']
        data = {'name': file_name, 'id': file_contents['blob_id'],
                'path': file_contents['file_path'], 'size': file_contents['size'],
                'mime_type': mimetypes.guess_type(file_name)[0]}

        if last_commit is not None:
            data['modified'] = last_commit['committed_date']

        if first_commit is not None:
            data['created'] = first_commit['committed_date']

        return GitLabFileMetadata(data, path, host=self.VIEW_URL, owner=self.owner, repo=self.repo)

    async def _fetch_file_contents(self, path: GitLabPath) -> dict:
        """Fetch and return the metadata for the file represented by ``path``.  Metadata returned
        includes the file name, size, and base64-encoded content.

        API docs: https://docs.gitlab.com/ce/api/repository_files.html#get-file-from-repository

        :param GitLabPath path: the file to get metadata for
        :rtype: `dict`
        :return: file metadata from the GitLab endpoint
        """
        url = self._build_file_url(path)
        logger.debug('_fetch_file_contents url: {}'.format(url))
        resp = await self.make_request(
            'GET',
            url,
            expects=(200,),
            throws=exceptions.NotFoundError,
        )
        raw_data = (await resp.read()).decode("utf-8")
        return json.loads(raw_data)

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
            logger.debug('_fetch_tree_contents url: {}'.format(url))
            resp = await self.make_request(
                'GET',
                url,
                expects=(200, 404),
                throws=exceptions.NotFoundError,
            )
            if resp.status == 404:
                await resp.release()
                raise exceptions.NotFoundError(path.full_path)

            data_page = await resp.json()

            # GitLab currently returns 200 OK for nonexistent directories
            # See: https://gitlab.com/gitlab-org/gitlab-ce/issues/34016
            # Fallback: empty directories shouldn't exist in git, unless it's the root
            if page_nbr == 1 and len(data_page) == 0 and not path.is_root:
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

    async def _get_commit_sha_for_branch(self, branch_name: str) -> str:
        """Translate a branch name into the SHA of the commit it currently points to.

        API docs: https://docs.gitlab.com/ee/api/branches.html#get-single-repository-branch

        :param str branch_name: name of a branch in the repo
        :rtype: `str`
        :return: the SHA of the commit that `branch_name` points to
        """

        url = self._build_repo_url('repository', 'branches', branch_name)
        resp = await self.make_request(
            'GET',
            url,
            expects=(200,),
            throws=exceptions.NotFoundError,
        )
        data = await resp.json()
        return data['commit']['id']
