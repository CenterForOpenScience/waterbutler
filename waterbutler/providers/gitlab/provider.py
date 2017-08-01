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

    * git doesn't have a concept of empty folders, so this provider creates 0-byte ``.gitkeep``
      files in the requested folder.

    """
    NAME = 'gitlab'

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

    @property
    def committer(self):
        """ Information about the commit author.

        :rtype: :class:`dict` with `name` and `email` of the author
        """
        return {
            'name': self.name,
            'email': self.email,
        }

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
        :raises: :class:`waterbutler.core.exceptions.MetadataError`
        """
        try:
            if path.is_dir:
                return (await self._metadata_folder(path, **kwargs))
            else:
                return (await self._metadata_file(path, **kwargs))
        except:
            raise exceptions.MetadataError('error on fetch metadata from path {0}'
                    .format(path.full_path))

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

        :param str path: The path to the file on gitlab
        :param dict kwargs: Ignored
        :raises: :class:`waterbutler.core.exceptions.DownloadError`
        """

        url = None
        if path.commit_sha:
            url = self._build_repo_url('repository', 'files', path.full_path, ref=path.commit_sha)
        else:
            url = self._build_repo_url('repository', 'files', path.full_path, ref=path.branch_name)

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
        """Build the repository url with the params, retuning the complete repository url.

        :param list segments: The list of child paths
        :param dict query: The query used to append the parameters on url
        :rtype: str
        """
        segments = ('projects', self.repo_id) + segments
        return self.build_url(*segments, **query)

    async def _fetch_file_contents(self, path):
        url = self._build_repo_url('repository', 'files', path.full_path, ref=path.branch_name)
        resp = await self.make_request(
            'GET',
            url,
            expects=(200,),
            throws=exceptions.NotFoundError(path.full_path)
        )
        return await resp.json()

    async def _fetch_tree_contents(self, path):
        if path.is_root:
            url = self._build_repo_url('repository', 'tree', ref=path.branch_name)
        else:
            url = self._build_repo_url('repository', 'tree',
                                       path=path.raw_path, ref=path.branch_name)

        resp = await self.make_request(
            'GET',
            url,
            expects=(200, 404),
            throws=exceptions.NotFoundError(path.full_path)
        )

        data = await resp.json()

        if isinstance(data, dict):
            # Empty Project
            if data['message'] == '404 Tree Not Found':
                return []
            # True Not Found
        elif resp.status == 404:
            raise exceptions.NotFoundError(path.full_path)

        return data

    async def _fetch_default_branch(self):
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
                ret.append(GitLabFileMetadata(item, file_path, host=self.VIEW_URL,
                                              owner=self.owner, repo=self.repo))

        return ret

    async def _metadata_file(self, path, **kwargs):
        file_contents = await self._fetch_file_contents(path)
        if not file_contents:
            raise exceptions.NotFoundError(str(path))

        file_name = file_contents['file_name']
        data = {'name': file_name, 'id': file_contents['blob_id'],
                'path': file_contents['file_path'], 'size': file_contents['size']}

        mimetype = mimetypes.guess_type(file_name)[0]
        if mimetype:
            data['mimetype'] = mimetype

        return GitLabFileMetadata(data, path, host=self.VIEW_URL, owner=self.owner, repo=self.repo)
