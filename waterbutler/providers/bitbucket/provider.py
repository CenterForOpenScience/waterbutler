from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions

from waterbutler.providers.bitbucket import settings
from waterbutler.providers.bitbucket.path import BitbucketPath
from waterbutler.providers.bitbucket.metadata import BitbucketFileMetadata
from waterbutler.providers.bitbucket.metadata import BitbucketFolderMetadata
from waterbutler.providers.bitbucket.metadata import BitbucketRevisionMetadata


class BitbucketProvider(provider.BaseProvider):
    """Provider for Bitbucket repositories.

    API docs:

    * v1: https://confluence.atlassian.com/bitbucket/version-1-423626337.html

    * v2: https://developer.atlassian.com/bitbucket/api/2/reference/resource/

    Quirks:

    * Bitbucket does not have a (public) write API, so this provider is read-only. Attempting to
      upload, update, or delete files and folders on the provider will throw a 501 Not Implemented
      error.

    * I think bitbucket lets you name branches the same as commits.  Then how does it resolve them?
    """

    NAME = 'bitbucket'
    BASE_URL = settings.BASE_URL
    VIEW_URL = settings.VIEW_URL

    def __init__(self, auth, credentials, settings):
        super().__init__(auth, credentials, settings)
        self.name = self.auth.get('name', None)
        self.email = self.auth.get('email', None)
        self.token = self.credentials['token']
        self.owner = self.settings['owner']
        self.repo = self.settings['repo']
        self._parent_dir = None  # cache parent directory listing if v1

    @staticmethod
    def bitbucket_path_to_name(file_path: str, folder_path: str) -> str:
        """The bitbucket API has a curious convention for file paths.  If a file called ``foo.txt``
        exists in a subdirectory ``bar``, bar's path is ``bar/`` and foo's is ``bar/foo.txt``.
        If foo lives the root of the repository, the root path is ``/`` but the file path is just
        ``foo.txt`` (no leading slash).  This method constructs this style of paths to help when
        looking for entries in BB json responses"""
        name = file_path
        if name.startswith(folder_path):  # files under root don't need stripping
            name = name[len(folder_path):]  # strip folder path prefix from file name
        return name

    @property
    def default_headers(self) -> dict:
        return {'Authorization': 'Bearer {}'.format(self.token)}

    async def validate_v1_path(self, path: str, **kwargs) -> BitbucketPath:
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
            return BitbucketPath(path, _ids=[(commit_sha, branch_name)])

        path_obj = BitbucketPath(path)
        for part in path_obj.parts:
            part._id = (commit_sha, branch_name)

        self._parent_dir = await self._fetch_dir_listing(path_obj.parent)

        if path_obj.is_dir:
            if path_obj.name not in self._parent_dir['directories']:
                raise exceptions.NotFoundError(str(path))
        else:
            if path_obj.name not in [
                    self.bitbucket_path_to_name(x['path'], self._parent_dir['path'])
                    for x in self._parent_dir['files']
            ]:
                raise exceptions.NotFoundError(str(path))

        # _fetch_dir_listing will tell us the commit sha used to look up the listing
        # if not set in path_obj or if the lookup sha is shorter than the returned sha, update it
        if not commit_sha or (len(commit_sha) < len(self._parent_dir['node'])):
            path_obj.set_commit_sha(self._parent_dir['node'])

        return path_obj

    async def validate_path(self, path: str, **kwargs) -> BitbucketPath:
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
            return BitbucketPath(path, _ids=[(commit_sha, branch_name)])

        path_obj = BitbucketPath(path)
        for part in path_obj.parts:
            part._id = (commit_sha, branch_name)

        return path_obj

    def path_from_metadata(self,  # type: ignore
                           parent_path: BitbucketPath,
                           metadata) -> BitbucketPath:
        return parent_path.child(metadata.name, folder=metadata.is_folder)

    async def metadata(self, path: BitbucketPath, **kwargs):  # type: ignore
        """Get metadata about the requested file or folder.

        :param BitbucketPath path: A Bitbucket path object for the file or folder
        :rtype dict:
        :rtype list:
        """
        if path.is_dir:
            return (await self._metadata_folder(path, **kwargs))
        else:
            return (await self._metadata_file(path, **kwargs))

    async def revisions(self, path: BitbucketPath, **kwargs) -> list:  # type: ignore
        """Returns a list of revisions for a file.  As a VCS, Bitbucket doesn't have a single
        canonical history for a file.  The revisions returned will be those of the file starting
        with the reference supplied to or inferred by validate_v1_path().

        https://confluence.atlassian.com/bitbucket/repository-resource-1-0-296095202.html#repositoryResource1.0-GETsthehistoryofafileinachangeset

        """
        resp = await self.make_request(
            'GET',
            self._build_v1_repo_url('filehistory', path.ref, path.path),
            expects=(200, ),
            throws=exceptions.RevisionsError
        )
        revisions = await resp.json()

        valid_revisions = []
        for revision in revisions:
            file_was_removed = False
            for file_status in revision['files']:
                if file_status['type'] == 'removed' and file_status['file'] == path.full_path.lstrip('/'):
                    file_was_removed = True
                    # break  #  don't save this one, move to next revision

            if not file_was_removed:
                valid_revisions.append(revision)

        return [
            BitbucketRevisionMetadata(item)
            for item in valid_revisions
        ]

    async def download(self, path: BitbucketPath, **kwargs):  # type: ignore
        '''Get the stream to the specified file on bitbucket
        :param str path: The path to the file on bitbucket
        '''
        metadata = await self.metadata(path)

        resp = await self.make_request(
            'GET',
            self._build_v1_repo_url('raw', path.commit_sha, *path.path_tuple()),
            expects=(200, ),
            throws=exceptions.DownloadError,
        )

        return streams.ResponseStreamReader(resp, size=metadata.size)

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

    # copy is okay if source is bitbucket and destination is not
    async def copy(self, dest_provider, *args, **kwargs):
        if dest_provider.NAME == self.NAME:
            raise exceptions.ReadOnlyProviderError(self.NAME)
        return await super().copy(dest_provider, *args, **kwargs)

    def _build_v1_repo_url(self, *segments, **query):
        segments = ('1.0', 'repositories', self.owner, self.repo) + segments
        return self.build_url(*segments, **query)

    async def _metadata_file(self, path: BitbucketPath, revision: str=None, **kwargs):
        """Fetch metadata for a single file

        :param BitbucketPath path: the path whose metadata should be retrieved
        :param str revision:
        :rtype BitbucketFileMetadata:
        :return: BitbucketFileMetadata object
        """

        parent = path.parent
        if self._parent_dir is None or self._parent_dir['path'] != str(parent):
            parent_dir = await self._fetch_dir_listing(parent)
        else:
            parent_dir = self._parent_dir

        data = [
            x for x in parent_dir['files']
            if path.name == self.bitbucket_path_to_name(x['path'], parent_dir['path'])
        ]

        return BitbucketFileMetadata(data[0], path, owner=self.owner, repo=self.repo)

    async def _metadata_folder(self, folder: BitbucketPath, **kwargs) -> list:

        this_dir = await self._fetch_dir_listing(folder)
        if folder.commit_sha is None:
            folder.set_commit_sha(this_dir['node'])

        ret = []
        for name in this_dir['directories']:
            ret.append(BitbucketFolderMetadata(
                {'name': name},
                folder.child(name, folder=True),
                owner=self.owner,
                repo=self.repo,
            ))

        for item in this_dir['files']:
            name = self.bitbucket_path_to_name(item['path'], this_dir['path'])
            # TODO: mypy doesn't like the mix of File & Folder Metadata objects
            ret.append(BitbucketFileMetadata(  # type: ignore
                item,
                folder.child(name, folder=False),
                owner=self.owner,
                repo=self.repo,
            ))

        return ret

    async def _fetch_default_branch(self) -> str:
        """Get the name of the default branch ("main branch" in bitbucket parlance) of the
        attached repository.

        https://confluence.atlassian.com/bitbucket/repository-resource-1-0-296095202.html#repositoryResource1.0-GETtherepository%27smainbranch

        :rtype str:
        :return: the name of the attached repo's default branch.
        """
        resp = await self.make_request(
            'GET',
            self._build_v1_repo_url('main-branch'),
            expects=(200, ),
            throws=exceptions.ProviderError
        )
        return (await resp.json())['name']

    async def _fetch_dir_listing(self, folder: BitbucketPath) -> dict:
        """Get listing of contents within a BitbucketPath folder object.

        https://confluence.atlassian.com/bitbucket/src-resources-296095214.html#srcResources-GETalistofreposource

        Note::

            Using this endpoint for a file will return the file contents.

        :param BitbucketPath folder: the folder whose contents should be listed
        :rtype dict:
        :returns: a directory listing of the contents of the folder
        """
        assert folder.is_dir  # don't use this method on files

        resp = await self.make_request(
            'GET',
            self._build_v1_repo_url('src', folder.ref, *folder.path_tuple()) + '/',
            expects=(200, ),
            throws=exceptions.ProviderError,
        )
        return await resp.json()
