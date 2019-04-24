import logging
from typing import List, Tuple

from waterbutler.core import exceptions, provider, streams

from waterbutler.providers.bitbucket.path import BitbucketPath
from waterbutler.providers.bitbucket import settings as pd_settings
from waterbutler.providers.bitbucket.metadata import (BitbucketFileMetadata,
                                                      BitbucketFolderMetadata,
                                                      BitbucketRevisionMetadata, )

logger = logging.getLogger(__name__)


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

    * Bitbucket doesn't respect Range header on downloads for either v1.0 or v2.0 API
    """

    NAME = 'bitbucket'
    BASE_URL = pd_settings.BASE_URL
    VIEW_URL = pd_settings.VIEW_URL

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

    async def download(self, path: BitbucketPath,  # type: ignore
                       range: Tuple[int, int]=None, **kwargs) -> streams.ResponseStreamReader:
        """Get the stream to the specified file on Bitbucket

        :param path: The path to the file on Bitbucket
        :param range: the range header
        """
        metadata = await self.metadata(path)

        logger.debug('requested-range:: {}'.format(range))
        resp = await self.make_request(
            'GET',
            self._build_v1_repo_url('raw', path.commit_sha, *path.path_tuple()),
            range=range,
            expects=(200, ),
            throws=exceptions.DownloadError,
        )
        logger.debug('download-headers:: {}'.format([(x, resp.headers[x]) for x in resp.headers]))

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

    def _build_v2_repo_url(self, *segments, **query):
        segments = ('2.0', 'repositories', self.owner, self.repo) + segments
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

    async def _metadata_folder(self, folder: BitbucketPath, **kwargs) -> List:

        # Fetch metadata itself
        dir_meta = await self._fetch_path_metadata(folder)
        dir_commit_sha = dir_meta['commit']['hash']
        dir_path = '/' if dir_meta['path'] == '' else dir_meta['path']

        # Fetch content list
        dir_list = await self._fetch_dir_listing(folder)

        # Set the commit hash
        if folder.commit_sha is None:
            folder.set_commit_sha(dir_commit_sha[:12])

        # Build the metadata to return
        ret = []
        for value in dir_list:
            if value['type'] == 'commit_file':
                name = self.bitbucket_path_to_name(value['path'], dir_path)
                # TODO: Find alternatives for timestamp
                item = {
                    'revision': value['commit']['hash'][:12],
                    'size': value['size'],
                    'path': value['path'],
                    'timestamp': None,
                    'utctimestamp': None
                }
                # TODO: mypy doesn't like the mix of File & Folder Metadata objects
                ret.append(  # type: ignore
                    BitbucketFileMetadata(
                        item,
                        folder.child(name, folder=False),
                        owner=self.owner,
                        repo=self.repo,
                    )
                )
            if value['type'] == 'commit_directory':
                ret.append(
                    BitbucketFolderMetadata(
                        {'name': value['path']},
                        folder.child(value['path'], folder=True),
                        owner=self.owner,
                        repo=self.repo,
                    )
                )

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

    async def _fetch_path_metadata(self, path: BitbucketPath) -> dict:
        """Get the metadata for folder and file itself.

        Bitbucket API 2.0 provides an easy way to fetch metadata for files and folders by simply
        appending ``?format=meta`` to the endpoint.  However, this new feature no longer provides
        two WB-required attributes: revision and timestamp.

        1) For the revision, we can still get it from the ``resp_dict['commit']['hash']``, of which
           the first 12 chars turn out to be the revision.
        2) TODO: add notes for how to get timestamp

        API Doc: https://developer.atlassian.com/bitbucket/api/2/reference/resource/repositories/%7Busername%7D/%7Brepo_slug%7D/src/%7Bnode%7D/%7Bpath%7D

        :param path: the file or folder of which the metadata is requested
        :return: the file metadata dict
        """

        resp = await self.make_request(
            'GET',
            self._build_v2_repo_url('src', path.ref, *path.path_tuple()) + '/?format=meta',
            expects=(200,),
            throws=exceptions.ProviderError,
        )
        return await resp.json()

    async def _fetch_dir_listing(self, folder: BitbucketPath) -> List:
        """Get a list of the folder's full contents (upto the max limit setting if there is one).

        Bitbucket API 2.0 refactored the response structure for listing folder contents.

        1) The response is paginated.  If ``resp_dict`` contains the key ``next``, the contents are
           partial.  The caller must use the URL provided by ``dict['next']`` to fetch the next page
           after this method returns.

        2) The response no longer provides two WB-required attributes: ``node`` (folder commit sha)
           and ``path``.  For the folder commit sha, we can still get it from the first 12 chars of
           the ``resp_dict['commit']['hash']``.

        TODO: add notes for ``path`` after more investigation

        API Doc: https://developer.atlassian.com/bitbucket/api/2/reference/resource/repositories/%7Busername%7D/%7Brepo_slug%7D/src/%7Bnode%7D/%7Bpath%7D

        :param folder: the folder of which the contents should be listed
        :returns: a list of the folder's full contents
        """
        dir_content = await self._fetch_dir_listing_first_page(folder)
        page_index = dir_content['page']
        page_len = dir_content['pagelen']
        next_url = dir_content.get('next', None)
        dir_list = dir_content['values']
        while next_url:
            if page_index > pd_settings.MAX_RESP_PAGE_NUMBER \
                    or page_len * page_index > pd_settings.MAX_DIR_LIST_SIZE:
                # TODO: Should we limit this? If so, by page number, by list size or both?
                pass
            more_content = await self._fetch_dir_listing_next_page(next_url)
            page_index = more_content['page']
            page_len = more_content['pagelen']
            next_url = more_content.get('next', None)
            dir_list.extend(more_content['values'])
        return dir_list

    async def _fetch_dir_listing_first_page(self, folder: BitbucketPath) -> dict:
        """Get the first page which lists the folder's full or partial contents.

        :param folder: the folder of which the contents should be listed
        :returns: a dict of which the ``['values']`` contains a list of the folder's contents
        """
        assert folder.is_dir  # don't use this method on files

        resp = await self.make_request(
            'GET',
            self._build_v2_repo_url('src', folder.ref, *folder.path_tuple()) + '/',
            expects=(200, ),
            throws=exceptions.ProviderError,
        )
        return await resp.json()

    async def _fetch_dir_listing_next_page(self, next_url: str) -> dict:
        """Get the next page for more contents for the folder.

        :param next_url: the URL to get the next page of the folder's contents
        :return: a dict whose ``['values']`` contains a list of the folder's partial contents
        """
        resp = await self.make_request(
            'GET',
            next_url,
            expects=(200,),
            throws=exceptions.ProviderError,
        )
        return await resp.json()
