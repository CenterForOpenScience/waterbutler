import logging
from typing import Tuple

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

        # Cache parent directory listing (a WB API V1 feature)
        # Note: Property ``_parent_dir`` has been re-structured for Bitbucket API 2.0.  Please refer
        #       to ``_fetch_path_metadata()`` and ``_fetch_dir_listing()`` for detailed information.
        self._parent_dir = {
            'metadata': await self._fetch_path_metadata(path_obj.parent),
            'contents': await self._fetch_dir_listing(path_obj.parent)
        }

        # Tweak dir_commit_sha and dir_path for Bitbucket API 2.0
        parent_dir_commit_sha = self._parent_dir['metadata']['commit']['hash'][:12]
        parent_dir_path = '{}/'.format(self._parent_dir['metadata']['path'])

        # Check file or folder existence
        path_obj_type = 'commit_directory' if path_obj.is_dir else 'commit_file'
        if path_obj.name not in [
                self.bitbucket_path_to_name(x['path'], parent_dir_path)
                for x in self._parent_dir['contents'] if x['type'] == path_obj_type
        ]:
            raise exceptions.NotFoundError(str(path))

        # _fetch_dir_listing will tell us the commit sha used to look up the listing
        # if not set in path_obj or if the lookup sha is shorter than the returned sha, update it
        if not commit_sha or (len(commit_sha) < len(parent_dir_commit_sha)):
            path_obj.set_commit_sha(parent_dir_commit_sha)

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

        In BB API 2.0, the ``repo/username/repo_slug/src/node/path`` endpoint is used for download.

        Please note that same endpoint has several different usages / behaviors depending on the
        type of the path and the query params.

        1) File download: type is file, no query param``format=meta``
        2) File metadata: type is file, with ``format=meta`` as query param
        3) Folder contents: type is folder, no query param``format=meta``
        4) Folder metadata: type is folder, with ``format=meta`` as query param

        API Doc: https://developer.atlassian.com/bitbucket/api/2/reference/resource/repositories/%7Busername%7D/%7Brepo_slug%7D/src/%7Bnode%7D/%7Bpath%7D

        :param path: the BitbucketPath object of the file to be downloaded
        :param range: the range header
        """
        metadata = await self.metadata(path)
        logger.debug('requested-range:: {}'.format(range))
        resp = await self.make_request(
            'GET',
            self._build_v2_repo_url('src', path.commit_sha, *path.path_tuple()),
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

    async def _metadata_file(self, path: BitbucketPath, **kwargs):
        """Fetch the metadata for a single file

        :param path: the BitbucketPath object of the file of which the metadata should be retrieved
        :return: a BitbucketFileMetadata object
        """
        file_meta = await self._fetch_path_metadata(path)
        # TODO: Find alternatives for timestamp
        data = {
            'revision': file_meta['commit']['hash'][:12],
            'size': file_meta['size'],
            'path': file_meta['path'],
            'timestamp': None,
            'utctimestamp': None
        }
        return BitbucketFileMetadata(data, path, owner=self.owner, repo=self.repo)

    async def _metadata_folder(self, folder: BitbucketPath, **kwargs) -> list:
        """Get a list of the folder contents, each item of which is a BitbucketPath object.

        :param folder: the folder of which the contents should be listed
        :return: a list of BitbucketFileMetadata and BitbucketFolderMetadata objects
        """

        # Fetch metadata itself
        dir_meta = await self._fetch_path_metadata(folder)
        # Quirk: ``node`` attribute is no longer available for folder metadata in BB API 1.0.  The
        #        value of ``node`` can still be extracted from the commit hash of which the first 12
        #        chars turn out to be the one we need.
        dir_commit_sha = dir_meta['commit']['hash'][:12]
        # Quirk: the ``path`` attribute in folder metadata no longer has an ending slash in BB API
        #        2.0.  To keep ``bitbucket_path_to_name()`` intact, the slash is added to the end.
        dir_path = '{}/'.format(dir_meta['path'])

        # Fetch content list
        dir_list = await self._fetch_dir_listing(folder)

        # Set the commit hash
        if folder.commit_sha is None:
            folder.set_commit_sha(dir_commit_sha)

        # Build the metadata to return
        # Quirk: BB API 2.0 treats both files and folders the same way.
        #        1) ``path`` for both is a full/absolute path.  ``bitbucket_path_to_name()`` must be
        #           called to get the correct name.
        #        2) Both share the same list and use the same dict/json structure. Use the ``type``
        #           attribute to check if an item is a folder or not.
        #        3) Similar to ``node`` for folders, ``revision`` is gone but can be replaced with
        #           part of the commit hash.  However, it is a little tricky for files.  In order to
        #           obtain the correct hash value, WB needs to find the last commit by using the
        #           file history URL.  See ``_fetch_last_commit()`` for more info.
        #        4) ``timestamp`` and ``utctimestamp`` are gone as well but they can be extracted
        #           from the metadata of the last commit.  After obtaining the file history list as
        #           mentioned in 3), WB needs to make an extra request to fetch this metadata with
        #           the commit URL.  See ``_fetch_commit_date()`` for more info.
        ret = []
        for value in dir_list:
            if value['type'] == 'commit_file':
                name = self.bitbucket_path_to_name(value['path'], dir_path)
                file_history_url = value['links']['history']['href']
                # TODO: use a map to reduce duplicated requests for the same commit
                commit_hash, commit_date = await self._fetch_last_commit(file_history_url)
                # TODO: find out why timestamp doesn't show up on the files page
                item = {
                    'revision': commit_hash[:12],
                    'size': value['size'],
                    'path': value['path'],
                    'utctimestamp': commit_date,
                    'timestamp': commit_date
                }
                ret.append(BitbucketFileMetadata(  # type: ignore
                    item,
                    folder.child(name, folder=False),
                    owner=self.owner,
                    repo=self.repo,
                ))
            if value['type'] == 'commit_directory':
                name = self.bitbucket_path_to_name(value['path'], dir_path)
                ret.append(BitbucketFolderMetadata(  # type: ignore
                    {'name': name},
                    folder.child(name, folder=True),
                    owner=self.owner,
                    repo=self.repo,
                ))

        return ret

    async def _fetch_default_branch(self) -> str:
        """Get the name of the default branch of the attached repository.

        In Bitbucket, the default branch is called "main branch".  With BB API 2.0, the dedicated
        endpoint for fetching the main branch is gone.  Fortunately, this piece of information is
        still available where ``mainbranch`` is now a field from the repository endpoint.

        API Doc: https://developer.atlassian.com/bitbucket/api/2/reference/resource/repositories/%7Busername%7D/%7Brepo_slug%7D

        :return: the name of the attached repo's default branch.
        """
        resp = await self.make_request(
            'GET',
            self._build_v2_repo_url(),
            expects=(200,),
            throws=exceptions.ProviderError
        )
        return (await resp.json())['mainbranch']['name']

    async def _fetch_path_metadata(self, path: BitbucketPath) -> dict:
        """Get the metadata for folder and file itself.

        Bitbucket API 2.0 provides an easy way to fetch metadata for files and folders by simply
        appending ``?format=meta`` to the endpoint.  However, this new feature no longer provides
        several required attributes correctly: ``node`` and ``path`` for folder, ``revision`` and
        ``timestamp`` for file.

        1) The ``path`` attribute for folders no longer has an ending slash.
        2) The ``node`` and ``revision`` attributes are gone.  ``node`` can be extracted from the
           first 12 chars of the commit hash: ``resp_dict['commit']['hash'][:12]``.  ``revision``
           is obtained alternatively using the commit history when building the folder contents to
           avoid an extra file metadata call in ``_metadata_folder()``.
        3) ``timestamp`` and ``utctimestamp`` are gone.  A new attribute ``date`` can be extracted
           from the metadata of the last commit.

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

    async def _fetch_dir_listing(self, folder: BitbucketPath) -> list:
        """Get a list of the folder's full contents (upto the max limit setting if there is one).

        Bitbucket API 2.0 refactored the response structure for listing folder contents.

        1) The response is paginated.  If ``resp_dict`` contains the key ``next``, the contents are
           partial.  The caller must use the URL provided by ``dict['next']`` to fetch the next page
           after this method returns.

        2) The response no longer provides the metadata about the folder itself.  In order to obtain
           the ``node`` and ``path`` attributes, please use  ``_fetch_path_metadata()`` instead.

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

    async def _fetch_last_commit(self, file_history_url: str) -> Tuple:
        """Get the last commit hash and date.

        :param file_history_url: the dedicated file history url for the file
        :return: a tuple of the last commit hash and date
        """
        resp = await self.make_request(
            'GET',
            file_history_url,
            expects=(200,),
            throws=exceptions.ProviderError,
        )
        resp_dict = await resp.json()
        last_commit_hash = resp_dict['values'][0]['commit']['hash']
        last_commit_url = resp_dict['values'][0]['commit']['links']['self']['href']
        last_commit_date = await self._fetch_commit_date(last_commit_url)
        return last_commit_hash, last_commit_date

    async def _fetch_commit_date(self, commit_url: str) -> dict:
        """Get the date when the commit is made.

        :param commit_url: the dedicated metadata URL for the commit
        :return: a UTC timestamp string which looks like "2019-04-25T11:58:30+00:00"
        """
        resp = await self.make_request(
            'GET',
            commit_url,
            expects=(200,),
            throws=exceptions.ProviderError,
        )
        resp_dict = await resp.json()
        return resp_dict['date']
