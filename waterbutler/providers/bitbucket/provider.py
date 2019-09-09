import logging
from typing import Tuple
from urllib.parse import urlencode

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
    RESP_PAGE_LEN = pd_settings.RESP_PAGE_LEN

    def __init__(self, auth, credentials, settings, **kwargs):
        super().__init__(auth, credentials, settings, **kwargs)
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

        # _fetch_dir_listing() will tell us the commit sha used to look up the listing
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

        Quirks and Tricks about BB API 2.0, compared to 1.0

        1) It no longer returns the history before a file was deleted.
        2) It no longer provides the branch information for a commit.
        3) ``revision`` is a substring (first 12 chars) of the commit hash
        4) ``raw_node`` is the commit hash
        5) There is only one timestamp ``date``: "2019-04-25T11:58:24+00:00"
        6) The response is paginated with a default page size of 50 items, which can be changed by
           setting the ``pagelen`` query param.
        7) The default response does not contain detailed commit metadata such as author and date.
           Use ``values.commit.author`` and ``values.commit.date`` in the ``fields`` query param.

        API Doc: https://developer.atlassian.com/bitbucket/api/2/reference/resource/repositories/%7Busername%7D/%7Brepo_slug%7D/filehistory/%7Bnode%7D/%7Bpath%7D
        """
        revisions = await self._fetch_commit_history_by_path(path)
        valid_revisions = []

        for revision in revisions:
            # This check may not be necessary.  It seems that every item is of type 'commit_file'.
            if revision['type'] != 'commit_file':
                continue
            data = {
                'revision': revision['commit']['hash'][:12],
                'size': revision['size'],
                'path': revision['path'],
                'raw_node': revision['commit']['hash'],
                'raw_author': revision['commit']['author']['raw'],
                'branch': None,
                'timestamp': revision['commit']['date'],
                'utctimestamp': revision['commit']['date']
            }
            valid_revisions.append(data)
        return [BitbucketRevisionMetadata(item) for item in valid_revisions]

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

    def _build_v2_repo_url(self, *segments, **query):
        segments = ('2.0', 'repositories', self.owner, self.repo) + segments
        return self.build_url(*segments, **query)

    async def _metadata_file(self, path: BitbucketPath, **kwargs):
        """Fetch the metadata for a single file

        Quirks: BB API 2.0 no longer returns file details such as the time when the file is created
                and when the file is last modified.  WB must make an extra request to fetch the file
                history which is a list of commits.  The ``data`` field of the first and and last
                items are used respectively to set time created and time last modified.

        :param path: the BitbucketPath object of the file of which the metadata should be retrieved
        :return: a BitbucketFileMetadata object
        """
        file_meta = await self._fetch_path_metadata(path)
        commit_history = await self._fetch_commit_history_by_url(
            file_meta['links']['history']['href']
        )
        data = {
            'size': file_meta['size'],
            'path': file_meta['path'],
            'revision': commit_history[0]['commit']['hash'][:12],
            'timestamp': commit_history[0]['commit']['date'],
            'created_utc': commit_history[-1]['commit']['date'],
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
        #        value of ``node`` can still be obtained from the commit hash of which the first 12
        #        chars turn out to be the value we need.
        dir_commit_sha = dir_meta['commit']['hash'][:12]
        # Quirk: the ``path`` attribute in folder metadata no longer has an trailing slash in BB API
        #        2.0.  To keep ``bitbucket_path_to_name()`` intact, a trailing slash is added.
        dir_path = '{}/'.format(dir_meta['path'])

        # Fetch content list
        dir_list = await self._fetch_dir_listing(folder)

        # Set the commit hash
        if folder.commit_sha is None:
            folder.set_commit_sha(dir_commit_sha)

        # Build the metadata to return
        # Quirks:
        # 1) BB API 2.0 treats both files and folders the same way.``path`` for both is a full or
        #    absolute path.  ``bitbucket_path_to_name()`` must be called to get the correct name.
        # 2) Both files and folders share the same list and use the same dict/json structure. Use
        #    the ``type`` field to check whether a path is a folder or not.
        # 3) ``revision`` for files is gone but can be replaced with part of the commit hash.
        #    However, it is tricky for files.  The ``commit`` field of each file item in the
        #    returned content list is the latest branch commit.  In order to obtain the correct
        #    time when the file was last modified, WB needs to fetch the file history.  This adds
        #    lots of requests and significantly hits performance due to folder listing being called
        #    very frequently.  The decision is to remove them.
        # 4) Similar to ``revision``, ``timestamp``, and ``created_utc`` are removed.
        ret = []
        for value in dir_list:
            if value['type'] == 'commit_file':
                name = self.bitbucket_path_to_name(value['path'], dir_path)
                # TODO: existing issue - find out why timestamp doesn't show up on the files page
                item = {
                    'size': value['size'],
                    'path': value['path'],
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
            '{}/?{}'.format(self._build_v2_repo_url(), urlencode({'fields': 'mainbranch.name'})),
            expects=(200,),
            throws=exceptions.ProviderError
        )
        return (await resp.json())['mainbranch']['name']

    async def _fetch_branch_commit_sha(self, branch_name: str) -> str:
        """Fetch the commit sha (a.k.a node) for a branch.

        API Doc: https://developer.atlassian.com/bitbucket/api/2/reference/resource/repositories/%7Busername%7D/%7Brepo_slug%7D/refs/branches/%7Bname%7D

        :param branch_name: the name of the branch
        :return: the commit sha of the branch
        """

        query_params = {'fields': 'target.hash'}
        branch_url = '{}?{}'.format(self._build_v2_repo_url('refs', 'branches', branch_name),
                                    urlencode(query_params))
        resp = await self.make_request(
            'GET',
            branch_url,
            expects=(200,),
            throws=exceptions.ProviderError,
        )
        return (await resp.json())['target']['hash']

    async def _fetch_path_metadata(self, path: BitbucketPath) -> dict:
        """Get the metadata for folder and file itself.

        Bitbucket API 2.0 provides an easy way to fetch metadata for files and folders by simply
        appending ``?format=meta`` to the path endpoint.

        Quirk 1: This new feature no longer returns several WB required attributes out of the box:
        ``node`` and ``path`` for folder, ``revision``, ``timestamp`` and ``created_utc`` for file.

        1) The ``path`` for folders no longer has an ending slash.
        2) The ``node`` for folders and ``revision`` for files are gone.  They have always been the
           first 12 chars of the commit hash in both 1.0 and 2.0.
        3) ``timestamp`` and ``created_utc`` for files are gone and must be obtained using the file
           history endpoint indicated by ``links.history.href``. See ``_metadata_file()`` and
           ``_fetch_commit_history_by_url()`` for details.

        Quirk 2:

        This PATH endpoint ``/2.0/repositories/{username}/{repo_slug}/src/{node}/{path}`` returns
        HTTP 404 if the ``node`` segment is a branch of which the name contains a slash.  This is
        a either a limitation or a bug on several BB API 2.0 endpoints.  It has nothing to do with
        encoding.  More specifically, neither encoding / with %2F nor enclosing ``node`` with curly
        braces %7B%7D works.  Here is the closest reference to the issue we can find as of May 2019:
        https://bitbucket.org/site/master/issues/9969/get-commit-revision-api-does-not-accept.  The
        fix is simple, just make an extra request to fetch the commit sha of the branch.  See
        ``_fetch_branch_commit_sha()`` for details.  In addition, this will happen on all branches,
        no matter if the name contains a slash or not.

        API Doc: https://developer.atlassian.com/bitbucket/api/2/reference/resource/repositories/%7Busername%7D/%7Brepo_slug%7D/src/%7Bnode%7D/%7Bpath%7D

        :param path: the file or folder of which the metadata is requested
        :return: the file metadata dict
        """
        query_params = {
            'format': 'meta',
            'fields': 'commit.hash,commit.date,path,size,links.history.href'
        }
        if not path.commit_sha:
            path.set_commit_sha(await self._fetch_branch_commit_sha(path.branch_name))
        path_meta_url = self._build_v2_repo_url('src', path.ref, *path.path_tuple())
        resp = await self.make_request(
            'GET',
            '{}/?{}'.format(path_meta_url, urlencode(query_params)),
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
        query_params = {
            'pagelen': self.RESP_PAGE_LEN,
            'fields': 'values.path,values.size,values.type,next',
        }
        if not folder.commit_sha:
            folder.set_commit_sha(await self._fetch_branch_commit_sha(folder.branch_name))
        next_url = '{}/?{}'.format(self._build_v2_repo_url('src', folder.ref, *folder.path_tuple()),
                                   urlencode(query_params))
        dir_list = []  # type: ignore
        while next_url:
            resp = await self.make_request(
                'GET',
                next_url,
                expects=(200,),
                throws=exceptions.ProviderError,
            )
            content = await resp.json()
            next_url = content.get('next', None)
            dir_list.extend(content['values'])
        return dir_list

    async def _fetch_commit_history_by_path(self, path: BitbucketPath) -> list:
        if not path.commit_sha:
            path.set_commit_sha(await self._fetch_branch_commit_sha(path.branch_name))
        return await self._fetch_commit_history_by_url(
            self._build_v2_repo_url('filehistory', path.ref, path.path)
        )

    async def _fetch_commit_history_by_url(self, history_url: str) -> list:
        """Get the entire commit history for a file given the history endpoint url.

        :param history_url: the dedicated file history url for the file
        :return: a list of commit metadata objects from newest to oldest
        """
        query_params = {
            'pagelen': self.RESP_PAGE_LEN,
            'fields': ('values.commit.hash,values.commit.date,values.commit.author.raw,'
                       'values.size,values.path,values.type,next'),
        }

        next_url = '{}?{}'.format(history_url, urlencode(query_params))
        commit_history = []  # type: ignore
        while next_url:
            resp = await self.make_request(
                'GET',
                next_url,
                expects=(200,),
                throws=exceptions.ProviderError,
            )
            content = await resp.json()
            next_url = content.get('next', None)
            commit_history.extend(content['values'])

        return commit_history
