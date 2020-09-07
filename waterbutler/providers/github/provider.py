import copy
import json
import time
import uuid
import asyncio
import hashlib
import logging
from typing import Tuple
from http import HTTPStatus

import furl
from aiohttp.client import ClientResponse

from waterbutler.providers.github.path import GitHubPath
from waterbutler.core import streams, provider, exceptions
from waterbutler.providers.github import settings as pd_settings
from waterbutler.providers.github.metadata import (GitHubRevision,
                                                   GitHubFileTreeMetadata,
                                                   GitHubFolderTreeMetadata,
                                                   GitHubFileContentMetadata,
                                                   GitHubFolderContentMetadata, )
from waterbutler.providers.github.exceptions import (GitHubUnsupportedRepoError,
                                                     GitHubRateLimitExceededError, )

logger = logging.getLogger(__name__)


GIT_EMPTY_SHA = '4b825dc642cb6eb9a060e54bf8d69288fbee4904'


class GitHubProvider(provider.BaseProvider):
    """Provider for GitHub repositories.

    **On paths:**  WB and GH use slightly different default conventions for their paths, so we
    often have to munge our WB paths before comparison. Here is a quick overview::

        WB (dirs):  wb_dir.path == 'foo/bar/'     str(wb_dir) == '/foo/bar/'
        WB (file):  wb_file.path = 'foo/bar.txt'  str(wb_file) == '/foo/bar.txt'
        GH (dir):   'foo/bar'
        GH (file):  'foo/bar.txt'

    API docs: https://developer.github.com/v3/

    Quirks:

    * git doesn't have a concept of empty folders, so this provider creates 0-byte ``.gitkeep``
      files in the requested folder.

    * The ``contents`` endpoint cannot be used to fetch metadata reliably for all files. Requesting
      a file that is larger than 1Mb will result in a error response directing you to the ``blob``
      endpoint.  A recursive tree fetch may be used instead.

    * The tree endpoint truncates results after a large number of files.  It does not provide a way
      to page through the tree.  Since move, copy, and folder delete operations rely on whole-tree
      replacement, they cannot be reliably supported for large repos.  Attempting to use them will
      throw a 501 Not Implemented error.

    * GitHub doesn't respect Range header on downloads

    .. _rate-limiting:

    **Rate limiting**

    GitHub enforces rate-limits to avoid being overwhelmed with requests.  This limit is currently
    5,000 requests per hour per authenticated user.  Under normal usage patterns, the only WB
    actions likely to encounter this are large recursive move/copy actions.  For these actions, WB
    reserves the right to run the operation in a background task if it cannot complete them within a
    given timeframe.  The GitHub provider will add artifical delays between requests to try to
    ensure that a long-running request does not exhaust the rate-limits.

    .. _requests-and-tokens:

    *Requests and tokens*

    This provider uses the concept of requests and tokens to help determine how fast to issue
    requests.  Requests are discrete and correspond to the number of requests allowed by GitHub
    within a given time period.  Tokens are an artificial concept but are approximately equivalent
    to requests.  IOW, one token equals one request.  The difference is that the number of tokens
    is permitted to be fractional.  Every *n* seconds, the provider will add tokens to the pool.
    Once the number of tokens exceeds one, a single request may be made.  One token is subtracted,
    and the provider may continue to accumulate fractional tokens.

    *Reserves*

    GitHub sets rate-limits based off the authenticated user making the request.  Since a user may
    have multiple operations happening at one time (for example, two simultaneous registrations),
    the provider attempts to *reserve* a portion of requests for other processes to use.  When the
    number of requests remaining before the limit resets is less than the reserve, the provider
    sets the allowed requests rate to a minimum value.  This minimum value will cause some requests
    to wait a long time before being issued, but will hopefully allow long-running processes to
    complete successfully.
    """

    NAME = 'github'
    BASE_URL = pd_settings.BASE_URL
    VIEW_URL = pd_settings.VIEW_URL

    # Load settings for GitHub rate limiting
    RL_TOKEN_ADD_DELAY = pd_settings.RL_TOKEN_ADD_DELAY
    RL_MAX_AVAILABLE_TOKENS = pd_settings.RL_MAX_AVAILABLE_TOKENS
    RL_RESERVE_RATIO = pd_settings.RL_RESERVE_RATIO
    RL_RESERVE_BASE = pd_settings.RL_RESERVE_BASE
    RL_MIN_REQ_RATE = pd_settings.RL_MIN_REQ_RATE

    def __init__(self, auth, credentials, settings, **kwargs):

        super().__init__(auth, credentials, settings, **kwargs)
        self.name = self.auth.get('name', None)
        self.email = self.auth.get('email', None)
        self.token = self.credentials['token']
        self.owner = self.settings['owner']
        self.repo = self.settings['repo']
        self.metrics.add('repo', {'repo': self.repo, 'owner': self.owner})

        # debugging parameters
        self._my_id = uuid.uuid4()
        self._request_count = 0

        # `.rl_available_tokens` determines if a request should wait or proceed. Each provider
        # instance starts with a full bag (`.RL_MAX_AVAILABLE_TOKENS`) of tokens.
        self.rl_available_tokens = self.RL_MAX_AVAILABLE_TOKENS

        # `.rl_remaining` denotes the number of requests left before reset, which is the value from
        # GitHub API response header "X-RateLimiting-Remaining".
        self.rl_remaining = 0

        # Reserve a portion of the total number of remaining requests. Calculated after receiving
        # response from GH
        self.rl_reserved = 0

        # `.rl_reset` denotes the time when the limit will be reset, which is the value from GitHub
        # API response header "X-RateLimiting-Reset".
        self.rl_reset = 0

    async def make_request(self, method: str, url: str, *args, **kwargs) -> ClientResponse:
        """Wrap the parent `make_request()` to handle GH rate limiting.  Only requests handled by
        WB Celery are affected.

        1. For both Celery and non-Celery requests: intercept HTTP 403 Forbidden response.  Throw a
        dedicated ``GitHubRateLimitExceededError`` if it is caused by rate-limiting. Re-throw other
        403s as they are.

        2. Only for Celery requests: each request must wait until there are enough available tokens
        to before continue.  The tokens are replenished based several factors including: time since
        last update, number of requests remaining, time until limit is reset, etc.
        """

        self._request_count += 1

        logger.debug('P({}):{}: '.format(self._my_id, self._request_count))
        logger.debug('P({}):{}:make_request: begin!'.format(self._my_id, self._request_count))

        # Only update `expects` when it exists in the original request
        expects = kwargs.get('expects', None)
        if expects:
            kwargs.update({'expects': expects + (int(HTTPStatus.FORBIDDEN), )})

        # If not a celery task, default to regular behavior (but inform about rate limits)
        if not self.is_celery_task:
            logger.debug('P({}):{}:make_request: NOT a celery task, bypassing '
                         'limits'.format(self._my_id, self._request_count))
            resp = await super().make_request(method, url, **kwargs)

            if resp.status == HTTPStatus.FORBIDDEN:
                # Must not release the response here. It needs to be and will be released or
                # consumed by `_rl_handle_forbidden_error()` and `exception_from_response()`.
                raise await self._rl_handle_forbidden_error(resp, **kwargs)

            logger.debug('P({}):{}:make_request: done successfully!'.format(self._my_id,
                                                                            self._request_count))
            return resp

        logger.debug('P({}):{}:make_request: IS a celery task, start token '
                     'verification'.format(self._my_id, self._request_count))
        logger.debug('P({}):{}:make_request: init state: tokens({}) '
                     'remaining({}) reserved({}) reset({})'.format(self._my_id,
                                                                   self._request_count,
                                                                   self.rl_available_tokens,
                                                                   self.rl_remaining,
                                                                   self.rl_reserved,
                                                                   self.rl_reset))

        await self._rl_check_available_tokens()
        resp = await super().make_request(method, url, *args, **kwargs)
        self.rl_remaining = int(resp.headers['X-RateLimit-Remaining'])
        self.rl_reset = int(resp.headers['X-RateLimit-Reset'])

        # GitHub's rate limiting is per authenticated user. It is entirely probable that users are
        # making additional requests that count against the quota when the copy/move is in process.
        #
        # Reserve a portion (20% by default) of the total number of remaining requests plus a base
        # number (100 by default). This reservation is critical for preventing the current provider
        # instance from running out of tokens when additional calls are made for the same user on
        # OSF. In addition, this should support at least 3 concurrent large move/copy actions.
        #
        self.rl_reserved = self.rl_remaining * self.RL_RESERVE_RATIO + self.RL_RESERVE_BASE

        logger.debug('P({}):{}:make_request: final state: tokens({}) '
                     'remaining({}) reserved({}) reset({})'.format(self._my_id,
                                                                   self._request_count,
                                                                   self.rl_available_tokens,
                                                                   self.rl_remaining,
                                                                   self.rl_reserved,
                                                                   self.rl_reset))

        # Raise error if rate limit cap is hit even after WB's balancing effort. This can happen if
        # users try to copy/register multiple repos with many files/folders at the same time.
        if resp.status == HTTPStatus.FORBIDDEN:
            # Must not release the response here. It needs to be and will be released or consumed by
            # `_rl_handle_forbidden_error()` and `exception_from_response()`.
            raise await self._rl_handle_forbidden_error(resp, **kwargs)

        logger.debug('P({}):{}:make_request: done successfully!'.format(self._my_id,
                                                                        self._request_count))
        return resp

    async def validate_v1_path(self, path, **kwargs):
        """Validate the path part of the request url, asserting that the path exists, and
        determining the branch or commit implied by the request.

        **Identifying a branch or commit.**  Unfortunately, we've used a lot of different query
        parameters over the life of WB to specify the target ref.  We've also been inconsistent
        about whether each query parameter represents a branch name or a commit sha.  We don't want
        to break any parameters that are still being used in the wild.  See the documentation for
        the `_interpret_query_parameters` method for a full explanation of the supported parameters.

        Additional supported kwargs:

        * ``fileSha``: a blob SHA, used to identify a particular version of a file.

        """
        if not getattr(self, '_repo', None):
            self._repo = await self._fetch_repo()
            self.default_branch = self._repo['default_branch']

        ref, ref_type, ref_from = self._interpret_query_parameters(**kwargs)
        self.metrics.add('branch_ref_from', ref_from)
        self.metrics.add('ref_type', ref_type)

        if path == '/':
            return GitHubPath(path, _ids=[(ref, '')])

        tree_sha = None
        if ref_type == 'branch_name':
            branch_data = await self._fetch_branch(ref)
            tree_sha = branch_data['commit']['commit']['tree']['sha']
        else:
            commit_data = await self._fetch_commit(ref)
            tree_sha = commit_data['tree']['sha']

        # throws Not Found if path not in tree
        await self._search_tree_for_path(path, tree_sha)

        gh_path = GitHubPath(path)
        for part in gh_path.parts:
            part._id = (ref, None)

        # TODO Validate that filesha is a valid sha
        gh_path.parts[-1]._id = (ref, kwargs.get('fileSha'))
        self.metrics.add('file_sha_given', True if kwargs.get('fileSha') else False)

        return gh_path

    async def validate_path(self, path, **kwargs):
        """See ``validate_v1_path`` docstring for details on supported query parameters."""
        if not getattr(self, '_repo', None):
            self._repo = await self._fetch_repo()
            self.default_branch = self._repo['default_branch']

        ref, ref_type, ref_from = self._interpret_query_parameters(**kwargs)
        self.metrics.add('branch_ref_from', ref_from)
        self.metrics.add('ref_type', ref_type)

        gh_path = GitHubPath(path)
        for part in gh_path.parts:
            part._id = (ref, None)

        # TODO Validate that filesha is a valid sha
        gh_path.parts[-1]._id = (ref, kwargs.get('fileSha'))
        self.metrics.add('file_sha_given', True if kwargs.get('fileSha') else False)

        return gh_path

    async def revalidate_path(self, base, path, folder=False):
        return base.child(path, _id=((base.branch_ref, None)), folder=folder)

    def path_from_metadata(self, parent_path, metadata):
        """Build a path from a parent path and a metadata object.  Will correctly set the _id
        Used for building zip archives."""
        file_sha = metadata.extra.get('fileSha', None)
        return parent_path.child(metadata.name, _id=(metadata.ref, file_sha),
                                 folder=metadata.is_folder, )

    def can_duplicate_names(self):
        return False

    @property
    def default_headers(self):
        return {'Authorization': 'token {}'.format(self.token)}

    @property
    def committer(self):
        return {
            'name': self.name,
            'email': self.email,
        }

    def build_repo_url(self, *segments, **query):
        segments = ('repos', self.owner, self.repo) + segments
        return self.build_url(*segments, **query)

    def can_intra_move(self, other, path=None):
        return self.can_intra_copy(other, path=path)

    def can_intra_copy(self, other, path=None):
        return (
            type(self) == type(other) and
            self.repo == other.repo and
            self.owner == other.owner
        )

    # do these need async?
    async def intra_copy(self, dest_provider, src_path, dest_path):
        return (await self._do_intra_move_or_copy(src_path, dest_path, True))

    async def intra_move(self, dest_provider, src_path, dest_path):
        return (await self._do_intra_move_or_copy(src_path, dest_path, False))

    async def download(self, path: GitHubPath, range: Tuple[int, int]=None,  # type: ignore
                       **kwargs) -> streams.ResponseStreamReader:
        """Get the stream to the specified file on github
        :param GitHubPath path: The path to the file on github
        :param range: The range header
        :param dict kwargs: Additional kwargs are ignored
        """

        data = await self.metadata(path)
        file_sha = path.file_sha or data.extra['fileSha']

        logger.debug('requested-range:: {}'.format(range))
        resp = await self.make_request(
            'GET',
            self.build_repo_url('git', 'blobs', file_sha),
            headers={'Accept': 'application/vnd.github.v3.raw'},
            range=range,
            expects=(200, ),
            throws=exceptions.DownloadError,
        )

        return streams.ResponseStreamReader(resp, size=data.size)

    async def upload(self, stream, path, message=None, branch=None, **kwargs):
        assert self.name is not None
        assert self.email is not None

        try:
            exists = await self.exists(path)
        except exceptions.ProviderError as e:
            if e.data.get('message') == 'Git Repository is empty.':
                self.metrics.add('upload.initialized_empty_repo', True)
                exists = False
                resp = await self.make_request(
                    'PUT',
                    self.build_repo_url('contents', '.gitkeep'),
                    data=json.dumps({
                        'content': '',
                        'path': '.gitkeep',
                        'committer': self.committer,
                        'branch': path.branch_ref,
                        'message': 'Initial commit'
                    }),
                    expects=(201,),
                    throws=exceptions.CreateFolderError
                )
                data = await resp.json()
                latest_sha = data['commit']['sha']
        else:
            latest_sha = await self._get_latest_sha(ref=path.branch_ref)

        blob = await self._create_blob(stream)
        tree = await self._create_tree({
            'base_tree': latest_sha,
            'tree': [{
                'path': path.path,
                'mode': '100644',
                'type': 'blob',
                'sha': blob['sha']
            }]
        })

        if exists and await self._is_blob_in_tree(blob, path):  # Avoids empty commits
            return GitHubFileTreeMetadata({
                'path': path.path,
                'sha': blob['sha'],
                'size': stream.size,
            }, ref=path.branch_ref), not exists

        commit = await self._create_commit({
            'tree': tree['sha'],
            'parents': [latest_sha],
            'committer': self.committer,
            'message': message or (pd_settings.UPDATE_FILE_MESSAGE
                                   if exists else pd_settings.UPLOAD_FILE_MESSAGE),
        })

        # Doesn't return anything useful
        await self._update_ref(commit['sha'], ref=path.branch_ref)

        # You're hacky
        return GitHubFileTreeMetadata({
            'path': path.path,
            'sha': blob['sha'],
            'size': stream.size,
        }, commit=commit, ref=path.branch_ref), not exists

    async def delete(self, path, sha=None, message=None, branch=None,
               confirm_delete=0, **kwargs):
        """Delete file, folder, or provider root contents

        :param GitHubPath path: GitHubPath path object for file, folder, or root
        :param str sha: SHA-1 checksum of file/folder object
        :param str message: Commit message
        :param str branch: Repository branch
        :param int confirm_delete: Must be 1 to confirm root folder delete
        """
        assert self.name is not None
        assert self.email is not None

        if path.is_root:
            if confirm_delete == 1:
                await self._delete_root_folder_contents(path)
            else:
                raise exceptions.DeleteError(
                    'confirm_delete=1 is required for deleting root provider folder',
                    code=400,
                )
        elif path.is_dir:
            await self._delete_folder(path, message, **kwargs)
        else:
            await self._delete_file(path, message, **kwargs)

    async def metadata(self, path: GitHubPath, **kwargs):  # type: ignore
        """Get Metadata about the requested file or folder
        :param GitHubPath path: The path to a file or folder
        :rtype dict: if file, metadata object describing the file
        :rtype list: if folder, array of metadata objects describing contents
        """
        if path.is_dir:
            return (await self._metadata_folder(path, **kwargs))
        else:
            return (await self._metadata_file(path, **kwargs))

    async def revisions(self, path, **kwargs):
        resp = await self.make_request(
            'GET',
            self.build_repo_url('commits', path=path.path, sha=path.branch_ref),
            expects=(200, ),
            throws=exceptions.RevisionsError
        )

        return [
            GitHubRevision(item)
            for item in (await resp.json())
        ]

    async def create_folder(self, path, branch=None, message=None, **kwargs):
        GitHubPath.validate_folder(path)

        assert self.name is not None
        assert self.email is not None
        message = message or pd_settings.UPLOAD_FILE_MESSAGE

        keep_path = path.child('.gitkeep')

        data = {
            'content': '',
            'path': keep_path.path,
            'committer': self.committer,
            'branch': path.branch_ref,
            'message': message or pd_settings.UPLOAD_FILE_MESSAGE
        }

        resp = await self.make_request(
            'PUT',
            self.build_repo_url('contents', keep_path.path),
            data=json.dumps(data),
            expects=(201, 422, 409),
            throws=exceptions.CreateFolderError
        )

        data = await resp.json()

        if resp.status in (422, 409):
            if (
                    resp.status == 409 or
                    data.get('message') == 'Invalid request.\n\n"sha" wasn\'t supplied.'
            ):
                raise exceptions.FolderNamingConflict(path.name)
            raise exceptions.CreateFolderError(data, code=resp.status)

        data['content']['name'] = path.name
        data['content']['path'] = data['content']['path'].replace('.gitkeep', '')

        return GitHubFolderContentMetadata(data['content'], commit=data['commit'],
                                           ref=path.branch_ref)

    async def _delete_file(self, path, message=None, **kwargs):
        if path.file_sha:
            sha = path.file_sha
        else:
            sha = (await self.metadata(path)).extra['fileSha']

        data = {
            'sha': sha,
            'branch': path.branch_ref,
            'committer': self.committer,
            'message': message or pd_settings.DELETE_FILE_MESSAGE,
        }

        resp = await self.make_request(
            'DELETE',
            self.build_repo_url('contents', path.path),
            headers={'Content-Type': 'application/json'},
            data=json.dumps(data),
            expects=(200, ),
            throws=exceptions.DeleteError,
        )
        await resp.release()

    async def _delete_folder(self, path, message=None, **kwargs):
        message = message or pd_settings.DELETE_FOLDER_MESSAGE
        # _create_tree fails with empty tree (422 Invalid tree info), so catch it if this folder
        # is the last contents of this repository and use _delete_root_folder_contents instead.
        if path.parent.is_root:
            root_metadata = await self.metadata(path.parent)
            if len(root_metadata) == 1:
                if root_metadata[0].materialized_path == path.materialized_path:
                    await self._delete_root_folder_contents(path, message=message, **kwargs)
                    return

        branch_data = await self._fetch_branch(path.branch_ref)

        old_commit_sha = branch_data['commit']['sha']
        old_commit_tree_sha = branch_data['commit']['commit']['tree']['sha']

        # e.g. 'level1', 'level2', or ''
        tree_paths = path.parts[1:]
        trees = [{
            'target': tree_paths[0].value,
            'tree': [
                {
                    'path': item['path'],
                    'mode': item['mode'],
                    'type': item['type'],
                    'sha': item['sha'],
                }
                for item in (await self._fetch_tree(old_commit_tree_sha))['tree']
            ]
        }]

        for idx, tree_path in enumerate(tree_paths[:-1]):
            try:
                tree_sha = next(x for x in trees[-1]['tree'] if x['path'] == tree_path.value)['sha']
            except StopIteration:
                raise exceptions.MetadataError(
                    'Could not delete folder \'{0}\''.format(path),
                    code=404,
                )
            trees.append({
                'target': tree_paths[idx + 1].value,
                'tree': [
                    {
                        'path': item['path'],
                        'mode': item['mode'],
                        'type': item['type'],
                        'sha': item['sha'],
                    }
                    for item in (await self._fetch_tree(tree_sha))['tree']
                ]
            })

        # The last tree's structure is rewritten w/o the target folder, all others
        # in the hierarchy are simply updated to reflect this change.
        tree = trees.pop()

        # Delete the folder from the tree cast to list iterator over all values
        current_tree = tree['tree']
        tree['tree'] = list(filter(lambda x: x['path'] != tree['target'], tree['tree']))
        if current_tree == tree['tree']:
            raise exceptions.NotFoundError(str(path))

        tree_data = await self._create_tree({'tree': tree['tree']})
        tree_sha = tree_data['sha']

        # Update parent tree(s)
        for tree in reversed(trees):
            for item in tree['tree']:
                if item['path'] == tree['target']:
                    item['sha'] = tree_sha
                    break
            tree_data = await self._create_tree({'tree': tree['tree']})
            tree_sha = tree_data['sha']

        # Create a new commit which references our top most tree change.
        commit_resp = await self.make_request(
            'POST',
            self.build_repo_url('git', 'commits'),
            headers={'Content-Type': 'application/json'},
            data=json.dumps({
                'message': message,
                'committer': self.committer,
                'tree': tree_sha,
                'parents': [
                    old_commit_sha,
                ],
            }),
            expects=(201, ),
            throws=exceptions.DeleteError,
        )
        commit_data = await commit_resp.json()
        commit_sha = commit_data['sha']

        # Update repository reference, point to the newly created commit.
        # No need to store data, rely on expects to raise exceptions
        resp = await self.make_request(
            'PATCH',
            self.build_repo_url('git', 'refs', 'heads', path.branch_ref),
            headers={'Content-Type': 'application/json'},
            data=json.dumps({'sha': commit_sha}),
            expects=(200, ),
            throws=exceptions.DeleteError,
        )
        await resp.release()

    async def _delete_root_folder_contents(self, path, message=None, **kwargs):
        """Delete the contents of the root folder.

        :param GitHubPath path: GitHubPath path object for folder
        :param str message: Commit message
        """
        branch_data = await self._fetch_branch(path.branch_ref)
        old_commit_sha = branch_data['commit']['sha']
        tree_sha = GIT_EMPTY_SHA
        message = message or pd_settings.DELETE_FOLDER_MESSAGE
        commit_resp = await self.make_request(
            'POST',
            self.build_repo_url('git', 'commits'),
            headers={'Content-Type': 'application/json'},
            data=json.dumps({
                'message': message,
                'committer': self.committer,
                'tree': tree_sha,
                'parents': [
                    old_commit_sha,
                ],
            }),
            expects=(201, ),
            throws=exceptions.DeleteError,
        )
        commit_data = await commit_resp.json()
        commit_sha = commit_data['sha']

        # Update repository reference, point to the newly created commit.
        # No need to store data, rely on expects to raise exceptions
        await self.make_request(
            'PATCH',
            self.build_repo_url('git', 'refs', 'heads', path.branch_ref),
            headers={'Content-Type': 'application/json'},
            data=json.dumps({'sha': commit_sha}),
            expects=(200, ),
            throws=exceptions.DeleteError,
        )

    async def _fetch_branch(self, branch):
        """Fetch a branch by name

        API docs: https://developer.github.com/v3/repos/branches/#get-branch
        """

        resp = await self.make_request('GET', self.build_repo_url('branches', branch))
        if resp.status == 404:
            await resp.release()
            raise exceptions.NotFoundError('. No such branch \'{}\''.format(branch))

        return await resp.json()

    async def _fetch_contents(self, path, ref=None):
        """Get the metadata and base64-encoded contents for a file.

        API docs: https://developer.github.com/v3/repos/contents/#get-contents
        """
        url = furl.furl(self.build_repo_url('contents', path.path))
        if ref:
            url.args.update({'ref': ref})
        resp = await self.make_request(
            'GET',
            url.url,
            expects=(200, ),
            throws=exceptions.MetadataError
        )
        return await resp.json()

    async def _fetch_repo(self):
        """Get metadata about the repo.

        API docs: https://developer.github.com/v3/repos/#get
        """
        resp = await self.make_request(
            'GET',
            self.build_repo_url(),
            expects=(200, ),
            throws=exceptions.MetadataError
        )
        return await resp.json()

    async def _fetch_commit(self, commit_sha):
        """Get metadata about a specific commit.

        API docs: https://developer.github.com/v3/commits/#get
        """
        resp = await self.make_request(
            'GET',
            self.build_repo_url('git', 'commits', commit_sha),
            expects=(200, ),
            throws=exceptions.MetadataError
        )
        return await resp.json()

    async def _fetch_tree(self, sha, recursive=False):
        url = furl.furl(self.build_repo_url('git', 'trees', sha))
        if recursive:
            url.args.update({'recursive': 1})
        resp = await self.make_request(
            'GET',
            url.url,
            expects=(200, ),
            throws=exceptions.MetadataError
        )
        tree = await resp.json()

        if tree['truncated']:
            raise GitHubUnsupportedRepoError('')

        return tree

    async def _search_tree_for_path(self, path, tree_sha, recursive=True):
        """Search through the given tree for an entity matching the name and type of `path`.
        """
        tree = await self._fetch_tree(tree_sha, recursive=True)

        implicit_type = 'tree' if path.endswith('/') else 'blob'

        for entity in tree['tree']:
            if entity['path'] == path.strip('/') and entity['type'] == implicit_type:
                return entity

        raise exceptions.NotFoundError(str(path))

    async def _create_tree(self, tree):
        resp = await self.make_request(
            'POST',
            self.build_repo_url('git', 'trees'),
            headers={'Content-Type': 'application/json'},
            data=json.dumps(tree),
            expects=(201, ),
            throws=exceptions.ProviderError,
        )
        return (await resp.json())

    async def _create_commit(self, commit):
        resp = await self.make_request(
            'POST',
            self.build_repo_url('git', 'commits'),
            headers={'Content-Type': 'application/json'},
            data=json.dumps(commit),
            expects=(201, ),
            throws=exceptions.ProviderError,
        )
        return (await resp.json())

    async def _create_blob(self, stream):
        blob_stream = streams.JSONStream({
            'encoding': 'base64',
            'content': streams.Base64EncodeStream(stream),
        })

        sha1_calculator = streams.HashStreamWriter(hashlib.sha1)
        stream.add_writer('sha1', sha1_calculator)
        git_blob_header = 'blob {}\0'.format(str(stream.size))
        sha1_calculator.write(git_blob_header.encode('utf-8'))

        resp = await self.make_request(
            'POST',
            self.build_repo_url('git', 'blobs'),
            data=blob_stream,
            headers={
                'Content-Type': 'application/json',
                'Content-Length': str(blob_stream.size),
            },
            expects=(201, ),
            throws=exceptions.UploadError,
        )

        blob_metadata = await resp.json()
        if stream.writers['sha1'].hexdigest != blob_metadata['sha']:
            raise exceptions.UploadChecksumMismatchError()

        return blob_metadata

    def _web_view(self, path):
        segments = (self.owner, self.repo, 'blob', path.branch_ref, path.path)
        return provider.build_url(pd_settings.VIEW_URL, *segments)

    async def _metadata_folder(self, path, **kwargs):
        ref = path.branch_ref

        try:
            # it's cool to use the contents API here because we know path is a dir and won't hit
            # the 1mb size limit
            data = await self._fetch_contents(path, ref=ref)
        except exceptions.MetadataError as e:
            if e.data.get('message') == 'This repository is empty.':
                data = []
            else:
                raise

        if isinstance(data, dict):
            raise exceptions.MetadataError(
                'Could not retrieve folder "{0}"'.format(str(path)),
                code=404,
            )

        ret = []
        for item in data:
            if item['type'] == 'dir':
                ret.append(GitHubFolderContentMetadata(item, ref=ref))
            else:
                ret.append(GitHubFileContentMetadata(item, ref=ref, web_view=item['html_url']))

        return ret

    async def _metadata_file(self, path, **kwargs):
        resp = await self.make_request(
            'GET',
            self.build_repo_url('commits', path=path.path, sha=path.branch_ref),
            expects=(200, ),
            throws=exceptions.MetadataError,
        )

        commits = await resp.json()

        if not commits:
            raise exceptions.NotFoundError(str(path))

        latest = commits[0]
        tree = await self._fetch_tree(latest['commit']['tree']['sha'], recursive=True)

        try:
            data = next(
                x for x in tree['tree']
                if x['path'] == path.path
            )
        except StopIteration:
            raise exceptions.NotFoundError(str(path))

        return GitHubFileTreeMetadata(
            data, commit=latest['commit'], web_view=self._web_view(path),
            ref=path.branch_ref
        )

    async def _get_latest_sha(self, ref='master'):
        resp = await self.make_request(
            'GET',
            self.build_repo_url('git', 'refs', 'heads', ref),
            expects=(200, ),
            throws=exceptions.ProviderError
        )
        data = await resp.json()
        return data['object']['sha']

    async def _update_ref(self, sha, ref='master'):
        resp = await self.make_request(
            'POST',
            self.build_repo_url('git', 'refs', 'heads', ref),
            data=json.dumps({
                'sha': sha,
            }),
            expects=(200, ),
            throws=exceptions.ProviderError
        )
        return (await resp.json())

    async def _do_intra_move_or_copy(self, src_path, dest_path, is_copy):

        # ON PATHS:
        #   WB and GH use slightly different default conventions for their paths, so we often
        #   have to munge our WB paths before comparison. Here is a quick overview:
        #     WB (dirs):  wb_dir.path == 'foo/bar/'     str(wb_dir) == '/foo/bar/'
        #     WB (file):  wb_file.path = 'foo/bar.txt'  str(wb_file) == '/foo/bar.txt'
        #     GH (dir):   'foo/bar'
        #     GH (file):  'foo/bar.txt'

        src_tree, src_head = await self._get_tree_and_head(src_path.branch_ref)

        # these are the blobs to copy/move
        blobs = [
            item
            for item in src_tree['tree']
            if src_path.is_dir and item['path'].startswith(src_path.path) or
            src_path.is_file and item['path'] == src_path.path
        ]

        if len(blobs) == 0:
            raise exceptions.NotFoundError(str(src_path))

        if src_path.is_file:
            assert len(blobs) == 1, 'Found multiple targets'

        commit_msg = pd_settings.COPY_MESSAGE if is_copy else pd_settings.MOVE_MESSAGE
        commit = None

        if src_path.branch_ref == dest_path.branch_ref:
            exists = self._path_exists_in_tree(src_tree['tree'], dest_path)

            # if we're overwriting an existing dir, we must remove its blobs from the tree
            if dest_path.is_dir:
                src_tree['tree'] = self._remove_path_from_tree(src_tree['tree'], dest_path)

            # if this is a copy, duplicate and append our source blobs. The originals will be
            # updated with the new destination path.
            if is_copy:
                src_tree['tree'].extend(copy.deepcopy(blobs))

            # see, I told you they'd be overwritten
            self._reparent_blobs(blobs, src_path, dest_path)

            src_tree['tree'] = self._prune_subtrees(src_tree['tree'])

            commit = await self._commit_tree_and_advance_branch(src_tree, {'sha': src_head},
                                                                commit_msg, src_path.branch_ref)

        else:
            dest_tree, dest_head = await self._get_tree_and_head(dest_path.branch_ref)

            exists = self._path_exists_in_tree(dest_tree['tree'], dest_path)

            dest_tree['tree'] = self._remove_path_from_tree(dest_tree['tree'], dest_path)

            new_blobs = copy.deepcopy(blobs)
            self._reparent_blobs(new_blobs, src_path, dest_path)
            dest_tree['tree'].extend(new_blobs)

            dest_tree['tree'] = self._prune_subtrees(dest_tree['tree'])

            commit = await self._commit_tree_and_advance_branch(dest_tree, {'sha': dest_head},
                                                                commit_msg, dest_path.branch_ref)

            if not is_copy:
                src_tree['tree'] = self._remove_path_from_tree(src_tree['tree'], src_path)
                src_tree['tree'] = self._prune_subtrees(src_tree['tree'])
                await self._commit_tree_and_advance_branch(src_tree, {'sha': src_head},
                                                           commit_msg, src_path.branch_ref)

            blobs = new_blobs  # for the metadata

        if dest_path.is_file:
            assert len(blobs) == 1, 'Destination file should have exactly one candidate'
            return GitHubFileTreeMetadata(
                blobs[0], commit=commit, ref=dest_path.branch_ref
            ), not exists

        folder = GitHubFolderTreeMetadata({
            'path': dest_path.path.strip('/')
        }, commit=commit, ref=dest_path.branch_ref)

        folder.children = []

        for item in blobs:
            if item['path'] == dest_path.path.rstrip('/'):
                continue
            if item['type'] == 'tree':
                folder.children.append(GitHubFolderTreeMetadata(item, ref=dest_path.branch_ref))
            else:
                folder.children.append(GitHubFileTreeMetadata(item, ref=dest_path.branch_ref))

        return folder, not exists

    async def _get_blobs_and_trees(self, branch_ref):
        """This method takes a branch ref (usually the branch name) to call the github api and
        returns a flat list of a repo's blobs and trees (with no commits).

        :param str branch_ref: The reference which leads to the branch, that the blobs and trees
        are gathered from.
        :returns dict response json: This is a JSON dict with the flattened list of blobs and trees
        include in the dict.
        """

        resp = await self.make_request(
            'GET',
            self.build_repo_url('git', 'trees') + '/{}:?recursive=99999'.format(branch_ref),
            expects=(200,)
        )
        return await resp.json()

    async def _is_blob_in_tree(self, new_blob, path):
        """This method checks to see if a branch's tree already contains a blob with the same sha
        and at the path provided, basically checking if a new blob has identical path and has
        identical content to a blob already in the tree. This ensures we don't overwrite a blob if
        it serves no purpose.

        :param dict new_blob: a dict with data and metadata of the newly created blob which is not
        yet committed.
        :param GitHubPath path: The path where the newly created blob is to be committed.
        :returns: bool: True if new_blob is in the tree, False if no blob or a different blob
        exists at the path given
        """

        blob_tree = await self._get_blobs_and_trees(path.branch_ref)
        return any(new_blob['sha'] == blob['sha'] and
                   path.path == blob['path'] for blob in blob_tree['tree'])

    async def _get_tree_and_head(self, branch):
        """Fetch the head commit and tree for the given branch.

        :param str branch: The branch to fetch
        :returns dict: A GitHub tree object. Contents are under the ``tree`` key.
        :returns dict: A GitHub commit object. The SHA is under the ``sha`` key.
        """
        branch_data = await self._fetch_branch(branch)
        head = branch_data['commit']['sha']

        tree_sha = branch_data['commit']['commit']['tree']['sha']
        tree = await self._fetch_tree(tree_sha, recursive=True)

        return tree, head

    def _path_exists_in_tree(self, tree, path):
        """Search through a tree and return true if the given path is found.

        :param list tree: A list of blobs in a git tree.
        :param GitHubPath path:  The path to search for.
        :returns bool: true if ``path`` is found in ``tree``
        """
        return any(x['path'] == path.path.rstrip('/') for x in tree)

    def _remove_path_from_tree(self, tree, path):
        """Search through a tree and remove any blobs or trees that match ``path`` or are a child of
        ``path``.

        :param list tree: A list of blobs in a git tree.
        :param GitHubPath path:  The path to exclude.
        :returns list: A new list containing the filtered tree contents.
        """
        return [
            item
            for item in tree
            if (path.is_file and not item['path'] == path.path) or  # file != path
            (path.is_dir and not
             (item['path'].startswith(path.path) or  # file/folder != child of path
              (item['type'] == 'tree' and item['path'] == path.path.rstrip('/'))))  # folder != path

        ]

    def _reparent_blobs(self, blobs, src_path, dest_path):
        """Take a list of blobs and replace the source path with the dest path.

        Two caveats:

        * This method operates on the list of blobs in place. This is intentional. Anything you pass
        as the ``blobs`` arg will be mutated back in the calling scope.

        * This method assumes that the list of blobs all begin with ``src_path``, since its purpose
        is to rewite all the blobs found at or under ``src_path`` to be at or under ``dest_path``.
        If you pass it something that is not located under ``src_path``, a later part of the path
        may be updated.

        :param list blobs: A list of blobs whose paths should be updated.
        :param GitHubPath src_path:  The original path.
        :param GitHubPath dest_path:  The new path.
        :returns None: This methods returns **nothing**. It operates on the blobs in-place.
        """
        for blob in blobs:
            if blob['path'] == src_path.path.rstrip('/') and blob['type'] == 'tree':
                # Renaming the parent folder is not necessary. Tress are pruned before uploading
                # to GH.  This is only here because at somepoint someone will use it without pruning
                # and wonder why on earth the parent folder isn't renamed.
                blob['path'] = dest_path.path.rstrip('/')
            else:
                blob['path'] = blob['path'].replace(src_path.path, dest_path.path, 1)
        return

    def _prune_subtrees(self, tree):
        """Takes in a list representing a git tree and remove all the entries that are also trees.
        Only blobs should remain. GitHub infers tree structure from blob paths.  Deleting a blob
        without removing its parent tree will result in the blob *NOT* being deleted. See:
        http://www.levibotelho.com/development/commit-a-file-with-the-github-api/

        :param list tree: A list representing a git tree. May contain trees, in addition to blobs.
        :returns list: A new list containing just the blobs.
        """
        return [item for item in tree if item['type'] != 'tree']

    async def _commit_tree_and_advance_branch(self, old_tree, old_head, commit_msg, branch_ref):
        """Utilty method to bundle several commands into one.  Takes a tree, head commit, a message,
        and a branch, creates a new commit pointing to tree, then advances branch to point to the
        new commit. Basically the same thing as ``git commit -am "foo message"`` on the command
        line.  Returns the new commit.

        :param list old_tree: A list of blobs representing the new file tree.
        :param dict old_head: The commit that's the parent of the new commit. Must have 'sha' key.
        :param str commit_msg: The commit message for the new commit.
        :param str branch_ref: The branch that will be advanced to the new commit.
        :returns dict new_head: The commit object returned by GitHub.
        """
        new_tree = await self._create_tree({'tree': old_tree['tree']})

        # Create a new commit which references our top most tree change.

        if new_tree['sha'] == old_tree['sha']:  # prevents empty commits
            return None
        else:
            new_head = await self._create_commit({
                'tree': new_tree['sha'],
                'parents': [old_head['sha']],
                'committer': self.committer,
                'message': commit_msg,
            })

        # Update repository reference, point to the newly created commit.
        # No need to store data, rely on expects to raise exceptions
        await self._update_ref(new_head['sha'], ref=branch_ref)

        return new_head

    def _interpret_query_parameters(self, **kwargs) -> Tuple[str, str, str]:
        """This one hurts.

        Over the life of WB, the github provider has accepted the following parameters to identify
        the ref (commit or branch) that the path entity is to be found on: ``ref``, ``branch``,
        ``version``, ``sha``, ``revision``.  Worse, sometimes the values are commit SHAs and other
        times are branch names.  Worserer (sic), some queries contain two or three of these
        parameters at the same time.

        WB strives to maintain backcompat, so the following method tries to divine the user
        intention as much as possible, using the following heuristics:

        * If both a commit SHA and branch name are provided, the commit SHA should be used, since
          it is more specific.

        * Each parameter will be checked.  If present and not equal to the empty string, it will be
          run through the `_looks_like_sha` method to decide whether it is a SHA or branch name.

        * The order in which the query parameters are tested differs for commit SHAs and branches.
          The order for commit SHAs from highest precedence to lowest precedence is: ``ref``,
          ``version``, ``sha``, ``revision``.  ``ref`` is returned in the action links for file and
          folder metadata, and so takes precendence.  ``version`` is the most common parameter, used
          heavily in the OSF.  ``sha`` occurs occasionally, but despite the name can be either a
          commit SHA or branch name. ``revision`` does not appear to be in regular use anymore, but
          has cropped up in development in old metadata entries.

        * The order for branch names from highest priority to lowest priority is: ``branch``,
          ``ref``, ``version``, ``sha``, ``revision``. ``branch`` is used by fangorn and so far
          appears to always be a branch name.  The others follow the same priority for commit SHAs.

        * If multiple values are passed for one param, that param will be ignored and the check will
          move on to the next params.  If the only params found have multiple values, an error will
          be thrown to warn the user not to do this.

        * If none of the query parameters are present, WB will fall back to the default branch for
          the repo.

        This method also returns the type of ref it found (``commit_sha`` or ``branch``) and a
        string identifying the source of ref.  The former is used to avoid making a branch-specific
        query during validation.  The latter is used for analytics.
        """

        all_possible_params = ['ref', 'version', 'sha', 'revision', 'branch']

        # empty string values should be made None
        possible_values = {}
        for param in all_possible_params:
            possible_values[param] = kwargs.get(param, '')
            if possible_values[param] == '':
                possible_values[param] = None

        inferred_ref, ref_from = None, None
        multiple_args_seen = False

        # look for commit SHA likes ('branch' is least likely to be a sha)
        sha_priority_order = ['ref', 'version', 'sha', 'revision', 'branch']
        for param in sha_priority_order:
            v = possible_values[param]

            if isinstance(v, list):
                multiple_args_seen = True
                continue

            if v is not None and self._looks_like_sha(v):
                inferred_ref = v
                ref_from = 'query_{}'.format(param)
                break

        if inferred_ref is not None:
            return inferred_ref, 'commit_sha', ref_from  # found a SHA!

        # look for branch names ('branch' is most likely to be a branchname)
        branch_priority_order = ['branch', 'ref', 'version', 'sha', 'revision']
        for param in branch_priority_order:
            v = possible_values[param]

            if isinstance(v, list):
                multiple_args_seen = True
                continue

            if v is not None:
                inferred_ref = v
                ref_from = 'query_{}'.format(param)
                break

        if inferred_ref is None:
            if multiple_args_seen:
                raise exceptions.InvalidParameters('Multiple values provided for parameter '
                                                   '"{}". Only one ref or branch may be '
                                                   'given.'.format(ref_from))
            inferred_ref = self.default_branch
            ref_from = 'default_branch'

        return inferred_ref, 'branch_name', ref_from

    def _looks_like_sha(self, ref):
        """Returns `True` if ``ref`` could be a valid SHA (i.e. is a valid hex number).  If ``True``
        also checks to make sure ``ref`` is a valid number of characters, as GH doesn't like
        abbreviated refs.  Currently only check for 40 characters (length of a sha1-name), but a
        future git release will add support for 64-character sha256-names.

        :param str ref: the string to test
        :rtype: `bool`
        :returns: whether ``ref`` could be a valid SHA
        """
        try:
            int(ref, 16)  # is revision valid hex?
        except (TypeError, ValueError):
            return False

        # 'in' instead of '==' b/c git shas will be changing in future git release.
        return len(ref) in pd_settings.GITHUB_SHA_LENGTHS

    async def _rl_handle_forbidden_error(self, resp: ClientResponse, **kwargs) -> Exception:
        """Check if an HTTP 403 response is caused by rate limiting or not. If so, throw a special
        ``GitHubRateLimitExceededError`` with rate limiting information. Otherwise, re-throw the
        original error in the response or an ``UnhandledProviderError`` if there is none.
        """

        exc = None
        if int(resp.headers['X-RateLimit-Remaining']) == 0:
            rate_limit_reset = int(resp.headers['X-RateLimit-Reset'])
            exc = GitHubRateLimitExceededError(rate_limit_reset)
            # It is recommended to release the response here since it is no longer needed.
            resp.release()
            logger.debug('P({}):{}:_rl_handle_forbidden_error: ran out of requests, will reset '
                         'at {}'.format(self._my_id, self._request_count, rate_limit_reset))
        else:
            throws = kwargs.get('throws', exceptions.UnhandledProviderError)
            # No need to release the response here since `exception_from_response` guarantees to
            # either release (i.e. `.release()`) or consume (i.e. `.read()` or `.json()`) it.
            exc = await exceptions.exception_from_response(resp, error=throws, **kwargs)
            logger.debug('P({}):{}:_rl_handle_forbidden_error: got a non-rate-limit error. '
                         'Bailing out.'.format(self._my_id, self._request_count))

        return exc

    async def _rl_check_available_tokens(self) -> None:
        """Checks if there are any available tokens.  If so, consume one and return.  Otherwise,
        continue to add new tokens until an entire token is available.  Waits for a short period
        of time between each add request.  The wait time is defined by `.RL_TOKEN_ADD_DELAY`
        and its default is 1 second.
        """
        logger.debug('P({}):{}:token_check: starting with {} '
                     'tokens'.format(self._my_id, self._request_count, self.rl_available_tokens))

        # If no full tokens are available, wait and add fractional tokens based on current rate.
        while self.rl_available_tokens < 1:
            self._rl_add_more_tokens()
            await asyncio.sleep(self.RL_TOKEN_ADD_DELAY)

        logger.debug('P({}):{}:token_check: consuming token'.format(self._my_id,
                                                                    self._request_count))

        # Consume one token for the upcoming request.
        self.rl_available_tokens -= 1

    def _rl_add_more_tokens(self) -> None:
        """Add a number of tokens equivalent to the approximate time since this was last called and
        the current request rate.  Can add fractional tokens.  Caps the number of available tokens
        at `self.RL_MAX_AVAILABLE_TOKENS`.

        Calculate the rate at which WB should make requests.  Instead of maximizing the rate with
        ``$remaining  / $time_to_reset``, WB sets a lower one by reserving "20% of the total
        number of remaining requests plus 10 extra ones".  We want to make sure that some requests
        are set aside for regular user interactions (e.g. reloading the page on the OSF). We also
        want to handle the case where the user has initiated multiple move/copies from a
        WB-connected GitHub repo.  Both of these parameters are tweakable via the GH provider
        settings.py file.
        """

        # `rl_req_rate` denotes the number of requests allowed per second.
        rl_req_rate = None

        if self.rl_remaining < self.rl_reserved:
            # With the default setting, the number of reserved requests is greater or equal to the
            # number of remaining requests when the latter is 125 or less. Set the reference request
            # rate to the minimum value (0.01 by default) which allows only 36 requests in a hour.
            # Given that 4 > 125 / 36 > 3, at least 3 concurrent move/copy actions are supported
            # even in the worst case scenario.
            rl_req_rate = self.RL_MIN_REQ_RATE
        else:
            # Otherwise, calculate the reference request rate according to the adjusted number of
            # remaining requests and the time between now and next limit reset. If no other major
            # requests (i.e. another copy/move) are being made at the same time, this reference
            # request rate keeps increasing slightly.
            seconds_until_reset = max((self.rl_reset - time.time()), 1)
            rl_req_rate = (self.rl_remaining - self.rl_reserved) / seconds_until_reset

        logger.debug('P({}):{}:adding_tokens: current_tokens:({}) '
                     'request_rate:({})'.format(self._my_id, self._request_count,
                                                self.rl_available_tokens, rl_req_rate))

        # Add a number of tokens equal to: seconds since we last added tokens (which is
        # approximately RL_TOKEN_ADD_DELAY) times the rate at which we should add tokens
        # (rl_req_rate).  Update self.rl_available_tokens, which should never go
        # beyond .RL_MAX_AVAILABLE_TOKENS.
        self.rl_available_tokens = min(
            self.rl_available_tokens + (self.RL_TOKEN_ADD_DELAY * rl_req_rate),
            self.RL_MAX_AVAILABLE_TOKENS
        )
