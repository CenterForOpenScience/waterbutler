import copy
import json
import hashlib

import furl

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions

from waterbutler.providers.github import settings
from waterbutler.providers.github.path import GitHubPath
from waterbutler.providers.github.metadata import GitHubRevision
from waterbutler.providers.github.metadata import GitHubFileContentMetadata
from waterbutler.providers.github.metadata import GitHubFolderContentMetadata
from waterbutler.providers.github.metadata import GitHubFileTreeMetadata
from waterbutler.providers.github.metadata import GitHubFolderTreeMetadata
from waterbutler.providers.github.exceptions import GitHubUnsupportedRepoError


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
    """
    NAME = 'github'
    BASE_URL = settings.BASE_URL
    VIEW_URL = settings.VIEW_URL

    def __init__(self, auth, credentials, settings):
        super().__init__(auth, credentials, settings)
        self.name = self.auth.get('name', None)
        self.email = self.auth.get('email', None)
        self.token = self.credentials['token']
        self.owner = self.settings['owner']
        self.repo = self.settings['repo']
        self.metrics.add('repo', {'repo': self.repo, 'owner': self.owner})

    async def validate_v1_path(self, path, **kwargs):
        if not getattr(self, '_repo', None):
            self._repo = await self._fetch_repo()
            self.default_branch = self._repo['default_branch']

        branch_ref, ref_from = None, None
        if kwargs.get('ref'):
            branch_ref = kwargs.get('ref')
            ref_from = 'query_ref'
        elif kwargs.get('branch'):
            branch_ref = kwargs.get('branch')
            ref_from = 'query_branch'
        else:
            branch_ref = self.default_branch
            ref_from = 'default_branch'
        if isinstance(branch_ref, list):
            raise exceptions.InvalidParameters('Only one ref or branch may be given.')
        self.metrics.add('branch_ref_from', ref_from)

        if path == '/':
            return GitHubPath(path, _ids=[(branch_ref, '')])

        branch_data = await self._fetch_branch(branch_ref)

        # throws Not Found if path not in tree
        await self._search_tree_for_path(path, branch_data['commit']['commit']['tree']['sha'])

        path = GitHubPath(path)
        for part in path.parts:
            part._id = (branch_ref, None)

        # TODO Validate that filesha is a valid sha
        path.parts[-1]._id = (branch_ref, kwargs.get('fileSha'))
        self.metrics.add('file_sha_given', True if kwargs.get('fileSha') else False)

        return path

    async def validate_path(self, path, **kwargs):
        if not getattr(self, '_repo', None):
            self._repo = await self._fetch_repo()
            self.default_branch = self._repo['default_branch']

        path = GitHubPath(path)
        branch_ref, ref_from = None, None
        if kwargs.get('ref'):
            branch_ref = kwargs.get('ref')
            ref_from = 'query_ref'
        elif kwargs.get('branch'):
            branch_ref = kwargs.get('branch')
            ref_from = 'query_branch'
        else:
            branch_ref = self.default_branch
            ref_from = 'default_branch'
        if isinstance(branch_ref, list):
            raise exceptions.InvalidParameters('Only one ref or branch may be given.')
        self.metrics.add('branch_ref_from', ref_from)

        for part in path.parts:
            part._id = (branch_ref, None)

        # TODO Validate that filesha is a valid sha
        path.parts[-1]._id = (branch_ref, kwargs.get('fileSha'))
        self.metrics.add('file_sha_given', True if kwargs.get('fileSha') else False)

        return path

    async def revalidate_path(self, base, path, folder=False):
        return base.child(path, _id=((base.branch_ref, None)), folder=folder)

    def path_from_metadata(self, parent_path, metadata):
        """Build a path from a parent path and a metadata object.  Will correctly set the _id
        Used for building zip archives."""
        file_sha = metadata.extra.get('fileSha', None)
        return parent_path.child(metadata.name, _id=(metadata.ref, file_sha), folder=metadata.is_folder, )

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

    async def download(self, path, revision=None, **kwargs):
        '''Get the stream to the specified file on github
        :param str path: The path to the file on github
        :param str ref: The git 'ref' a branch or commit sha at which to get the file from
        :param str fileSha: The sha of file to be downloaded if specifed path will be ignored
        :param dict kwargs: Ignored
        '''
        data = await self.metadata(path, revision=revision)
        file_sha = path.file_sha or data.extra['fileSha']

        resp = await self.make_request(
            'GET',
            self.build_repo_url('git', 'blobs', file_sha),
            headers={'Accept': 'application/vnd.github.v3.raw'},
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
            'message': message or (settings.UPDATE_FILE_MESSAGE if exists else settings.UPLOAD_FILE_MESSAGE),
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

    async def metadata(self, path, **kwargs):
        """Get Metadata about the requested file or folder
        :param str path: The path to a file or folder
        :rtype dict: if file, metadata object describing the file
        :rtype list: if folder, array of metadata objects describing contents
        """
        if path.is_dir:
            return (await self._metadata_folder(path, **kwargs))
        else:
            return (await self._metadata_file(path, **kwargs))

    async def revisions(self, path, sha=None, **kwargs):
        resp = await self.make_request(
            'GET',
            self.build_repo_url('commits', path=path.path, sha=sha or path.file_sha),
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
        message = message or settings.UPLOAD_FILE_MESSAGE

        keep_path = path.child('.gitkeep')

        data = {
            'content': '',
            'path': keep_path.path,
            'committer': self.committer,
            'branch': path.branch_ref,
            'message': message or settings.UPLOAD_FILE_MESSAGE
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
            if resp.status == 409 or data.get('message') == 'Invalid request.\n\n"sha" wasn\'t supplied.':
                raise exceptions.FolderNamingConflict(path.name)
            raise exceptions.CreateFolderError(data, code=resp.status)

        data['content']['name'] = path.name
        data['content']['path'] = data['content']['path'].replace('.gitkeep', '')

        return GitHubFolderContentMetadata(data['content'], commit=data['commit'], ref=path.branch_ref)

    async def _delete_file(self, path, message=None, **kwargs):
        if path.file_sha:
            sha = path.file_sha
        else:
            sha = (await self.metadata(path)).extra['fileSha']

        data = {
            'sha': sha,
            'branch': path.branch_ref,
            'committer': self.committer,
            'message': message or settings.DELETE_FILE_MESSAGE,
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
        message = message or settings.DELETE_FOLDER_MESSAGE
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
        message = message or settings.DELETE_FOLDER_MESSAGE
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
        resp = await self.make_request(
            'GET',
            self.build_repo_url('branches', branch)
        )

        if resp.status == 404:
            await resp.release()
            raise exceptions.NotFoundError('. No such branch \'{}\''.format(branch))

        return (await resp.json())

    async def _fetch_contents(self, path, ref=None):
        url = furl.furl(self.build_repo_url('contents', path.path))
        if ref:
            url.args.update({'ref': ref})
        resp = await self.make_request(
            'GET',
            url.url,
            expects=(200, ),
            throws=exceptions.MetadataError
        )
        return (await resp.json())

    async def _fetch_repo(self):
        resp = await self.make_request(
            'GET',
            self.build_repo_url(),
            expects=(200, ),
            throws=exceptions.MetadataError
        )
        return (await resp.json())

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
        return provider.build_url(settings.VIEW_URL, *segments)

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

    async def _metadata_file(self, path, revision=None, **kwargs):
        resp = await self.make_request(
            'GET',
            self.build_repo_url('commits', path=path.path, sha=revision or path.branch_ref),
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

        commit_msg = settings.COPY_MESSAGE if is_copy else settings.MOVE_MESSAGE
        commit = None

        if src_path.branch_ref == dest_path.branch_ref:
            exists = self._path_exists_in_tree(src_tree['tree'], dest_path)

            # if we're overwriting an existing dir, we must remove its blobs from the tree
            if dest_path.is_dir:
                src_tree['tree'] = self._remove_path_from_tree(src_tree['tree'], dest_path)

            # if this is a copy, duplicate and append our source blobs. The originals will be updated
            # with the new destination path.
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
        :param dict old_head: The commit object will be the parent of the new commit. Must have 'sha' key.
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
