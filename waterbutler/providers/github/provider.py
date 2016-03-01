import copy
import json
import asyncio

import furl

from waterbutler.core import path
from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions

from waterbutler.providers.github import settings
from waterbutler.providers.github.metadata import GitHubRevision
from waterbutler.providers.github.metadata import GitHubFileContentMetadata
from waterbutler.providers.github.metadata import GitHubFolderContentMetadata
from waterbutler.providers.github.metadata import GitHubFileTreeMetadata
from waterbutler.providers.github.metadata import GitHubFolderTreeMetadata


GIT_EMPTY_SHA = '4b825dc642cb6eb9a060e54bf8d69288fbee4904'


class GitHubPathPart(path.WaterButlerPathPart):
    def increment_name(self, _id=None):
        """Overridden to preserve branch from _id upon incrementing"""
        self._id = _id or (self._id[0], None)
        self._count += 1
        return self


class GitHubPath(path.WaterButlerPath):
    PART_CLASS = GitHubPathPart

    def child(self, name, _id=None, folder=False):
        if _id is None:
            _id = (self.identifier[0], None)
        return super().child(name, _id=_id, folder=folder)


class GitHubProvider(provider.BaseProvider):
    NAME = 'github'
    BASE_URL = settings.BASE_URL
    VIEW_URL = settings.VIEW_URL

    @staticmethod
    def is_sha(ref):
        # sha1 is always 40 characters in length
        try:
            if len(ref) != 40:
                return False
            # sha1 is always base 16 (hex)
            int(ref, 16)
        except (TypeError, ValueError, ):
            return False
        return True

    def __init__(self, auth, credentials, settings):
        super().__init__(auth, credentials, settings)
        self.name = self.auth.get('name', None)
        self.email = self.auth.get('email', None)
        self.token = self.credentials['token']
        self.owner = self.settings['owner']
        self.repo = self.settings['repo']

    @asyncio.coroutine
    def validate_v1_path(self, path, **kwargs):
        if not getattr(self, '_repo', None):
            self._repo = yield from self._fetch_repo()
            self.default_branch = self._repo['default_branch']

        branch_ref = kwargs.get('ref') or kwargs.get('branch') or self.default_branch

        implicit_folder = path.endswith('/')

        url = furl.furl(self.build_repo_url('contents', path))
        url.args.update({'ref': branch_ref})
        resp = yield from self.make_request(
            'GET',
            url.url,
            expects=(200, ),
            throws=exceptions.MetadataError
        )

        content = yield from resp.json()
        explicit_folder = isinstance(content, list)

        if implicit_folder != explicit_folder:
            raise exceptions.NotFoundError(path)

        path = GitHubPath(path)
        for part in path.parts:
            part._id = (branch_ref, None)

        # TODO Validate that filesha is a valid sha
        path.parts[-1]._id = (branch_ref, kwargs.get('fileSha'))

        return path

    @asyncio.coroutine
    def validate_path(self, path, **kwargs):
        if not getattr(self, '_repo', None):
            self._repo = yield from self._fetch_repo()
            self.default_branch = self._repo['default_branch']

        path = GitHubPath(path)
        branch_ref = kwargs.get('ref') or kwargs.get('branch') or self.default_branch

        for part in path.parts:
            part._id = (branch_ref, None)

        # TODO Validate that filesha is a valid sha
        path.parts[-1]._id = (branch_ref, kwargs.get('fileSha'))

        return path

    @asyncio.coroutine
    def revalidate_path(self, base, path, folder=False):
        return base.child(path, _id=((base.identifier[0], None)), folder=folder)

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

    def intra_copy(self, dest_provider, src_path, dest_path):
        return (yield from self._do_intra_move_or_copy(src_path, dest_path, True))

    def intra_move(self, dest_provider, src_path, dest_path):
        return (yield from self._do_intra_move_or_copy(src_path, dest_path, False))

    @asyncio.coroutine
    def download(self, path, revision=None, **kwargs):
        '''Get the stream to the specified file on github
        :param str path: The path to the file on github
        :param str ref: The git 'ref' a branch or commit sha at which to get the file from
        :param str fileSha: The sha of file to be downloaded if specifed path will be ignored
        :param dict kwargs: Ignored
        '''
        data = yield from self.metadata(path, revision=revision)
        file_sha = path.identifier[1] or data.extra['fileSha']

        resp = yield from self.make_request(
            'GET',
            self.build_repo_url('git', 'blobs', file_sha),
            headers={'Accept': 'application/vnd.github.VERSION.raw'},
            expects=(200, ),
            throws=exceptions.DownloadError,
        )

        return streams.ResponseStreamReader(resp, size=data.size)

    @asyncio.coroutine
    def upload(self, stream, path, message=None, branch=None, **kwargs):
        assert self.name is not None
        assert self.email is not None

        try:
            exists = yield from self.exists(path)
        except exceptions.ProviderError as e:
            if e.data.get('message') == 'Git Repository is empty.':
                exists = False
                resp = yield from self.make_request(
                    'PUT',
                    self.build_repo_url('contents', '.gitkeep'),
                    data=json.dumps({
                        'content': '',
                        'path': '.gitkeep',
                        'committer': self.committer,
                        'branch': path.identifier[0],
                        'message': 'Initial commit'
                    }),
                    expects=(201,),
                    throws=exceptions.CreateFolderError
                )
                data = yield from resp.json()
                latest_sha = data['commit']['sha']
        else:
            latest_sha = yield from self._get_latest_sha(ref=path.identifier[0])

        blob = yield from self._create_blob(stream)
        tree = yield from self._create_tree({
            'base_tree': latest_sha,
            'tree': [{
                'path': path.path,
                'mode': '100644',
                'type': 'blob',
                'sha': blob['sha']
            }]
        })

        commit = yield from self._create_commit({
            'tree': tree['sha'],
            'parents': [latest_sha],
            'committer': self.committer,
            'message': message or (settings.UPDATE_FILE_MESSAGE if exists else settings.UPLOAD_FILE_MESSAGE),
        })

        # Doesn't return anything useful
        yield from self._update_ref(commit['sha'], ref=path.identifier[0])

        # You're hacky
        return GitHubFileTreeMetadata({
            'path': path.path,
            'sha': blob['sha'],
            'size': stream.size,
        }, commit=commit), not exists

    @asyncio.coroutine
    def delete(self, path, sha=None, message=None, branch=None, **kwargs):
        """Delete file, folder, or provider root contents

        :param GitHubPath path: GitHubPath path object for file, folder, or root
        :param str sha: SHA-1 checksum of file/folder object
        :param str message: Commit message
        :param str branch: Repository branch
        """
        assert self.name is not None
        assert self.email is not None

        if path.is_root:
            yield from self._delete_folder_contents(path)
        elif path.is_dir:
            yield from self._delete_folder(path, message, **kwargs)
        else:
            yield from self._delete_file(path, message, **kwargs)

    @asyncio.coroutine
    def metadata(self, path, ref=None, recursive=False, **kwargs):
        """Get Metadata about the requested file or folder
        :param str path: The path to a file or folder
        :param str ref: A branch or a commit SHA
        :rtype dict:
        :rtype list:
        """
        if path.is_dir:
            return (yield from self._metadata_folder(path, ref=ref, recursive=recursive, **kwargs))
        else:
            return (yield from self._metadata_file(path, ref=ref, **kwargs))

    @asyncio.coroutine
    def revisions(self, path, sha=None, **kwargs):
        resp = yield from self.make_request(
            'GET',
            self.build_repo_url('commits', path=path.path, sha=sha or path.identifier),
            expects=(200, ),
            throws=exceptions.RevisionsError
        )

        return [
            GitHubRevision(item)
            for item in (yield from resp.json())
        ]

    @asyncio.coroutine
    def create_folder(self, path, branch=None, message=None, **kwargs):
        GitHubPath.validate_folder(path)

        assert self.name is not None
        assert self.email is not None
        message = message or settings.UPLOAD_FILE_MESSAGE

        keep_path = path.child('.gitkeep')

        data = {
            'content': '',
            'path': keep_path.path,
            'committer': self.committer,
            'branch': path.identifier[0],
            'message': message or settings.UPLOAD_FILE_MESSAGE
        }

        resp = yield from self.make_request(
            'PUT',
            self.build_repo_url('contents', keep_path.path),
            data=json.dumps(data),
            expects=(201, 422, 409),
            throws=exceptions.CreateFolderError
        )

        data = yield from resp.json()

        if resp.status in (422, 409):
            if resp.status == 409 or data.get('message') == 'Invalid request.\n\n"sha" wasn\'t supplied.':
                raise exceptions.FolderNamingConflict(str(path))
            raise exceptions.CreateFolderError(data, code=resp.status)

        data['content']['name'] = path.name
        data['content']['path'] = data['content']['path'].replace('.gitkeep', '')

        return GitHubFolderContentMetadata(data['content'], commit=data['commit'])

    @asyncio.coroutine
    def _delete_file(self, path, message=None, **kwargs):
        if path.identifier[1]:
            sha = path.identifier
        else:
            sha = (yield from self.metadata(path)).extra['fileSha']

        if not sha:
            raise exceptions.MetadataError('A sha is required for deleting')

        data = {
            'sha': sha,
            'branch': path.identifier[0],
            'committer': self.committer,
            'message': message or settings.DELETE_FILE_MESSAGE,
        }

        yield from self.make_request(
            'DELETE',
            self.build_repo_url('contents', path.path),
            headers={'Content-Type': 'application/json'},
            data=json.dumps(data),
            expects=(200, ),
            throws=exceptions.DeleteError,
        )

    @asyncio.coroutine
    def _delete_folder(self, path, message=None, **kwargs):
        branch_data = yield from self._fetch_branch(path.identifier[0])

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
                for item in (yield from self._fetch_tree(old_commit_tree_sha))['tree']
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
                    for item in (yield from self._fetch_tree(tree_sha))['tree']
                ]
            })

        # The last tree's structure is rewritten w/o the target folder, all others
        # in the hierarchy are simply updated to reflect this change.
        tree = trees.pop()
        if tree['target'] == '':
            # Git Empty SHA
            tree_sha = GIT_EMPTY_SHA
        else:
            # Delete the folder from the tree cast to list iterator over all values
            tree['tree'] = list(filter(lambda x: x['path'] != tree['target'], tree['tree']))

            tree_data = yield from self._create_tree({'tree': tree['tree']})
            tree_sha = tree_data['sha']

            # Update parent tree(s)
            for tree in reversed(trees):
                for item in tree['tree']:
                    if item['path'] == tree['target']:
                        item['sha'] = tree_sha
                        break
                tree_data = yield from self._create_tree({'tree': tree['tree']})
                tree_sha = tree_data['sha']

        # Create a new commit which references our top most tree change.
        message = message or settings.DELETE_FOLDER_MESSAGE
        commit_resp = yield from self.make_request(
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
        commit_data = yield from commit_resp.json()
        commit_sha = commit_data['sha']

        # Update repository reference, point to the newly created commit.
        # No need to store data, rely on expects to raise exceptions
        yield from self.make_request(
            'PATCH',
            self.build_repo_url('git', 'refs', 'heads', path.identifier[0]),
            headers={'Content-Type': 'application/json'},
            data=json.dumps({'sha': commit_sha}),
            expects=(200, ),
            throws=exceptions.DeleteError,
        )

    @asyncio.coroutine
    def _delete_folder_contents(self, path, message=None, **kwargs):
        """Delete the contents of a folder. For use against provider root.

        :param GitHubPath path: GitHubPath path object for folder
        :param str message: Commit message
        """
        meta = (yield from self.metadata(path))
        for child in meta:
            github_path = yield from self.validate_path(child.path)
            yield from self.delete(github_path)

    @asyncio.coroutine
    def _fetch_branch(self, branch):
        resp = yield from self.make_request(
            'GET',
            self.build_repo_url('branches', branch)
        )
        return (yield from resp.json())

    @asyncio.coroutine
    def _fetch_contents(self, path, ref=None):
        url = furl.furl(self.build_repo_url('contents', path.path))
        if ref:
            url.args.update({'ref': ref})
        resp = yield from self.make_request(
            'GET',
            url.url,
            expects=(200, ),
            throws=exceptions.MetadataError
        )
        return (yield from resp.json())

    @asyncio.coroutine
    def _fetch_repo(self):
        resp = yield from self.make_request(
            'GET',
            self.build_repo_url(),
            expects=(200, ),
            throws=exceptions.MetadataError
        )
        return (yield from resp.json())

    @asyncio.coroutine
    def _fetch_tree(self, sha, recursive=False):
        url = furl.furl(self.build_repo_url('git', 'trees', sha))
        if recursive:
            url.args.update({'recursive': 1})
        resp = yield from self.make_request(
            'GET',
            url.url,
            expects=(200, ),
            throws=exceptions.MetadataError
        )
        tree = yield from resp.json()

        if tree['truncated']:
            raise exceptions.ProviderError(
                ('Some folder operations on large GitHub repositories cannot be supported without'
                 ' data loss.  To carry out this operation, please perform it in a local git'
                 ' repository, then push to the target repository on GitHub.'),
                code=501
            )

        return tree

    @asyncio.coroutine
    def _create_tree(self, tree):
        resp = yield from self.make_request(
            'POST',
            self.build_repo_url('git', 'trees'),
            headers={'Content-Type': 'application/json'},
            data=json.dumps(tree),
            expects=(201, ),
            throws=exceptions.ProviderError,
        )
        return (yield from resp.json())

    @asyncio.coroutine
    def _create_commit(self, commit):
        resp = yield from self.make_request(
            'POST',
            self.build_repo_url('git', 'commits'),
            headers={'Content-Type': 'application/json'},
            data=json.dumps(commit),
            expects=(201, ),
            throws=exceptions.ProviderError,
        )
        return (yield from resp.json())

    @asyncio.coroutine
    def _create_blob(self, stream):
        blob_stream = streams.JSONStream({
            'encoding': 'base64',
            'content': streams.Base64EncodeStream(stream),
        })

        resp = yield from self.make_request(
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
        return (yield from resp.json())

    def _is_sha(self, ref):
        # sha1 is always 40 characters in length
        try:
            if len(ref) != 40:
                return False
            # sha1 is always base 16 (hex)
            int(ref, 16)
        except (TypeError, ValueError, ):
            return False
        return True

    def _web_view(self, path):
        segments = (self.owner, self.repo, 'blob', path.identifier[0], path.path)
        return provider.build_url(settings.VIEW_URL, *segments)

    @asyncio.coroutine
    def _metadata_folder(self, path, recursive=False, **kwargs):
        # if we have a sha or recursive lookup specified we'll need to perform
        # the operation using the git/trees api which requires a sha.

        if not (self._is_sha(path.identifier[0]) or recursive):
            try:
                data = yield from self._fetch_contents(path, ref=path.identifier[0])
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
                    ret.append(GitHubFolderContentMetadata(item))
                else:
                    ret.append(GitHubFileContentMetadata(item, web_view=item['html_url']))
            return ret

    @asyncio.coroutine
    def _metadata_file(self, path, revision=None, ref=None, **kwargs):
        resp = yield from self.make_request(
            'GET',
            self.build_repo_url('commits', path=path.path, sha=revision or ref or path.identifier[0]),
            expects=(200, ),
            throws=exceptions.MetadataError,
        )

        commits = yield from resp.json()

        if not commits:
            raise exceptions.NotFoundError(str(path))

        latest = commits[0]
        tree = yield from self._fetch_tree(latest['commit']['tree']['sha'], recursive=True)

        try:
            data = next(
                x for x in tree['tree']
                if x['path'] == path.path
            )
        except StopIteration:
            raise exceptions.NotFoundError(str(path))

        if isinstance(data, list):
            raise exceptions.MetadataError(
                'Could not retrieve file "{0}"'.format(str(path)),
                code=404,
            )

        return GitHubFileTreeMetadata(data, commit=latest['commit'], web_view=self._web_view(path))

    @asyncio.coroutine
    def _get_latest_sha(self, ref='master'):
        resp = yield from self.make_request(
            'GET',
            self.build_repo_url('git', 'refs', 'heads', ref),
            expects=(200, ),
            throws=exceptions.ProviderError
        )
        data = yield from resp.json()
        return data['object']['sha']

    @asyncio.coroutine
    def _update_ref(self, sha, ref='master'):
        resp = yield from self.make_request(
            'POST',
            self.build_repo_url('git', 'refs', 'heads', ref),
            data=json.dumps({
                'sha': sha,
            }),
            expects=(200, ),
            throws=exceptions.ProviderError
        )
        return (yield from resp.json())

    @asyncio.coroutine
    def _do_intra_move_or_copy(self, src_path, dest_path, is_copy):

        # ON PATHS:
        #   WB and GH use slightly different default conventions for their paths, so we often
        #   have to munge our WB paths before comparison. Here is a quick overview:
        #     WB (dirs):  wb_dir.path == 'foo/bar/'     str(wb_dir) == '/foo/bar/'
        #     WB (file):  wb_file.path = 'foo/bar.txt'  str(wb_file) == '/foo/bar.txt'
        #     GH (dir):   'foo/bar'
        #     GH (file):  'foo/bar.txt'

        branch = src_path.identifier[0]
        branch_data = yield from self._fetch_branch(branch)

        old_commit_sha = branch_data['commit']['sha']
        old_commit_tree_sha = branch_data['commit']['commit']['tree']['sha']

        tree = yield from self._fetch_tree(old_commit_tree_sha, recursive=True)
        exists = any(x['path'] == dest_path.path.rstrip('/') for x in tree['tree'])

        blobs = [
            item
            for item in tree['tree']
            if src_path.is_dir and item['path'].startswith(src_path.path) or
            src_path.is_file and item['path'] == src_path.path
        ]

        if len(blobs) == 0:
            raise exceptions.NotFoundError(str(src_path))

        if src_path.is_file:
            assert len(blobs) == 1, 'Found multiple targets'

        if is_copy:
            tree['tree'].extend([copy.deepcopy(blob) for blob in blobs])

        for blob in blobs:
            blob['path'] = blob['path'].replace(src_path.path, dest_path.path, 1)

        # github infers tree contents from blob paths
        # see: http://www.levibotelho.com/development/commit-a-file-with-the-github-api/
        tree['tree'] = [item for item in tree['tree'] if item['type'] != 'tree']
        new_tree_data = yield from self._create_tree({'tree': tree['tree']})
        new_tree_sha = new_tree_data['sha']

        # Create a new commit which references our top most tree change.
        commit_resp = yield from self.make_request(
            'POST',
            self.build_repo_url('git', 'commits'),
            headers={'Content-Type': 'application/json'},
            data=json.dumps({
                'tree': new_tree_sha,
                'parents': [old_commit_sha],
                'committer': self.committer,
                'message': settings.COPY_MESSAGE if is_copy else settings.MOVE_MESSAGE
            }),
            expects=(201, ),
            throws=exceptions.DeleteError,
        )

        commit = yield from commit_resp.json()

        # Update repository reference, point to the newly created commit.
        # No need to store data, rely on expects to raise exceptions
        yield from self.make_request(
            'PATCH',
            self.build_repo_url('git', 'refs', 'heads', branch),
            headers={'Content-Type': 'application/json'},
            data=json.dumps({'sha': commit['sha']}),
            expects=(200, ),
            throws=exceptions.DeleteError,
        )

        if dest_path.is_file:
            assert len(blobs) == 1, 'Destination file should have exactly one candidate'
            return GitHubFileTreeMetadata(blobs[0], commit=commit), not exists

        folder = GitHubFolderTreeMetadata({
            'path': dest_path.path.strip('/')
        }, commit=commit)

        folder.children = []

        for item in blobs:
            if item['path'] == src_path.path.rstrip('/'):
                continue
            if item['type'] == 'tree':
                folder.children.append(GitHubFolderTreeMetadata(item))
            else:
                folder.children.append(GitHubFileTreeMetadata(item))

        return folder, not exists
