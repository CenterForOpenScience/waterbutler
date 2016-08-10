import copy
import json
import pdb
import base64
import aiohttp
import mimetypes

import furl

from waterbutler.core import path
from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions

from waterbutler.providers.gitlab import settings
from waterbutler.providers.gitlab.metadata import GitLabRevision
from waterbutler.providers.gitlab.metadata import GitLabFileContentMetadata
from waterbutler.providers.gitlab.metadata import GitLabFolderContentMetadata
from waterbutler.providers.gitlab.metadata import GitLabFileTreeMetadata
from waterbutler.providers.gitlab.metadata import GitLabFolderTreeMetadata
from waterbutler.providers.gitlab.exceptions import GitLabUnsupportedRepoError


GIT_EMPTY_SHA = '4b825dc642cb6eb9a060e54bf8d69288fbee4904'


class GitLabPathPart(path.WaterButlerPathPart):
    def increment_name(self, _id=None):
        """Overridden to preserve branch from _id upon incrementing"""
        self._id = _id or (self._id[0], None)
        self._count += 1
        return self


class GitLabPath(path.WaterButlerPath):
    PART_CLASS = GitLabPathPart

    def child(self, name, _id=None, folder=False):
        if _id is None:
            _id = (self.identifier[0], None)
        return super().child(name, _id=_id, folder=folder)


class GitLabProvider(provider.BaseProvider):
    """Provider for GitLab repositories.

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
    NAME = 'gitlab'
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
        self.repo_id = self.settings['repo_id']

    async def validate_v1_path(self, path, **kwargs):
        if not getattr(self, '_repo', None):
            self._repo = await self._fetch_repo()
            self.default_branch = self._repo['default_branch']

        branch_ref = kwargs.get('ref') or kwargs.get('branch') or self.default_branch

        if path == '/':
            return GitLabPath(path, _ids=[(branch_ref, '')])

        branch_data = await self._fetch_branch(branch_ref)
        await self._search_tree_for_path(path, branch_data['commit']['commit']['tree']['sha'])

        path = GitLabPath(path)
        for part in path.parts:
            part._id = (branch_ref, None)

        # TODO Validate that filesha is a valid sha
        path.parts[-1]._id = (branch_ref, kwargs.get('fileSha'))

        return path

    async def validate_path(self, path, **kwargs):
        if not getattr(self, '_repo', None):
            self._repo = await self._fetch_repo()
            self.default_branch = self._repo['default_branch']

        path = GitLabPath(path)
        branch_ref = kwargs.get('ref') or kwargs.get('branch') or self.default_branch

        for part in path.parts:
            part._id = (branch_ref, None)

        # TODO Validate that filesha is a valid sha
        path.parts[-1]._id = (branch_ref, kwargs.get('fileSha'))

        return path

    async def revalidate_path(self, base, path, folder=False):
        return base.child(path, _id=((base.identifier[0], None)), folder=folder)

    def can_duplicate_names(self):
        return False

    @property
    def default_headers(self):
        return {'Authorization': 'Bearer {}'.format(self.token)}

    @property
    def committer(self):
        return {
            'name': self.name,
            'email': self.email,
        }

    def build_repo_url(self, *segments, **query):
        segments = ('projects', self.repo_id) + segments
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
        :param dict kwargs: Ignored
        '''

        if not 'branch' in kwargs:
            raise exceptions.DownloadError(
                'you must specify the branch to download the file',
                code=400,
            )

        url = self.build_repo_url('repository', 'files', file_path=path.full_path, ref=kwargs['branch'])

        headers = {"Authorization": 'Bearer {}'.format(self.token)}

        resp = await self.make_request(
            'GET',
            url,
            headers=headers,
            expects=(200, ),
            throws=exceptions.DownloadError,
        )

        data = await resp.json()
        raw = base64.b64decode(data['content'])

        mdict = aiohttp.multidict.MultiDict(resp.headers)

        mimetype = mimetypes.guess_type(path.full_path)[0]

        mdict_options = {}

        if mimetype != None:
            mdict_options['CONTENT-TYPE'] = mimetype

        mdict.update(mdict_options)


        resp.headers = mdict
        resp.content = streams.StringStream(raw)

        return streams.ResponseStreamReader(resp, len(raw))

    async def upload(self, stream, path, message=None, branch=None, **kwargs):
        assert self.name is not None
        assert self.email is not None

        insert = False
        try:
            metadata = await self.metadata(path, ref=branch)
        except:
            insert = True

        blob = await self._upsert_blob(stream, path.path, branch, insert)

        metadata = await self.metadata(path, ref=branch)

        return metadata, insert

    async def delete(self, path, sha=None, message=None, branch=None,
               confirm_delete=0, **kwargs):
        """Delete file, folder, or provider root contents

        :param GitLabPath path: GitLabPath path object for file, folder, or root
        :param str sha: SHA-1 checksum of file/folder object
        :param str message: Commit message
        :param str branch: Repository branch
        :param int confirm_delete: Must be 1 to confirm root folder delete
        """
        assert self.name is not None
        assert self.email is not None

        if not path.is_dir:
            await self._delete_file(path, message, branch, **kwargs)

    async def metadata(self, path, ref=None, recursive=False, **kwargs):
        """Get Metadata about the requested file or folder
        :param str path: The path to a file or folder
        :param str ref: A branch or a commit SHA
        :rtype dict:
        :rtype list:
        """
        if path.is_dir:
            return (await self._metadata_folder(path, ref=ref, recursive=recursive, **kwargs))
        else:
            return (await self._metadata_file(path, ref=ref, **kwargs))

    async def revisions(self, path, sha=None, **kwargs):
        resp = await self.make_request(
            'GET',
            self.build_repo_url('commits', path=path.path, sha=sha or path.identifier),
            expects=(200, ),
            throws=exceptions.RevisionsError
        )

        return [
            GitLabRevision(item)
            for item in (await resp.json())
        ]

    async def create_folder(self, path, branch=None, message=None, **kwargs):
        GitLabPath.validate_folder(path)

        message = message or settings.UPLOAD_FILE_MESSAGE

        keep_path = path.child('.gitkeep')

        content = '\n'
        stream = streams.StringStream(content)
        branch = path.identifier[0]
        commit_msg = message or settings.UPLOAD_FILE_MESSAGE

        resp, insert = await self.upload(stream, keep_path, message, branch, **kwargs)

        metadata = await self.metadata(path, ref=branch, **kwargs)

        pdb.set_trace()

        if not metadata:
            raise exceptions.NotFoundError(path.full_path)

        return metadata[0]

    async def _delete_file(self, path, message=None, branch=None, **kwargs):

        if branch == None:
            raise exceptions.DeleteError(
                'you must specify the branch to delete the file',
                code=400,
            )


        if message == None:
            message = 'File {} deleted'.format(path.full_path)

        url = self.build_repo_url('repository', 'files', file_path=path.full_path, branch_name=branch, commit_message=message)

        headers = {"Authorization": 'Bearer {}'.format(self.token)}

        resp = await self.make_request(
            'DELETE',
            url,
            headers=headers,
            expects=(200, ),
            throws=exceptions.DeleteError,
        )
        await resp.release()

    async def _delete_folder(self, path, message=None, branch=None, **kwargs):

        if branch == None:
            raise exceptions.DeleteError(
                'you must specify the branch to delete the file',
                code=400,
            )


        if message == None:
            message = 'Folder {} deleted'.format(path.full_path)

        url = self.build_repo_url('repository', 'files', file_path=path.full_path, branch_name=branch, commit_message=message)

        headers = {"Authorization": 'Bearer {}'.format(self.token)}

        resp = await self.make_request(
            'DELETE',
            url,
            headers=headers,
            expects=(200, ),
            throws=exceptions.DeleteError,
        )
        await resp.release()

    async def _delete_root_folder_contents(self, path, message=None, **kwargs):
        """Delete the contents of the root folder.

        :param GitLabPath path: GitLabPath path object for folder
        :param str message: Commit message
        """
        branch_data = await self._fetch_branch(path.identifier[0])
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
            self.build_repo_url('git', 'refs', 'heads', path.identifier[0]),
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
        return (await resp.json())

    async def _fetch_contents(self, path, ref=None):
        url = furl.furl(self.build_repo_url('repository', 'tree'))

        if path.full_path:
            url.add({'path': path.full_path})

        if ref:
            url.args.update({'ref_name': ref})

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
            raise GitLabUnsupportedRepoError

        return tree

    async def _search_tree_for_path(self, path, tree_sha, recursive=True):
        """Search through the given tree for an entity matching the name and type of `path`.
        """
        tree = await self._fetch_tree(tree_sha, recursive=True)

        if tree['truncated']:
            raise GitLabUnsupportedRepoError

        implicit_type = 'tree' if path.endswith('/') else 'blob'

        for entity in tree['tree']:
            if entity['path'] == path.strip('/') and entity['type'] == implicit_type:
                return entity

        raise exceptions.NotFoundError(str(path))

    async def _upsert_blob(self, stream, filepath, branchname, insert=True):

        if insert:
            message = 'File {0} created'.format(filepath)
            method = 'POST'
        else:
            message = 'File {0} updated'.format(filepath)
            method = 'PUT'

        blob_stream = streams.JSONStream({
            'file_path': filepath,
            'branch_name': branchname,
            'commit_message': message,
            'encoding': 'base64',
            'content': streams.Base64EncodeStream(stream),
        })

        resp = await self.make_request(
            method,
            self.build_repo_url('repository', 'files'),
            data=blob_stream,
            headers={
                'Content-Type': 'application/json',
                'Content-Length': str(blob_stream.size),
            },
            throws=exceptions.UploadError,
        )
        return (await resp.json())

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

    async def _metadata_folder(self, path, recursive=False, ref=None, **kwargs):
        # if we have a sha or recursive lookup specified we'll need to perform
        # the operation using the git/trees api which requires a sha.

        if not (self._is_sha(ref) or recursive):
            try:
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
                commit = ref or item['id']
                if item['type'] == 'tree':
                    ret.append(GitLabFolderContentMetadata(item, thepath=path, commit=commit))
                else:
                    ret.append(GitLabFileContentMetadata(item, web_view=item['name'], thepath=path, commit=commit))
            return ret

    async def _metadata_file(self, path, revision=None, ref=None, **kwargs):

        if ref == None:
            ref = 'master'

        resp = await self.make_request(
            'GET',
            self.build_repo_url('repository', 'files', file_path=path.full_path, ref=ref),
            expects=(200, ),
            throws=exceptions.MetadataError,
        )

        commits = await resp.json()

        if not commits:
            raise exceptions.NotFoundError(str(path))

        data = {'name': commits['file_name'], 'id': commits['blob_id'], 'path': commits['file_path'], 'size': commits['size']}

        return GitLabFileTreeMetadata(data, commit=commits['commit_id'], thepath=commits['file_path'])

    async def _get_latest_sha(self, ref='master'):
        resp = await self.make_request(
            'GET',
            self.build_repo_url('git', 'refs', 'heads', ref),
            expects=(200, ),
            throws=exceptions.ProviderError
        )
        data = await resp.json()
        return data['object']['sha']

    async def _do_intra_move_or_copy(self, src_path, dest_path, is_copy):

        # ON PATHS:
        #   WB and GH use slightly different default conventions for their paths, so we often
        #   have to munge our WB paths before comparison. Here is a quick overview:
        #     WB (dirs):  wb_dir.path == 'foo/bar/'     str(wb_dir) == '/foo/bar/'
        #     WB (file):  wb_file.path = 'foo/bar.txt'  str(wb_file) == '/foo/bar.txt'
        #     GH (dir):   'foo/bar'
        #     GH (file):  'foo/bar.txt'

        branch = src_path.identifier[0]
        branch_data = await self._fetch_branch(branch)

        old_commit_sha = branch_data['commit']['sha']
        old_commit_tree_sha = branch_data['commit']['commit']['tree']['sha']

        tree = await self._fetch_tree(old_commit_tree_sha, recursive=True)
        exists = any(x['path'] == dest_path.path.rstrip('/') for x in tree['tree'])

        # these are the blobs to copy/move
        blobs = [
            item
            for item in tree['tree']
            if src_path.is_dir and item['path'].startswith(src_path.path) or
            src_path.is_file and item['path'] == src_path.path
        ]

        # if we're overwriting an existing dir, we must remove its blobs from the tree
        if dest_path.is_dir:
            tree['tree'] = [
                item
                for item in tree['tree']
                if not item['path'].startswith(dest_path.path)
            ]

        if len(blobs) == 0:
            raise exceptions.NotFoundError(str(src_path))

        if src_path.is_file:
            assert len(blobs) == 1, 'Found multiple targets'

        # if this is a copy, duplicate and append our source blobs. The originals will be updated
        # with the new destination path.
        if is_copy:
            tree['tree'].extend([copy.deepcopy(blob) for blob in blobs])

        # see, I told you they'd be overwritten
        for blob in blobs:
            blob['path'] = blob['path'].replace(src_path.path, dest_path.path, 1)

        # github infers tree contents from blob paths
        # see: http://www.levibotelho.com/development/commit-a-file-with-the-github-api/
        tree['tree'] = [item for item in tree['tree'] if item['type'] != 'tree']
        new_tree_data = await self._create_tree({'tree': tree['tree']})
        new_tree_sha = new_tree_data['sha']

        # Create a new commit which references our top most tree change.
        commit_resp = await self.make_request(
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

        commit = await commit_resp.json()

        # Update repository reference, point to the newly created commit.
        # No need to store data, rely on expects to raise exceptions
        resp = await self.make_request(
            'PATCH',
            self.build_repo_url('git', 'refs', 'heads', branch),
            headers={'Content-Type': 'application/json'},
            data=json.dumps({'sha': commit['sha']}),
            expects=(200, ),
            throws=exceptions.DeleteError,
        )
        await resp.release()

        if dest_path.is_file:
            assert len(blobs) == 1, 'Destination file should have exactly one candidate'
            return GitLabFileTreeMetadata(blobs[0], commit=commit), not exists

        folder = GitLabFolderTreeMetadata({
            'path': dest_path.path.strip('/')
        }, commit=commit)

        folder.children = []

        for item in blobs:
            if item['path'] == src_path.path.rstrip('/'):
                continue
            if item['type'] == 'tree':
                folder.children.append(GitLabFolderTreeMetadata(item))
            else:
                folder.children.append(GitLabFileTreeMetadata(item))

        return folder, not exists
