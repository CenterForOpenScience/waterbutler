import json
import base64
import aiohttp
import mimetypes

import furl

from waterbutler.core import path
from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.gitlab import settings
from waterbutler.providers.gitlab.metadata import GitLabRevision
from waterbutler.providers.gitlab.metadata import GitLabFileContentMetadata
from waterbutler.providers.gitlab.metadata import GitLabFolderContentMetadata
from waterbutler.providers.gitlab.metadata import GitLabFileTreeMetadata
from waterbutler.providers.gitlab.exceptions import GitLabUnsupportedRepoError


GIT_EMPTY_SHA = '4b825dc642cb6eb9a060e54bf8d69288fbee4904'


class GitLabPathPart(path.WaterButlerPathPart):
    def increment_name(self, _id=None):
        """Overridden to preserve branch from _id upon incrementing"""
        self._id = _id or (self._id[0], None)
        self._count += 1
        return self


class GitLabProvider(provider.BaseProvider):
    """Provider for GitLab repositories.

    **On paths:**  WB and GL use slightly different default conventions for their paths, so we
    often have to munge our WB paths before comparison. Here is a quick overview::

        WB (dirs):  wb_dir.path == 'foo/bar/'     str(wb_dir) == '/foo/bar/'
        WB (file):  wb_file.path = 'foo/bar.txt'  str(wb_file) == '/foo/bar.txt'
        GL (dir):   'foo/bar'
        GL (file):  'foo/bar.txt'

    API docs: https://docs.gitlab.com/ce/api/

    Quirks:

    * git doesn't have a concept of empty folders, so this provider creates 0-byte ``.gitkeep``
      files in the requested folder.

    """
    NAME = 'gitlab'

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
        self.BASE_URL = self.settings['base_url']
        self.VIEW_URL = self.settings['view_url']

    @property
    def default_headers(self):
        """ Headers to be included with every request.

        :rtype: :class:`dict` with `Authorization` token
        """
        return {'Authorization': 'Bearer {}'.format(self.token)}

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

        :param str path: The path to a file
        :param dict kwargs: Without `ref` or `branch` will use `default_branch`
        :rtype: WaterButlerPath
        """
        if not getattr(self, '_repo', None):
            self._repo = await self._fetch_repo()
            self.default_branch = self._repo['default_branch']

        branch_ref = kwargs.get('ref') or kwargs.get('branch') or self.default_branch

        if path == '/':
            return WaterButlerPath(path, _ids=[(branch_ref, '')])

        branch_data = await self._fetch_branch(branch_ref)
        await self._search_tree_for_path(path, branch_data['commit']['commit']['tree']['sha'])

        path = WaterButlerPath(path)
        for part in path.parts:
            part._id = (branch_ref, None)

        # TODO Validate that filesha is a valid sha
        path.parts[-1]._id = (branch_ref, kwargs.get('fileSha'))

        return path

    async def validate_path(self, path, **kwargs):
        """Ensure path is in Waterbutler format.

        :param str path: The path to a file
        :param dict kwargs: Without `ref` or `branch` will use `default_branch`
        :rtype: WaterButlerPath
        """
        if not getattr(self, '_repo', None):
            self._repo = await self._fetch_repo()
            self.default_branch = self._repo['default_branch']

        path = WaterButlerPath(path)
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

    def build_repo_url(self, *segments, **query):
        """Build the repository url with the params, retuning the complete repository url.

        :param list segments: The list of child paths
        :param dict query: The query used to append the parameters on url
        :rtype: str
        """
        segments = ('projects', self.repo_id) + segments
        return self.build_url(*segments, **query)

    async def download(self, path, revision=None, **kwargs):
        """Get the stream to the specified file on gitlab.

        :param str path: The path to the file on gitlab
        :param str revision: The revision of the file on gitlab
        :param dict kwargs: Must have `branch`
        """

        if 'branch' not in kwargs:
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

        if mimetype is not None:
            mdict_options['CONTENT-TYPE'] = mimetype

        mdict.update(mdict_options)

        resp.headers = mdict
        resp.content = streams.StringStream(raw)

        return streams.ResponseStreamReader(resp, len(raw))

    async def upload(self, stream, path, message=None, branch=None, **kwargs):
        """Uploads the given stream to GitLab.

        :param waterbutler.core.streams.RequestWrapper stream: The stream to put to GitLab
        :param str path: The full path of the key to upload to/into
        :param str message: The commit message
        :param str branch: The branch which the ``stream`` will be added
        :param dict kwargs: Ignored

        :rtype: dict, bool
        """
        assert self.name is not None
        assert self.email is not None

        insert = False
        try:
            metadata = await self.metadata(path, ref=branch)
        except:
            insert = True

        await self._upsert_blob(stream, path.path, branch, insert)

        metadata = await self.metadata(path, ref=branch)

        return metadata, insert

    async def delete(self, path, sha=None, message=None, branch=None,
               confirm_delete=0, **kwargs):
        """Delete file, folder, or provider root contents.

        :param WaterButlerPath path: WaterButlerPath path object for file, folder, or root
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
        """Get Metadata about the requested file or folder.

        :param str path: The path to a file or folder
        :param str ref: A branch or a commit SHA
        :rtype: :class:`GitLabFileTreeMetadata`
        :rtype: :class:`list` of :class:`GitLabFileContentMetadata` or :class:`GitLabFolderContentMetadata`
        """
        if path.is_dir:
            return (await self._metadata_folder(path, ref=ref, recursive=recursive, **kwargs))
        else:
            return (await self._metadata_file(path, ref=ref, **kwargs))

    async def revisions(self, path, sha=None, **kwargs):
        """Get past versions of the request file.

        :param str path: The user specified path
        :param str sha: The sha of the revision
        :param dict kwargs: Ignored
        :rtype: :class:`list` of :class:`GitLabRevision`
        """
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
        """Create a folder at `path`. Returns a `GitLabFolderContentMetadata` object
        if successful.

        :param str path: user-supplied path to create. must be a directory
        :param str branch: user-supplied repository git branch to create folder
        :param str message: user-supplied message used as commit message
        :rtype: :class:`GitLabFolderContentMetadata`
        """
        WaterButlerPath.validate_folder(path)

        message = message or settings.UPLOAD_FILE_MESSAGE
        branch = branch or path.identifier[0]

        keep_path = path.child('.gitkeep')

        content = '\n'
        stream = streams.StringStream(content)

        resp, insert = await self.upload(stream, keep_path, message, branch, **kwargs)

        raw = {'name': path.path.strip('/').split('/')[-1]}
        return GitLabFolderContentMetadata(raw, thepath=path.parent)

    async def _delete_file(self, path, message=None, branch=None, **kwargs):

        if branch is None:
            raise exceptions.DeleteError(
                'you must specify the branch to delete the file',
                code=400,
            )

        if message is None:
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

        if branch is None:
            raise exceptions.DeleteError(
                'you must specify the branch to delete the file',
                code=400,
            )

        if message is None:
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

        :param WaterButlerPath path: WaterButlerPath path object for folder
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
        if type(stream) is not streams.Base64EncodeStream:
            stream = streams.Base64EncodeStream(stream)

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
            'content': stream
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

        if ref is None:
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
