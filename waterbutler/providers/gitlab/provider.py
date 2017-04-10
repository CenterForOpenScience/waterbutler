import base64
import aiohttp
import mimetypes

import furl

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.gitlab import settings
from waterbutler.providers.gitlab.metadata import GitLabRevision
from waterbutler.providers.gitlab.metadata import GitLabFileMetadata
from waterbutler.providers.gitlab.metadata import GitLabFolderMetadata
from waterbutler.providers.gitlab.path import GitLabPath


class GitLabProvider(provider.BaseProvider):
    """Provider for GitLab repositories.

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
        self.BASE_URL = self.settings['host'] + '/api/v3'
        self.VIEW_URL = self.settings['host']

    @property
    def default_headers(self):
        """ Headers to be included with every request.

        :rtype: :class:`dict` with `Authorization` token
        """
        return {'Authorization': 'Bearer {}'.format(self.token), 'Accept': 'text/json'}

    @property
    def committer(self):
        """ Information about the commit author.

        :rtype: :class:`dict` with `name` and `email` of the author
        """
        return {
            'name': self.name,
            'email': self.email,
        }

    async def revalidate_path(self, base, path, folder=False):
        return base.child(path, _id=((base.branch_ref, None)), folder=folder)

    async def _fetch_file_contents(self, path, ref):

        url = self.build_repo_url('repository', 'files',
                                  file_path=path,
                                  ref=ref)

        resp = await self.make_request(
            'GET',
            url,
            expects=(200,),
            throws=exceptions.NotFoundError(path.full_path)
        )

        return await resp.json()


    async def _fetch_tree_contents(self, path, ref):

        url = self.build_repo_url('repository', 'tree', path=path, ref=ref)

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

    async def validate_v1_path(self, path, **kwargs):
        """Ensure path is in Waterbutler v1 format.

        :param str path: The path to a file/folder
        :rtype: GitLabPath
        :raises: :class:`waterbutler.core.exceptions.NotFoundError`
        """

        if 'ref' not in kwargs:
            raise exceptions.InvalidPathError('you must specify the ref branch')

        ref = kwargs['ref']

        g_path = GitLabPath(path)

        if g_path.is_dir:
            data = await self._fetch_tree_contents(g_path, ref)
        else:
            data = await self._fetch_file_contents(g_path, ref)

        return g_path

    async def validate_path(self, path, **kwargs):
        """Ensure path is in Waterbutler format.

        :param str path: The path to a file
        :rtype: GitLabPath
        """
        return GitLabPath(path)

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
        :raises: :class:`waterbutler.core.exceptions.DownloadError`
        """

        if 'branch' not in kwargs:
            raise exceptions.DownloadError(
                'you must specify the branch to download the file',
                code=400,
            )

        url = self.build_repo_url('repository', 'files', file_path=path.full_path,
                                  ref=kwargs['branch'])

        resp = await self.make_request(
            'GET',
            url,
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
        raise NotImplementedError

    async def delete(self, path, sha=None, message=None, branch=None,
               confirm_delete=0, **kwargs):
        raise NotImplementedError

    async def metadata(self, path, ref=None, recursive=False, **kwargs):
        """Get Metadata about the requested file or folder.

        :param GitLabPath path: The path to a file or folder
        :param str ref: A branch or a commit SHA
        :rtype: :class:`GitLabFileMetadata`
        :rtype: :class:`list` of :class:`GitLabFileMetadata` or :class:`GitLabFolderMetadata`
        :raises: :class:`waterbutler.core.exceptions.MetadataError`
        """
        try:
            if path.is_dir:
                return (await self._metadata_folder(path, ref=ref, recursive=recursive, **kwargs))
            else:
                if ref is not None:
                    return (await self._metadata_file(path, ref=ref, **kwargs))
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
        #TODO:
        resp = await self.make_request(
            'GET',
            self.build_repo_url('commits', path=path.path, sha=sha or path.identifier),
            expects=(200, ),
            throws=exceptions.RevisionsError
        )

        return [GitLabRevision(item) for item in (await resp.json())]

    async def create_folder(self, path, branch=None, message=None, **kwargs):
        raise NotImplementedError

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

    async def _metadata_folder(self, path, recursive=False, ref=None, **kwargs):
        # if we have a sha or recursive lookup specified we'll need to perform
        # the operation using the git/trees api which requires a sha.

        if not (self._is_sha(ref) or recursive):

            data = await self._fetch_tree_contents(path, ref=ref)

            ret = []
            for item in data:
                commit = ref or item['id']
                if item['type'] == 'tree':
                    ret.append(GitLabFolderMetadata(item, thepath=path, commit=commit))
                else:
                    ret.append(GitLabFileMetadata(item, web_view=item['name'],
                                                         thepath=path, commit=commit))
            return ret

    async def _metadata_file(self, path, revision=None, ref='master', **kwargs):

        resp = await self._fetch_file_contents(path, ref)

        data = await resp.json()

        if not data:
            raise exceptions.NotFoundError(str(path))

        data = {'name': data['file_name'], 'id': data['blob_id'],
                'path': data['file_path'], 'size': data['size']}

        return GitLabFileMetadata(data, commit=data['commit_id'],
                                      thepath=data['file_path'])
