import base64
import aiohttp
import mimetypes

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
        self.BASE_URL = self.settings['host'] + '/api/v4'
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
        return base.child(path, _id=((base.branch_name, None)), folder=folder)

    async def _fetch_file_contents(self, path):

        url = self.build_repo_url('repository', 'files', file_path=path.raw_path, ref=path.branch_name)

        resp = await self.make_request(
                'GET',
                url,
                expects=(200,),
                throws=exceptions.NotFoundError(path.full_path)
                )

        return await resp.json()

    async def _fetch_tree_contents(self, path):

        url = self.build_repo_url('repository', 'tree', path=path.raw_path, ref=path.branch_name)

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

    async def _fetch_default_branch(self):
        url = self.build_repo_url()

        resp = await self.make_request(
                'GET',
                url,
                expects=(200,),
                throws=exceptions.NotFoundError,
                )

        data = await resp.json()

        if 'default_branch' not in data:
            raise exceptions.NotFoundError

        return data['default_branch']

    async def validate_v1_path(self, str_path, **kwargs):
        """Ensure path is in Waterbutler v1 format.

        :param str str_path: The path to a file/folder
        :rtype: GitLabPath
        :raises: :class:`waterbutler.core.exceptions.NotFoundError`
        """
        branch_name = kwargs.get('ref') or kwargs.get('branch')
        file_sha = kwargs.get('fileSha')

        if not branch_name and not file_sha:
            if 'revisions' in kwargs:
                branch_name = kwargs['revisions']
            else:
                branch_name = await self._fetch_default_branch()

        if str_path == '/':
            return GitLabPath(str_path, _ids=[(branch_name, file_sha)])

        path = GitLabPath(str_path, _ids=[(branch_name, file_sha)])
        for part in path.parts:
            part._id = (branch_name, file_sha)

        data = await self._fetch_tree_contents(path.parent)

        data_list = []

        if path.is_dir:
            data_list = list(filter(lambda x: x['type'] == 'tree', data))
        else:
            data_list = list(filter(lambda x: x['type'] == 'blob', data))

        data_found = list(filter(lambda x: x['name'] == path.name, data_list))

        if not data_found:
            raise exceptions.NotFoundError(path.full_path)

        file_sha = data_found[0]['id']

        path.set_file_sha(file_sha)

        return path

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

        url = self.build_repo_url('repository', 'files', file_path=path.full_path,
                ref=path.branch_name)

        resp = await self.make_request(
                'GET',
                url,
                expects=(200,),
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

    async def metadata(self, path, **kwargs):
        """Get Metadata about the requested file or folder.

        :param GitLabPath path: The path to a file or folder
        :rtype: :class:`GitLabFileMetadata`
        :rtype: :class:`list` of :class:`GitLabFileMetadata` or :class:`GitLabFolderMetadata`
        :raises: :class:`waterbutler.core.exceptions.MetadataError`
        """
        try:
            if path.is_dir:
                return (await self._metadata_folder(path, **kwargs))
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
                expects=(200,),
                throws=exceptions.RevisionsError
                )

        return [GitLabRevision(item) for item in (await resp.json())]

    async def create_folder(self, path, branch=None, message=None, **kwargs):
        raise NotImplementedError

    async def _metadata_folder(self, path, **kwargs):

        data = await self._fetch_tree_contents(path)

        ret = []
        for item in data:
            name = item['name']
            if item['type'] == 'tree':
                folder_path = path.child(name, folder=True)
                ret.append(GitLabFolderMetadata(item, folder_path))
            else:
                file_path = path.child(name, folder=False)
                ret.append(GitLabFileMetadata(item, file_path, web_view=item['name']))

        return ret

    async def _metadata_file(self, path, **kwargs):

        resp = await self._fetch_file_contents(path)

        data = await resp.json()

        if not data:
            raise exceptions.NotFoundError(str(path))

        data = {'name': data['file_name'], 'id': data['blob_id'],
                'path': data['file_path'], 'size': data['size']}

        return GitLabFileMetadata(data, path)
