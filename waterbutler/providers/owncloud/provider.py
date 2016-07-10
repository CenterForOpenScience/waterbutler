import json
from urllib import parse
import aiohttp

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.owncloud import utils
from waterbutler.providers.owncloud.metadata import OwnCloudFolderMetadata


class OwnCloudProvider(provider.BaseProvider):
    """Provider for the ownCloud cloud storage service.

    This provider uses the OCS1.7 standard for communication

    API docs: https://www.freedesktop.org/wiki/Specifications/open-collaboration-services-1.7/
    """
    NAME = 'owncloud'

    def __init__(self, auth, credentials, settings):
        super().__init__(auth, credentials, settings)

        self.folder = settings['folder']
        self.url = credentials['host']
        self._auth = aiohttp.BasicAuth(
            credentials['username'],
            credentials['password']
        )
        self.connector = aiohttp.TCPConnector(verify_ssl=False)

    @property
    def _webdav_url_(self):
        if self.url[-1] != '/':
            return self.url + '/remote.php/webdav/'
        return self.url + 'remote.php/webdav/'

    async def validate_v1_path(self, path, **kwargs):
        if path == '/':
            return WaterButlerPath(path, prepend=self.folder)
        return WaterButlerPath(path, prepend=self.folder)

    async def validate_path(self, path, **kwargs):
        return await self.validate_v1_path(path, **kwargs)

    async def download(self, path, accept_url=False, range=None, **kwargs):
        metadata = await self.metadata(path)
        download_resp = await self.make_request(
            'GET',
            self._webdav_url_ + parse.quote(path.full_path),
            range=range,
            expects=(200, 204, 206),
            throws=exceptions.DownloadError,
            auth=self._auth,
            connector=self.connector
        )

        if metadata.size is not None:
            return streams.ResponseStreamReader(download_resp, size=metadata.size)

        stream = streams.StringStream(await download_resp.read())
        if download_resp.headers.get('Content-Type'):
            stream.content_type = download_resp.headers['Content-Type']
        stream.name = metadata.name
        return stream

    async def upload(self, stream, path, conflict='replace', **kwargs):
        if path.identifier and conflict == 'keep':
            path, _ = await self.handle_name_conflict(path, conflict=conflict, kind='folder')
            path._parts[-1]._id = None

        data_stream = streams.FormDataStream(
            attributes=json.dumps({
                'name': path.name,
                'parent': {
                    'id': path.parent.identifier
                }
            })
        )
        data_stream.add_file('file', stream, path.name, disposition='form-data')

        response = await self.make_request(
            'PUT',
            self._webdav_url_ + parse.quote(path.full_path),
            data=data_stream,
            headers=data_stream.headers,
            expects=(201, 200),
            throws=exceptions.UploadError,
            auth=self._auth,
            connector=self.connector
        )
        await response.release()
        meta = await self.metadata(path)
        return meta, True

    async def delete(self, path, **kwargs):

        delete_resp = await self.make_request(
            'DELETE',
            self._webdav_url_ + parse.quote(path.full_path),
            expects=(200, 204),
            throws=exceptions.DeleteError,
            auth=self._auth,
            connector=self.connector
        )
        await delete_resp.release()
        return

    async def metadata(self, path, **kwargs):
        if path.is_dir:
            return (await self._metadata_folder(path, **kwargs))
        else:
            return (await self._metadata_file(path, **kwargs))

    async def _metadata_file(self, path, **kwargs):
        items = await self._metadata_folder(path, skip_first=False, **kwargs)
        return items[0]

    async def _metadata_folder(self, path, skip_first=True, **kwargs):
        response = await self.make_request('PROPFIND',
            self._webdav_url_ + parse.quote(path.full_path),
            expects=(200, 204, 207),
            throws=exceptions.MetadataError,
            auth=self._auth,
            connector=self.connector
        )

        items = []
        if response.status == 207:
            content = await response.content.read()
            items = await utils.parse_dav_response(content, self.folder, skip_first)
        await response.release()
        return items

    async def create_folder(self, path, **kwargs):
        """Create a folder in the current provider at `path`. Returns a `OwnCloudFolderMetadata` object
        if successful.  May throw a 409 Conflict if a directory with the same name already exists.

        :param str path: user-supplied path to create. must be a directory.
        :param boolean precheck_folder: flag to check for folder before attempting create
        :rtype: :class:`waterbutler.core.metadata.BaseFolderMetadata`
        :raises: :class:`waterbutler.core.exceptions.FolderCreationError`
        """

        resp = await self.make_request(
            'MKCOL',
            self._webdav_url_ + parse.quote(self.folder + path.path),
            expects=(200, 201),
            throws=exceptions.CreateFolderError,
            auth=self._auth,
            connector=self.connector
        )
        await resp.release()
        # get the folder metadata
        meta = await self.metadata(path.parent)
        return [m for m in meta if m.path == OwnCloudFolderMetadata('/' + path.path, {}).path][0]

    def can_duplicate_names(self):
        return True

    def can_intra_copy(self, dest_provider, path=None):
        return self is dest_provider

    def can_intra_move(self, dest_provider, path=None):
        return self is dest_provider

    async def do_dav_move_copy(self, src_path, dest_path, operation):
        if operation != 'MOVE' and operation != 'COPY':
            raise NotImplementedError("ownCloud move/copy only supports MOVE and COPY endpoints")

        resp = await self.make_request(
            operation,
            self._webdav_url_ + parse.quote(self.folder + src_path.path),
            expects=(200, 201),
            throws=exceptions.CreateFolderError,
            auth=self._auth,
            connector=self.connector,
            headers={'Destination': parse.quote(self.folder + dest_path.path)}
        )
        return resp

    async def intra_copy(self, dest_provider, src_path, dest_path):
        """
            The quick copy and move commands for ownCloud utilize the COPY and MOVE actions in OCS.
            These actions only work for the same user on the same
        """
        response = await self.do_dav_move_copy(src_path, dest_path, 'COPY')
        content = await response.content.read()
        items = utils.parse_dav_response(content, self.folder)
        if len(items) == 1:
            return items[0], True
        meta = self.metadata(dest_path)
        meta.children = items
        return meta, True

    async def intra_move(self, dest_provider, src_path, dest_path):
        response = await self.do_dav_move_copy(src_path, dest_path, 'MOVE')
        content = await response.content.read()
        items = utils.parse_dav_response(content, self.folder)
        if len(items) == 1:
            return items[0], True
        meta = self.metadata(dest_path)
        meta.children = items
        return meta, True
