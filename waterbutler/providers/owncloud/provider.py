import aiohttp

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.owncloud import utils
from waterbutler.providers.owncloud.metadata import OwnCloudFileRevisionMetadata


class OwnCloudProvider(provider.BaseProvider):
    """
        Provider for the ownCloud cloud storage service.

        This provider uses the OCS1.7 standard for communication

        API docs: https://www.freedesktop.org/wiki/Specifications/open-collaboration-services-1.7/

        Required settings fields:
            * folder
            * verify_ssl

        Required credentials fields:
            * host
            * username
            * password

        Quirks:

            * User credentials are stored in a aiohttp.BasicAuth object. At the
            moment, there isn't a better way to do this.

            * Intra_move and Intra_copy fail at make_request when run inside of a
            celery worker with a bland
            RunTimeException("Non-thread-safe operation invoked on an event loop other than the current one").
            This has been "solved" by using the `is` keyword in can_intra_move/copy.
    """
    NAME = 'owncloud'

    def __init__(self, auth, credentials, settings):
        super().__init__(auth, credentials, settings)

        self.folder = settings['folder']
        self.verify_ssl = settings['verify_ssl']
        self.url = credentials['host']
        self._auth = aiohttp.BasicAuth(
            credentials['username'],
            credentials['password']
        )

    def connector(self):
        return aiohttp.TCPConnector(verify_ssl=self.verify_ssl)

    @property
    def _webdav_url_(self):
        """
            Formats the outgoing url appropriately. This accounts for some differences
            in oc server software.
        """
        if self.url[-1] != '/':
            return self.url + '/remote.php/webdav/'
        return self.url + 'remote.php/webdav/'

    def shares_storage_root(self, other):
        """Owncloud settings only include the root folder. If a cross-resource move occurs
        between two owncloud providers that are on different accounts but have the same folder
        base name, the parent method could incorrectly think the action is a self-overwrite.
        Comparing credentials means that this is unique per connected account."""
        return super().shares_storage_root(other) and self.credentials == other.credentials

    async def validate_v1_path(self, path, **kwargs):
        """
            Verifies if a path exists and if so, returns a waterbutler path object.
            WebDAV returns 200 for a single file, 207 for a multipart (folder) and
            404 for DNE.

            :param str path: user-supplied path to validate
            :returns :class:`waterbutler.core.path.WaterButlerPath`: WaterButlerPath representation of path
            :raises: :class:`waterbutler.core.exceptions.NotFoundError`
        """
        if path == '/':
            return WaterButlerPath(path, prepend=self.folder)
        full_path = WaterButlerPath(path, prepend=self.folder)

        response = await self.make_request('PROPFIND',
            self._webdav_url_ + full_path.full_path,
            expects=(200, 207, 404),
            throws=exceptions.MetadataError,
            auth=self._auth,
            connector=self.connector(),
        )
        content = await response.content.read()
        await response.release()
        if response.status == 404:
            raise exceptions.NotFoundError(str(full_path.full_path))

        try:
            item = await utils.parse_dav_response(content, '/')
        except exceptions.NotFoundError:
            # Re-raise with the proper path
            raise exceptions.NotFoundError(str(full_path.full_path))
        if full_path.kind != item[0].kind:
            raise exceptions.NotFoundError(full_path.full_path)
        return full_path

    async def validate_path(self, path, **kwargs):
        """
            The primary difference between `validate_path` and `validate_v1_path`
            is that the 404 is not raised here in case the file is not found,
            which is the case for paths checked before uploads.
        """
        if path == '/':
            return WaterButlerPath(path, prepend=self.folder)
        full_path = WaterButlerPath(path, prepend=self.folder)
        response = await self.make_request('PROPFIND',
            self._webdav_url_ + full_path.full_path,
            expects=(200, 207, 404),
            throws=exceptions.MetadataError,
            auth=self._auth,
            connector=self.connector(),
        )
        content = await response.content.read()
        await response.release()

        try:
            await utils.parse_dav_response(content, '/')
        except exceptions.NotFoundError:
            pass
        return full_path

    async def download(self, path, accept_url=False, range=None, **kwargs):
        """
            Creates a stream for downloading files from the remote host.
            If the metadata query for the file has no size metadata, downloads
            to memory.

            :param str path: user-supplied path to download.
            :raises: :class:`waterbutler.core.exceptions.UploadError`
        """
        download_resp = await self.make_request(
            'GET',
            self._webdav_url_ + path.full_path,
            range=range,
            expects=(200, 206,),
            throws=exceptions.DownloadError,
            auth=self._auth,
            connector=self.connector(),
        )
        return streams.ResponseStreamReader(download_resp)

    async def upload(self, stream, path, conflict='replace', **kwargs):
        """
            Utilizes default name conflict handling behavior then adds the
            appropriate headers and creates the upload request.

            :param str path: user-supplied path to upload.
            :raises: :class:`waterbutler.core.exceptions.UploadError`
        """
        if path.identifier and conflict == 'keep':
            path, _ = await self.handle_name_conflict(path, conflict=conflict, kind='folder')
            path._parts[-1]._id = None

        response = await self.make_request(
            'PUT',
            self._webdav_url_ + path.full_path,
            data=stream,
            headers={'Content-Length': str(stream.size)},
            expects=(201, 204,),
            throws=exceptions.UploadError,
            auth=self._auth,
            connector=self.connector(),
        )
        await response.release()
        meta = await self.metadata(path)
        return meta, response.status == 201

    async def delete(self, path, **kwargs):
        """
            Deletes path on remote host

            :param str path: user-supplied path to delete.
            :raises: :class:`waterbutler.core.exceptions.DeleteError`
        """

        delete_resp = await self.make_request(
            'DELETE',
            self._webdav_url_ + path.full_path,
            expects=(204,),
            throws=exceptions.DeleteError,
            auth=self._auth,
            connector=self.connector(),
        )
        await delete_resp.release()
        return

    async def metadata(self, path, **kwargs):
        """
            Queries the remote host for metadata and returns metadata objects
            based on the return value.

            :param str path: user-supplied path to query.
            :raises: :class:`waterbutler.core.exceptions.MetadataError`
        """
        if path.is_dir:
            return (await self._metadata_folder(path, **kwargs))
        else:
            return (await self._metadata_file(path, **kwargs))

    async def _metadata_file(self, path, **kwargs):

        items = await self._metadata_folder(path, skip_first=False, **kwargs)
        return items[0]

    async def _metadata_folder(self, path, skip_first=True, **kwargs):
        """
            Performs the actual query against oC. In this case the return code depends
            on the content:

            * 204: Empty response
            * 207: Multipart response
        """
        response = await self.make_request('PROPFIND',
            self._webdav_url_ + path.full_path,
            expects=(204, 207),
            throws=exceptions.MetadataError,
            auth=self._auth,
            connector=self.connector(),
        )

        items = []
        if response.status == 207:
            content = await response.content.read()
            items = await utils.parse_dav_response(content, self.folder, skip_first)
        await response.release()
        return items

    async def create_folder(self, path, **kwargs):
        """
            Create a folder in the current provider at `path`.
            Returns a `OwnCloudFolderMetadata` object
            if successful.

            :param str path: user-supplied path to create. must be a directory.
            :param boolean precheck_folder: flag to check for folder before attempting create
            :rtype: :class:`waterbutler.core.metadata.BaseFolderMetadata`
            :raises: :class:`waterbutler.core.exceptions.FolderCreationError`
        """
        resp = await self.make_request(
            'MKCOL',
            self._webdav_url_ + self.folder + path.path,
            expects=(201, 405),
            throws=exceptions.CreateFolderError,
            auth=self._auth,
            connector=self.connector()
        )
        await resp.release()
        if resp.status == 405:
            raise exceptions.FolderNamingConflict(path)
        # get the folder metadata
        meta = await self.metadata(path.parent)
        return [m for m in meta if m.path == path.materialized_path][0]

    def can_duplicate_names(self):
        return True

    def can_intra_copy(self, dest_provider, path=None):
        return self is dest_provider

    def can_intra_move(self, dest_provider, path=None):
        return self is dest_provider

    async def intra_copy(self, dest_provider, src_path, dest_path):
        return await self._do_dav_move_copy(src_path, dest_path, 'COPY')

    async def intra_move(self, dest_provider, src_path, dest_path):
        return await self._do_dav_move_copy(src_path, dest_path, 'MOVE')

    async def _do_dav_move_copy(self, src_path, dest_path, operation):
        """
            Performs a quick copy or move operation on the remote host.

            :param str src_path: user-supplied path to the source object
            :param str dest_path: user-supplied path to the destination object
            :param str operation: Either `COPY` or `MOVE`
            :rtype: :class:`waterbutler.core.waterbutler.metadata.OwnCloudFileMetadata`
            :raises: :class:`waterbutler.core.exceptions.IntraCopyError`
        """
        if operation != 'MOVE' and operation != 'COPY':
            raise NotImplementedError("ownCloud move/copy only supports MOVE and COPY endpoints")

        resp = await self.make_request(
            operation,
            self._webdav_url_ + self.folder + src_path.path,
            expects=(200, 201),
            throws=exceptions.IntraCopyError,
            auth=self._auth,
            connector=self.connector(),
            headers={'Destination': '/remote.php/webdav' + dest_path.full_path}
        )
        content = await resp.content.read()
        if content:
            items = await utils.parse_dav_response(content, self.folder)
            if len(items) == 1:
                return items[0], True

        meta = await self.metadata(dest_path)
        return meta, resp.status == 200

    async def revisions(self, path, **kwargs):
        metadata = await self.metadata(path)
        return [OwnCloudFileRevisionMetadata.from_metadata(metadata)]
