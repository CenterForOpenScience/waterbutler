import json
import typing
import asyncio
import logging
from http import HTTPStatus
from urllib import parse as urlparse

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions

from waterbutler.providers.onedrive import settings
from waterbutler.providers.onedrive.path import OneDrivePath
from waterbutler.providers.onedrive.metadata import (OneDriveFileMetadata,
                                                     OneDriveFolderMetadata,
                                                     OneDriveRevisionMetadata)

logger = logging.getLogger(__name__)


class OneDriveProvider(provider.BaseProvider):
    r"""Provider for the Microsoft OneDrive cloud storage service.

    This provider uses **ID-based paths**.

    **Auth:**

    * auth: ``{"name": "username", "email": "username@example.com"}``

    * credentials: ``{"token": "EWaa932BEN32042094DNFWJ40234=="}``

    * settings: ``{"folder": "/foo/", "drive_id": "1a2b3c-4d5e-6f"}``

    **API:**

    * Docs: https://docs.microsoft.com/en-us/onedrive/developer/rest-api/

    **Quirks:**

    * Special characters allowed in file and folder names::

        `~!@#$%^&()-_=+[]{};',

    * Special characters *not* allowed in file and folder names::

        "<>/?*:\|

    * File and folder names may not end with a period.

    """
    NAME = 'onedrive'

    MAX_REVISIONS = 250

    dont_escape_these = ",;[]'$#@&!~()+-_=:/"

    # ========== __init__ ==========

    def __init__(self, auth, credentials, settings, **kwargs):
        logger.debug('__init__ auth::{} settings::{}'.format(auth, settings))
        super().__init__(auth, credentials, settings, **kwargs)
        self.token = self.credentials['token']
        self.folder = self.settings['folder']
        self.drive_id = self.settings['drive_id']

    # ========== properties ==========

    @property
    def default_headers(self) -> dict:
        """Set Authorization header with access token from auth provider.

        API docs: https://docs.microsoft.com/en-us/onedrive/developer/rest-api/getting-started/graph-oauth
        """

        # yep, the lowercase "b" in "bearer" is intentional!  See api link in docstring.
        return {'Authorization': 'bearer {}'.format(self.token)}

    def has_real_root(self) -> bool:
        """Determine if the provider root is the drive root or a subfolder within the drive.

        If the provider root is a subfolder, some extra work will be needed to assure the given
        path is accessible by the user.
        """
        return self.folder == settings.ONEDRIVE_ABSOLUTE_ROOT_ID

    # ========== ro methods ==========

    async def validate_v1_path(self, path: str, **kwargs) -> OneDrivePath:
        r"""validate that ``path`` exists and matches the implicit semantics.

        See `provider.BaseProvider.validate_v1_path` for more.

        API Docs: https://docs.microsoft.com/en-us/onedrive/developer/rest-api/api/driveitem_get

        :param str path: A string representing the requested path. This will be everthing after
                         the provider name in the url.
        :param dict \*\*kwargs: Query parameters and other parameters splatted into the call.
        :raises: NotFoundError
        :rtype: OneDrivePath
        :return: a OneDrivePath object representing the new path.
        """
        logger.debug('validate_v1_path self::{} path::{} kwargs::{}'.format(repr(self),
                                                                            path, kwargs))

        if path == '/':
            return OneDrivePath(path, _ids=[self.folder])

        item_url = self._build_graph_item_url(path)
        logger.debug('item_url::{}'.format(item_url))
        resp = await self.make_request(
            'GET',
            item_url,
            expects=(HTTPStatus.OK, ),
            throws=exceptions.MetadataError
        )
        logger.debug('resp::{}'.format(repr(resp)))
        data = await resp.json()
        logger.debug('response_data::{}'.format(data))

        implicit_folder = path.endswith('/')
        explicit_folder = data.get('folder', None) is not None
        if implicit_folder != explicit_folder:
            raise exceptions.NotFoundError(path)

        base_folder = await self._assert_path_is_under_root(path, path_data=data)
        od_path = OneDrivePath.new_from_response(data, self.folder,
                                                 base_folder_metadata=base_folder)

        logger.debug('od_path.parts::{}'.format(repr(od_path._parts)))
        return od_path

    async def validate_path(self, path: str, **kwargs) -> OneDrivePath:
        logger.debug('validate_path self::{} path::{} kwargs::{}'.format(repr(self), path, kwargs))

        if path == '/':
            return OneDrivePath(path, _ids=[self.folder])

        item_url = self._build_graph_item_url(path)
        logger.debug('item_url::{}'.format(item_url))
        resp = await self.make_request(
            'GET',
            item_url,
            expects=(HTTPStatus.OK, ),
            throws=exceptions.MetadataError
        )
        logger.debug('resp::{}'.format(repr(resp)))
        data = await resp.json()
        logger.debug('response data::{}'.format(data))

        base_folder = await self._assert_path_is_under_root(path, path_data=data)
        od_path = OneDrivePath.new_from_response(data, self.folder,
                                                 base_folder_metadata=base_folder)

        logger.debug('od_path.parts::{}'.format(repr(od_path._parts)))
        return od_path

    async def revalidate_path(self,  # type: ignore
                              base: OneDrivePath,
                              path: str,
                              folder: bool=None) -> OneDrivePath:
        """Take a string file/folder name ``path`` and return a OneDrivePath object
        representing this file under ``base``.

        We hit the ``/$parent_id/children`` endpoint instead of the
        ``/$parent_id/?expand=children`` endpoint b/c the parent has already been validated and we
        don't need its metadata.
        """
        logger.debug('revalidate_path base::{} path::{} base.id::{} '
                     'folder::{}'.format(base, path, base.identifier, folder))

        assert isinstance(base, OneDrivePath), 'Base path should be validated'
        assert base.identifier, 'Base path should be validated'
        resp = await self.make_request(
            'GET',
            self._build_graph_item_url(base.identifier, 'children'),
            expects=(HTTPStatus.OK, HTTPStatus.NOT_FOUND),
            throws=exceptions.MetadataError
        )

        # Prior request is for the contents of the folder `base`.  We now need to search to see if
        # we can find `path` within the contents of `base`.  `revalidate_path` supports both extant
        # and non-extant paths.  If either `base` or `path` does not exist, then return a putative
        # path with the appropriate path parts but without the not-yet-created identifiers.
        path_id = None
        is_folder = folder
        if resp.status != HTTPStatus.NOT_FOUND:
            data = await resp.json()
            for child in data['value']:
                child_is_folder = 'folder' in child
                if (child['name'] == path) and (folder == child_is_folder):
                    path_id = child['id']
                    is_folder = child_is_folder
                    break

        return base.child(path, _id=path_id, folder=is_folder)

    async def metadata(self, path: OneDrivePath, **kwargs):  # type: ignore
        """Fetch metadata for the file or folder identified by ``path``.

        API docs: https://docs.microsoft.com/en-us/onedrive/developer/rest-api/api/driveitem_get

        :param OneDrivePath path: the file or folder to fetch metadata for
        :rtype: OneDriveMetadata
        :rtype: list(OneDriveFileMetadata|OneDriveFolderMetadata)
        :return: either a OneDriveFileMetada for a single file or an array of either
            `OneDriveFileMetadata` or `OneDriveFolderMetadata` objects
        """
        logger.debug('identifier::{} path::{}'.format(path.identifier, path))

        if path.identifier is None:  # TESTME
            raise exceptions.NotFoundError(str(path))

        url = self._build_graph_item_url(path.identifier, expand='children')
        logger.debug('url::{}'.format(repr(url)))
        resp = await self.make_request(
            'GET',
            url,
            expects=(HTTPStatus.OK, ),
            throws=exceptions.MetadataError
        )
        logger.debug('resp::{}'.format(repr(resp)))
        data = await resp.json()
        logger.debug('data::{}'.format(data))
        return self._construct_metadata(data, path)

    async def revisions(self,  # type: ignore
                        path: OneDrivePath,
                        **kwargs) -> typing.List[OneDriveRevisionMetadata]:
        """Get a list of revisions for the file identified by ``path``.

        API docs: https://docs.microsoft.com/en-us/onedrive/developer/rest-api/api/driveitem_delta

        :param OneDrivePath path: the file to get revisions for
        :rtype: list(OneDriveRevisionMetadata)
        :return: a list of `OneDriveRevisionMetadata` objects
        """
        logger.debug('revisions path::{} path.id::{} kwargs::{}'.format(path, path.identifier,
                                                                        kwargs))
        data = await self._revisions_json(path, **kwargs)

        return [
            OneDriveRevisionMetadata(item)
            for item in data['value']
        ]

    async def download(self,  # type: ignore
                       path: OneDrivePath,
                       revision: str=None,
                       range: typing.Tuple[int, int]=None,
                       **kwargs) -> streams.ResponseStreamReader:
        r"""Download the file identified by ``path``.  If ``revision`` is not ``None``, get
        the file at the version identified by ``revision``.

        API docs: https://docs.microsoft.com/en-us/onedrive/developer/rest-api/api/driveitem_get_content

        :param str path: The path to the file on OneDrive
        :param str revision: The revision of the file to download. If ``None``, download latest.
        :param dict \*\*kwargs: Ignored
        :raises: :class:`waterbutler.core.exceptions.DownloadError`
        :rtype: waterbutler.core.streams.ResponseStreamReader
        :return: a stream of the contents of the file
        """
        logger.debug('path::{} path.identifier::{} revision::{} range::{} '
                     'kwargs::{}'.format(path, path.identifier, revision, range, kwargs))

        if path.identifier is None:
            raise exceptions.DownloadError('"{}" not found'.format(str(path)),
                                           code=HTTPStatus.NOT_FOUND)

        download_url = None
        if revision:
            items = await self._revisions_json(path)
            for item in items['value']:
                if item['id'] == revision:
                    try:
                        download_url = item['@content.downloadUrl']
                    except KeyError:
                        raise exceptions.UnexportableFileTypeError(str(path))
                    break
        else:
            # TODO: we should be able to get the download url from validate_v1_path
            metadata_url = self._build_graph_item_url(path.identifier)
            logger.debug('metadata_url to get download path: url::{}'.format(metadata_url))
            metadata_resp = await self.make_request(
                'GET',
                metadata_url,
                expects=(HTTPStatus.OK, ),
                throws=exceptions.MetadataError
            )
            metadata = await metadata_resp.json()

            # If file is a OneNote file, we can't download it and must throw an error instead. If
            # file is an unknown type, let's just hope for the best.
            package_type = None
            try:
                package_type = metadata['package']['type']
            except KeyError:
                pass
            else:
                if package_type == 'oneNote':
                    raise exceptions.UnexportableFileTypeError(str(path))

            download_url = metadata.get('@microsoft.graph.downloadUrl', None)

        logger.debug('download_url::{}'.format(download_url))
        if download_url is None:
            raise exceptions.NotFoundError(str(path))

        download_resp = await self.make_request(
            'GET',
            download_url,
            range=range,
            expects=(HTTPStatus.OK, HTTPStatus.PARTIAL_CONTENT),
            headers={'accept-encoding': ''},
            throws=exceptions.DownloadError,
        )
        logger.debug('download_resp::{}'.format(repr(download_resp)))

        return streams.ResponseStreamReader(download_resp)

    def can_duplicate_names(self) -> bool:
        return False

    def shares_storage_root(self, other: provider.BaseProvider) -> bool:
        """OneDrive settings include the root folder id, which is unique across projects for
        subfolders. But the root folder of a Personal OD account always has an ID of 'root'.  This
        means that the root folders of two separate OneDrive accounts would incorrectly test as
        being the same storage root. Add a comparison of credentials to avoid this."""
        return super().shares_storage_root(other) and self.credentials == other.credentials

    def can_intra_move(self, other, path=None) -> bool:
        return self == other

    def can_intra_copy(self, other, path=None) -> bool:
        return self == other

    # ========== rw methods ==========

    async def upload(self, stream, path, conflict='replace', **kwargs):
        path, exists = await self.handle_name_conflict(path, conflict=conflict)
        if stream.size > settings.ONEDRIVE_CHUNKED_UPLOAD_FILE_SIZE:
            return await self._chunked_upload(stream, path, exists)

        return await self._contiguous_upload(stream, path, exists)

    async def create_folder(self, path: OneDrivePath, folder_precheck: bool=True,
                            **kwargs) -> OneDriveFolderMetadata:
        """Create the folder defined by ``path``.

        API docs: https://docs.microsoft.com/en-us/onedrive/developer/rest-api/api/driveitem_post_children

        :param OneDrivePath path: The path to create a folder at
        :rtype: `OneDriveFolderMetadata`
        :return: a `OneDriveFolderMetadata` object representing the new folder
        """
        OneDrivePath.validate_folder(path)

        if folder_precheck:
            if path.identifier is not None:
                raise exceptions.FolderNamingConflict(path.name)

        # upload_url should be like: /drives/items/F4D50E400DFE7D4E!105/children
        upload_url = self._build_graph_item_url(path.parent.identifier, 'children')
        logger.debug('upload url:{} path:{} folderName:{}'.format(upload_url, repr(path),
                                                                  repr(path.name)))

        payload = {'name': path.name, 'folder': {}}
        resp = await self.make_request(
            'POST',
            upload_url,
            data=json.dumps(payload),
            headers={'content-type': 'application/json'},
            expects=(HTTPStatus.CREATED,),
            throws=exceptions.CreateFolderError,
        )

        data = await resp.json()
        logger.debug('upload_data:{}'.format(data))
        # save new folder's id into the WaterButlerPath object. logs will need it later.
        path._parts[-1]._id = data['id']
        return OneDriveFolderMetadata(data, path)

    async def delete(self, path, confirm_delete=0, **kwargs):
        """Delete a file or folder from OneDrive.

        API docs: https://docs.microsoft.com/en-us/onedrive/developer/rest-api/api/driveitem_delete

        :param path OneDrivePath: a OneDrivePath representing the path to be deleted
        :return: None
        """

        # copy or move automatically try to delete all directories for 'conflict' = 'replace'
        if not path.identifier:
            raise exceptions.NotFoundError(str(path))

        if path.is_root:
            if confirm_delete == 1:
                await self._delete_folder_contents(path)
                return

            raise exceptions.DeleteError(
                'confirm_delete=1 is required for deleting root provider folder',
                code=HTTPStatus.BAD_REQUEST,
            )

        resp = await self.make_request(
            'DELETE',
            self._build_graph_item_url(path.identifier),
            data={},
            expects=(HTTPStatus.NO_CONTENT,),
            throws=exceptions.DeleteError,
        )
        await resp.release()

    # async def copy():  use default implementation

    # async def move():  use default implementation

    async def intra_copy(self, dest_provider, src_path, dest_path):
        """Copy a file or folder within a OneDrive provider.

        API docs: https://docs.microsoft.com/en-us/onedrive/developer/rest-api/api/driveitem_copy
        """

        dest_exist = dest_path.identifier is not None

        if dest_path.parent.is_root and self.has_real_root():
            parent_reference = {
                'path': '/drive/root:/'
            }
        else:
            parent_reference = {
                'id': dest_path.parent.identifier
            }

        payload = {'name': dest_path.name,
                   'parentReference': parent_reference}
        url = self._build_graph_item_url(src_path.identifier, 'copy')

        logger.debug('intra_copy dest_provider::{} src_path::{} '
                     'dest_path::{} url::{} payload::{}'.format(repr(dest_provider), repr(src_path),
                                                                repr(dest_path), repr(url), payload))
        resp = await self.make_request(
            'POST',
            url,
            data=json.dumps(payload),
            headers={'content-type': 'application/json', 'Prefer': 'respond-async'},
            expects=(HTTPStatus.ACCEPTED,),
            throws=exceptions.IntraCopyError,
        )
        logger.debug('resp::{}'.format(repr(resp)))
        status_url = resp.headers['LOCATION']
        logger.debug('status_url::{}'.format(repr(status_url)))
        await resp.release()

        try:
            # OATHPIT INSPECT:
            data = await asyncio.wait_for(
                self._wait_for_async_job(status_url, throws=exceptions.IntraCopyError),
                settings.ONEDRIVE_COPY_REQUEST_TIMEOUT or None
            )
        except asyncio.TimeoutError:
            raise exceptions.CopyError("OneDrive API file copy has not responded in a timely "
                                       "manner. Please wait for 1-2 minutes, then query for "
                                       "the file to see if the copy has completed",
                                       code=HTTPStatus.ACCEPTED)

        base_folder = await self._assert_path_is_under_root(dest_path, path_data=data)
        final_path = OneDrivePath.new_from_response(data, self.folder,
                                                    base_folder_metadata=base_folder)

        return self._intra_move_copy_metadata(data, final_path), not dest_exist

    async def intra_move(self, dest_provider, src_path, dest_path):
        """Move/rename a file or folder within a OneDrive provider.

        API docs: https://docs.microsoft.com/en-us/onedrive/developer/rest-api/api/driveitem_move
        """

        dest_exist = dest_path.identifier is not None

        if dest_exist:
            await dest_provider.delete(dest_path)

        if dest_path.parent.is_root and self.has_real_root():
            parent_reference = {'path': '/drive/root:/'}
        else:
            parent_reference = {'id': dest_path.parent.identifier}

        url = self._build_graph_item_url(src_path.identifier)
        payload = {'name': dest_path.name,
                   'parentReference': parent_reference}

        logger.debug('intra_move dest_path::{} src_path::{} '
                     'url::{} payload:{}'.format(str(dest_path.parent.identifier),
                                                 repr(src_path), url, payload))

        resp = await self.make_request(
            'PATCH',
            url,
            data=json.dumps(payload),
            headers={'content-type': 'application/json'},
            expects=(HTTPStatus.OK,),
            throws=exceptions.IntraMoveError,
        )

        data = await resp.json()

        base_folder = await self._assert_path_is_under_root(dest_path, path_data=data)
        final_path = OneDrivePath.new_from_response(data, self.folder,
                                                    base_folder_metadata=base_folder)

        return self._intra_move_copy_metadata(data, final_path), not dest_exist

    # ========== utility methods ==========

    def _build_graph_url(self, *segments, **query) -> str:
        return provider.build_url(settings.BASE_GRAPH_URL, *segments, **query)

    def _build_graph_drive_url(self, *segments, **query) -> str:
        return self._build_graph_url('drives', self.drive_id, *segments, **query)

    def _build_graph_item_url(self, *segments, **query) -> str:
        return self._build_graph_drive_url('items', *segments, **query)

    def _construct_metadata(self, data: dict, path):
        """Take a file/folder metadata response from OneDrive and a path object representing the
        queried path and return a `OneDriveFileMetadata` object if the repsonse represents a file
        or a list of `OneDriveFileMetadata` and `OneDriveFolderMetadata` objects if the response
        represents a folder. """
        if 'folder' in data.keys():
            ret = []
            if 'children' in data.keys():
                for item in data['children']:
                    is_folder = 'folder' in item.keys()
                    child_path = path.child(item['name'], _id=item['id'], folder=is_folder)
                    if is_folder:
                        ret.append(OneDriveFolderMetadata(item, child_path))  # type: ignore
                    else:
                        ret.append(OneDriveFileMetadata(item, child_path))  # type: ignore
            return ret

        return OneDriveFileMetadata(data, path)

    def _intra_move_copy_metadata(self, data: dict, path):
        """Return appropriate metadata from intra_copy/intra_move actions. If `data` represents
        a folder, will fetch and include `data`'s children.
        """
        path.parts[-1]._id = data['id']
        if 'folder' not in data.keys():
            return OneDriveFileMetadata(data, path)

        folder = OneDriveFileMetadata(data, path)
        folder._children = self._construct_metadata(data, path)  # type: ignore
        return folder

    async def _assert_path_is_under_root(self, path, path_data, **kwargs) -> dict:
        # If base folder isn't root or the immediate parent of the requested path, then we need
        # to verify that it actually is an ancestor of path.  Otherwise, a malicious user could
        # try to get access to a file outside of the configured root.
        base_folder = None
        if not self.has_real_root() and self.folder != path_data['parentReference']['id']:
            base_folder_resp = await self.make_request(
                'GET', self._build_graph_item_url(self.folder),
                expects=(HTTPStatus.OK, ),
                throws=exceptions.MetadataError
            )
            base_folder = await base_folder_resp.json()

            base_path_quoted = urlparse.quote(
                '{}/{}/'.format(
                    urlparse.unquote(base_folder['parentReference']['path']),
                    base_folder['name']
                ),
                self.dont_escape_these
            )

            parent_path_quoted = urlparse.quote('{}/'.format(
                urlparse.unquote(path_data['parentReference']['path'])
            ), self.dont_escape_these)

            if not parent_path_quoted.startswith(base_path_quoted):
                # the requested file is NOT a child of self.folder
                raise exceptions.NotFoundError(path)

        return base_folder

    async def _revisions_json(self, path: OneDrivePath, **kwargs) -> dict:
        """Fetch a list of revisions for the file at ``path``.

        API docs: https://docs.microsoft.com/en-us/onedrive/developer/rest-api/resources/driveitemversion

        :param OneDrivePath path: the path of the file to get revisions for
        :rtype: dict
        :return: list of revision metadata under a ``value`` key
        """
        revisions_url = self._build_graph_item_url(path.identifier, 'versions')
        logger.debug('revisions_url::{}'.format(repr(revisions_url)))
        try:
            resp = await self.make_request(
                'GET',
                revisions_url,
                expects=(HTTPStatus.OK, ),
                throws=exceptions.RevisionsError
            )
        except exceptions.RevisionsError as exc:
            if (
                exc.code == HTTPStatus.METHOD_NOT_ALLOWED and
                exc.data['error']['message'] == 'Item does not match expected type'
            ):
                # OneNote versioning not supported, instead return a lone revision called 'current'
                url = self._build_graph_item_url(path.identifier)
                logger.debug('revision_unsupported url::{}'.format(url))
                resp = await self.make_request(
                    'GET',
                    url,
                    expects=(HTTPStatus.OK,),
                    throws=exceptions.MetadataError
                )
                logger.debug('revision_unsupported resp::{}'.format(repr(resp)))
                data = await resp.json()
                logger.debug('revision_unsupported data::{}'.format(data))
                return {'value': [
                    {'id': 'current', 'lastModifiedDateTime': data['lastModifiedDateTime']},
                ]}  # fudge a fake revision response
            else:
                raise exc

        logger.debug('resp::{}'.format(repr(resp)))
        data = await resp.json()
        logger.debug('data::{}'.format(json.dumps(data)))

        return data

    async def _contiguous_upload(self, stream, path, exists):
        """Upload a file in a single request.  Subject to OneDrive's imposed limits.

        API docs: https://docs.microsoft.com/en-us/onedrive/developer/rest-api/api/driveitem_put_content

        Limited to 4MB file upload.
        """
        upload_url, expects = None, None
        if exists:
            upload_url = self._build_graph_item_url(path.identifier, 'content')
            expects = (HTTPStatus.OK,)
        else:
            upload_url = self._build_graph_item_url('{}:'.format(path.parent.identifier),
                                                    '{}:'.format(path.name), 'content')
            expects = (HTTPStatus.CREATED,)

        logger.debug('upload url::{}'.format(upload_url))
        resp = await self.make_request(
            'PUT',
            upload_url,
            headers={'Content-Length': str(stream.size)},
            data=stream,
            expects=expects,
            throws=exceptions.UploadError,
        )
        data = await resp.json()

        base_folder = await self._assert_path_is_under_root(path, path_data=data)
        new_path = OneDrivePath.new_from_response(data, self.folder,
                                                  base_folder_metadata=base_folder)

        return OneDriveFileMetadata(data, new_path), not exists

    async def _chunked_upload(self, stream, path, exists):
        """Upload a file over multiple requests.  Require if file size is greater than OneDrive's
        imposed limits.

        API docs: https://docs.microsoft.com/en-us/onedrive/developer/rest-api/api/driveitem_createuploadsession

        Process: create a new upload session, then upload chunks of file until stream is exhausted
        """
        upload_url = await self._chunked_upload_create_session(path)
        logger.debug('upload_url::{}'.format(upload_url))
        try:
            data = await self._chunked_upload_stream(upload_url, stream)
        except exceptions.UploadError as exc:
            await self.make_request(
                'DELETE',
                upload_url,
                expects=None
            )
            raise exc
        return OneDriveFileMetadata(data, path), not exists

    async def _chunked_upload_create_session(self, path):
        """Start an upload session to create a temp storage location to save data over multiple
        requests. Returns a body that includes a url to upload chunks to and an expiration time.

        API docs: https://docs.microsoft.com/en-us/onedrive/developer/rest-api/api/driveitem_createuploadsession#create-an-upload-session
        """
        create_session_url = self._build_graph_item_url('{}:'.format(path.parent.identifier),
                                                        '{}:'.format(path.name),
                                                        'createUploadSession')
        logger.debug('create_session_url::{}'.format(create_session_url))
        payload = {
            'item': {
                'name': path.name
            }
        }

        resp = await self.make_request(
            'POST',
            create_session_url,
            data=json.dumps(payload),
            headers={'content-type': 'application/json'},
            expects=(HTTPStatus.OK,),
            throws=exceptions.UploadError
        )
        data = await resp.json()
        return data['uploadUrl']

    async def _chunked_upload_stream(self, upload_url, stream):
        """Send chunks of the file intended to be uploaded to OD one-at-a-time.  `upload_url` is a
        temporary upload url provided by OD.  Read `chunk_size` bytes from the given stream, then
        send them to the OD dest url.

        API docs: https://docs.microsoft.com/en-us/onedrive/developer/rest-api/api/driveitem_createuploadsession#upload-bytes-to-the-upload-session
        """
        result = missed_chunks = None

        start_range = 0
        total_size = stream.size

        while not stream.at_eof():
            data = await stream.read(settings.ONEDRIVE_CHUNKED_UPLOAD_CHUNK_SIZE)
            if data or not result:
                # sometimes we have to make last upload call to commit file
                missed_chunks, result = await self._chunked_upload_stream_by_range(
                    upload_url,
                    data,
                    start_range=start_range,
                    total_size=total_size,
                )
                start_range += len(data)

        if missed_chunks or not result:
            raise exceptions.UploadError("OneDrive API upload request failed. Please repeat the "
                                         "upload request.", code=HTTPStatus.BAD_REQUEST)

        return result

    async def _chunked_upload_stream_by_range(self, upload_url, data, start_range=0,
                                              total_size=0):
        """Send a chunk of data to OD's chunked upload endpoint.  The chunk is contained in `data`
        and bookkeeping information is provided by `start_range` and `total_size`.

        API docs: https://docs.microsoft.com/en-us/onedrive/developer/rest-api/api/driveitem_createuploadsession#upload-bytes-to-the-upload-session

        :param str upload_url: OneDrive file upload url
        :param bytes data: file chunk
        :param int start_range:
        :param int total_size:
        :return: Return two arguments - next expected file chunk or result file metadata
        """
        resp = await self.make_request(
            'PUT',
            upload_url,
            data=data,
            headers={
                'Content-Length': str(len(data)),
                'Content-Range': self._build_range_header_for_upload(start_range,
                                                                     start_range + len(data) - 1,
                                                                     total_size)
            },
            expects=(HTTPStatus.ACCEPTED, HTTPStatus.CREATED),
            throws=exceptions.UploadError
        )
        data = await resp.json()
        if resp.status == HTTPStatus.CREATED:
            return None, data
        return data.get('nextExpectedRanges'), None

    def _build_range_header_for_upload(self, start, end, total):
        if end >= total:
            end = None
        return 'bytes {}-{}/{}'.format(
            '' if start is None else start,
            '' if end is None else end,
            total
        )

    async def _wait_for_async_job(self, url, throws=exceptions.ProviderError):
        """Followup on 202 responses (used by copy action).

        API docs: https://docs.microsoft.com/en-us/onedrive/developer/rest-api/concepts/long-running-actions

        :param str url: Status URL for Async Operation
        :param Exception throws: The exception to be raised from expects
        :return dict: Response json content
        """
        counter = 0
        while True:
            counter += 1
            logger.debug('await_job::{} url::{}'.format(counter, url))
            resp = await self.make_request(
                'GET',
                url,
                allow_redirects=False,
            )
            logger.debug('await_job::{} resp::{}'.format(counter, resp))
            if resp.status == HTTPStatus.SEE_OTHER:
                new_item_id = resp.headers['Location'].split('/')[-1]
                followup_url = self._build_graph_item_url(new_item_id)
                logger.debug('await_job:{} new_item_id::{} '
                             'followup_url::{}'.format(counter, new_item_id, followup_url))
                followup_resp = await self.make_request(
                    'GET',
                    followup_url,
                    allow_redirects=False,
                )
                logger.debug('await_job::{} followup_resp::{}'.format(counter, followup_resp))
                return await followup_resp.json()

            await resp.release()
            await asyncio.sleep(settings.ONEDRIVE_ASYNC_REQUEST_SLEEP_INTERVAL)
            logger.debug('await_job::{} No luck this loop, but trying again'.format(counter))

    async def _delete_folder_contents(self, path: OneDrivePath, **kwargs) -> None:
        """Delete the contents of a folder. For use against provider root.

        :param OneDrivePath path: OneDrivePath object for folder
        """
        children = await self.metadata(path)
        for child in children:  # type: ignore
            onedrive_path = await self.validate_path(child.path)
            await self.delete(onedrive_path)
