import json
import typing
import logging
from asyncio import sleep
from http import HTTPStatus
from urllib import parse as urlparse
from typing import Tuple

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath
from waterbutler.core.streams import CutoffStream

from waterbutler.providers.onedrive import settings
from waterbutler.providers.onedrive.path import OneDrivePath
from waterbutler.providers.onedrive.metadata import (OneDriveFileMetadata,
                                                     OneDriveFolderMetadata,
                                                     OneDriveRevisionMetadata, BaseOneDriveMetadata)

logger = logging.getLogger(__name__)


class OneDriveProvider(provider.BaseProvider):
    r"""Provider for the Microsoft OneDrive cloud storage service.

    This provider uses **ID-based paths**.

    Special drives: https://docs.microsoft.com/en-us/graph/api/resources/driveitem?view=graph-rest-1.0

    **Auth:**

    * auth: ``{"name": "username", "email": "username@example.com"}``

    * credentials: ``{"token": "EWaa932BEN32042094DNFWJ40234=="}``

    * settings: ``{"folder": "/foo/"}``

    **API:**

    * Docs:  https://docs.microsoft.com/en-us/graph/api/resources/onedrive?view=graph-rest-1.0

    * Get folder contents:  If folder is root, api path is ``/drive/root/children``. If folder
      is not root, api path is ``/drive/items/$item-id/children``.
      This provider used the OneDrive API, but now it uses the Microsoft Graph API. These APIs are mostly compatible.

    **Quirks:**

    * Special characters allowed in file and folder names::

        `~!@#$%^&()-_=+[]{};',

    * Special characters *not* allowed in file and folder names::

        "<>/?*:\|

    * File and folder names may not end with a period.

    """
    NAME = 'onedrive'
    BASE_URL = settings.BASE_URL

    MAX_REVISIONS = 250

    dont_escape_these = ",;[]'$#@&!~()+-_=:/"

    # ========== __init__ ==========

    def __init__(self, auth, credentials, settings, **kwargs):
        logger.debug('__init__ auth::{} settings::{}'.format(auth, settings))
        super().__init__(auth, credentials, settings, **kwargs)
        self.token = self.credentials['token']
        self.folder = self.settings['folder']

    # ========== properties ==========

    @property
    def default_headers(self) -> dict:
        """Set Authorization header with access token from auth provider.

        API docs: https://docs.microsoft.com/en-us/graph/auth/auth-concepts?view=graph-rest-1.0
        """
        return {'Authorization': 'bearer {}'.format(self.token)}

    # ========== methods ==========

    async def validate_v1_path(self, path: str, **kwargs) -> OneDrivePath:
        r"""validate that ``path`` exists and matches the implicit semantics.

        See `provider.BaseProvider.validate_v1_path` for more.

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

        resp = await self.make_request(
            'GET', self._build_item_url(path),
            expects=(200, ),
            throws=exceptions.MetadataError
        )
        logger.debug('validate_v1_path resp::{}'.format(repr(resp)))
        data = await resp.json()
        logger.debug('validate_v1_path data::{}'.format(json.dumps(data)))

        implicit_folder = path.endswith('/')
        explicit_folder = data.get('folder', None) is not None
        if implicit_folder != explicit_folder:
            raise exceptions.NotFoundError(path)

        # If base folder isn't root or the immediate parent of the requested path, then we need
        # to verify that it actually is an ancestor of path.  Otherwise, a malicious user could
        # try to get access to a file outside of the configured root.
        base_folder = None
        if self.folder != 'root' and self.folder != data['parentReference']['id']:
            base_folder_resp = await self.make_request(
                'GET', self._build_item_url(self.folder),
                expects=(200, ),
                throws=exceptions.MetadataError
            )
            logger.debug('validate_v1_path base_folder_resp::{}'.format(repr(base_folder_resp)))
            base_folder = await base_folder_resp.json()
            logger.debug('validate_v1_path base_folder::{}'.format(json.dumps(base_folder)))

            base_full_path = urlparse.quote(
                '{}/{}/'.format(
                    urlparse.unquote(base_folder['parentReference']['path']),
                    base_folder['name']
                ),
                self.dont_escape_these
            )

            if not data['parentReference']['path'].startswith(base_full_path):
                # the requested file is NOT a child of self.folder
                raise exceptions.NotFoundError(path)

        od_path = OneDrivePath.new_from_response(data, self.folder,
                                                 base_folder_metadata=base_folder)
        logger.debug('validate_v1_path od_path.parts::{}'.format(repr(od_path._parts)))
        return od_path

    async def validate_path(self, path: str, **kwargs) -> OneDrivePath:
        logger.debug('validate_path self::{} path::{} kwargs::{}'.format(repr(self), path, kwargs))

        if path == '/':
            return OneDrivePath(path, _ids=[self.folder])

        resp = await self.make_request(
            'GET', self._build_item_url(path),
            expects=(200, ),
            throws=exceptions.MetadataError
        )
        logger.debug('validate_path resp::{}'.format(repr(resp)))
        data = await resp.json()
        logger.debug('validate_path data::{}'.format(json.dumps(data)))

        # If base folder isn't root or the immediate parent of the requested path, then we need
        # to verify that it actually is an ancestor of path.  Otherwise, a malicious user could
        # try to get access to a file outside of the configured root.
        base_folder = None
        if self.folder != 'root' and self.folder != data['parentReference']['id']:
            base_folder_resp = await self.make_request(
                'GET', self._build_item_url(self.folder),
                expects=(200, ),
                throws=exceptions.MetadataError
            )
            logger.debug('validate_path base_folder_resp::{}'.format(repr(base_folder_resp)))
            base_folder = await base_folder_resp.json()
            logger.debug('validate_path base_folder::{}'.format(json.dumps(base_folder)))

            base_full_path = urlparse.quote(
                '{}/{}/'.format(
                    urlparse.unquote(base_folder['parentReference']['path']),
                    base_folder['name']
                ),
                self.dont_escape_these
            )

            if not data['parentReference']['path'].startswith(base_full_path):
                # the requested file is NOT a child of self.folder
                raise exceptions.NotFoundError(path)  # TESTME

        od_path = OneDrivePath.new_from_response(data, self.folder,
                                                 base_folder_metadata=base_folder)
        logger.debug('validate_path od_path.parts::{}'.format(repr(od_path._parts)))
        return od_path

    async def revalidate_path(self,  # type: ignore
                              base: OneDrivePath,
                              path: str,
                              folder: bool=None) -> OneDrivePath:
        """Take a string file/folder name ``path`` and return a OneDrivePath object
        representing this file under ``base``.
        """
        logger.debug('revalidate_path base::{} path::{} base.id::{} folder::{}'.format(
            base, path, base.identifier, folder))

        base_url = self._build_drive_url(*base.api_identifier, expand='children')
        base_resp = await self.make_request(
            'GET',
            base_url,
            expects=(200, ),
            throws=exceptions.MetadataError
        )
        logger.debug('revalidate_path base_resp::{}'.format(repr(base_resp)))
        base_data = await base_resp.json()
        logger.debug('revalidate_path base_data::{}'.format(json.dumps(base_data)))

        child_id = None
        for child in base_data['children']:
            if child['name'] == path and (child.get('folder', None) is not None) == folder:
                child_id = child['id']
                break

        return base.child(path, _id=child_id, folder=folder)

    async def metadata(self, path: OneDrivePath, **kwargs):  # type: ignore
        """Fetch metadata for the file or folder identified by ``path``.

        API docs: https://docs.microsoft.com/en-us/graph/api/driveitem-get?view=graph-rest-1.0

        :param OneDrivePath path: the file or folder to fetch metadata for
        :rtype: OneDriveMetadata
        :rtype: list(OneDriveFileMetadata|OneDriveFolderMetadata)
        :return: either a OneDriveFileMetada for a single file or an array of either
            `OneDriveFileMetadata` or `OneDriveFolderMetadata` objects
        """
        logger.debug('metadata identifier::{} path::{}'.format(path.identifier, path))

        if path.api_identifier is None:  # TESTME
            raise exceptions.NotFoundError(str(path))

        url = self._build_drive_url(*path.api_identifier, expand='children')
        logger.debug("metadata url::{}".format(repr(url)))
        resp = await self.make_request(
            'GET',
            url,
            expects=(200, ),
            throws=exceptions.MetadataError
        )
        logger.debug("metadata resp::{}".format(repr(resp)))

        data = await resp.json()
        logger.debug("metadata data::{}".format(json.dumps(data)))

        if data.get('deleted'):
            raise exceptions.MetadataError(  # TESTME
                "Could not retrieve {kind} '{path}'".format(
                    kind='folder' if data['folder'] else 'file',
                    path=path,
                ),
                code=HTTPStatus.NOT_FOUND,
            )

        return self._construct_metadata(data, path)

    async def revisions(self,  # type: ignore
                        path: OneDrivePath,
                        **kwargs) -> typing.List[OneDriveRevisionMetadata]:
        """Get a list of revisions for the file identified by ``path``.

        API docs: https://docs.microsoft.com/en-us/graph/api/driveitem-list-versions?view=graph-rest-1.0

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

        API docs: https://docs.microsoft.com/en-us/graph/api/driveitem-get-content?view=graph-rest-1.0

        :param OneDrivePath path: The path to the file on OneDrive
        :param str revision: The revision of the file to download. If ``None``, download latest.
        :param dict \*\*kwargs: Ignored
        :rtype: waterbutler.core.streams.ResponseStreamReader
        :return: a stream of the contents of the file
        :raises: :class:`waterbutler.core.exceptions.DownloadError`
        """
        logger.debug('download path::{} path.identifier::{} revision::{} range::{} '
                     'kwargs::{}'.format(path, path.identifier, revision, range, kwargs))

        if path.identifier is None:
            raise exceptions.DownloadError('"{}" not found'.format(str(path)), code=404)

        download_url = None
        if revision:
            items = await self._revisions_json(path)
            for item in items['value']:
                if item['id'] == revision:
                    try:
                        download_url = item['@microsoft.graph.downloadUrl']
                    except KeyError:
                        raise exceptions.UnexportableFileTypeError(str(path))
                    break
        else:
            metadata = await self.metadata(path, revision=revision)
            logger.debug('download metadata::{}'.format(json.dumps(metadata.raw)))
            if metadata.package_type == 'oneNote':
                raise exceptions.UnexportableFileTypeError(str(path))
            download_url = metadata.download_url

        if download_url is None:
            raise exceptions.NotFoundError(str(path))

        logger.debug('download download_url::{}'.format(download_url))
        download_resp = await self.make_request(
            'GET',
            download_url,
            range=range,
            expects=(200, 206),
            headers={'accept-encoding': ''},
            # TODO: raise error if download folder including oneNote as zip
            # TODO: raise 401 error download empty file {"response": ""}
            throws=exceptions.DownloadError,
        )
        logger.debug('download download_resp::{}'.format(repr(download_resp)))

        return streams.ResponseStreamReader(download_resp)

    def can_duplicate_names(self) -> bool:
        return False

    def can_intra_move(self, other: provider.BaseProvider, path: WaterButlerPath=None) -> bool:
        return self == other

    def can_intra_copy(self, other: provider.BaseProvider, path: WaterButlerPath=None) -> bool:
        return self == other

    async def intra_move(self,
                         dest_provider: 'OneDriveProvider',
                         src_path: OneDrivePath,
                         dest_path: OneDrivePath) -> Tuple[BaseOneDriveMetadata, bool]:
        """Move the file or folder identified by ``src_path`` to the path identified by ``dest_path``.

        API docs: https://docs.microsoft.com/en-us/graph/api/driveitem-move?view=graph-rest-1.0

        :param OneDriveProvider dest_provider: OneDrive provider
        :param OneDrivePath src_path: The source path to the file on OneDrive
        :param OneDrivePath dest_path: The destination path to the file on OneDrive
        :rtype: (:class:`.BaseOneDriveMetadata`, :class:`bool`)
        :raises: :class:`waterbutler.core.exceptions.IntraMoveError`
        """
        logger.debug('intra_move src_path::{} dest_path::{}'.format(repr(src_path), repr(dest_path)))

        url = self._build_drive_url(*src_path.api_identifier)
        logger.debug("intra_move url::{}".format(repr(url)))
        resp = await self.make_request(
            'PATCH',
            url,
            headers={
                'Content-Type': 'application/json',
            },
            data=json.dumps({
                'parentReference': {
                    'id': dest_path.parent.identifier
                },
                'name': dest_path.name,
                '@microsoft.graph.conflictBehavior': 'replace',
            }),
            expects=(200, ),
            throws=exceptions.IntraMoveError,
        )
        logger.debug('intra_move resp::{}'.format(repr(resp)))
        data = await resp.json()
        logger.debug('intra_move data::{}'.format(json.dumps(data)))

        dest_path.parts[-1]._id = data['id']
        if dest_path.is_dir:
            metadata = OneDriveFolderMetadata(data, dest_path, self.NAME)
            metadata._children = await self.metadata(dest_path)
            return metadata, True
        else:
            return OneDriveFileMetadata(data, dest_path, self.NAME), True

    async def intra_copy(self,
                         dest_provider: 'OneDriveProvider',
                         src_path: OneDrivePath,
                         dest_path: OneDrivePath) -> Tuple[BaseOneDriveMetadata, bool]:
        """Copy the file or folder identified by ``src_path`` to the path identified by ``dest_path``.

        API docs: https://docs.microsoft.com/en-us/graph/api/driveitem-copy?view=graph-rest-1.0

        :param OneDriveProvider dest_provider: OneDrive provider
        :param OneDrivePath src_path: The source path to the file on OneDrive
        :param OneDrivePath dest_path: The destination path to the file on OneDrive
        :rtype: (:class:`.BaseOneDriveMetadata`, :class:`bool`)
        :raises: :class:`waterbutler.core.exceptions.IntraCopyError`
        """
        logger.debug('intra_copy src_path::{} dest_path::{}'.format(repr(src_path), repr(dest_path)))

        url = self._build_drive_url(*src_path.api_identifier, 'copy')
        logger.debug("intra_copy url::{}".format(repr(url)))
        resp = await self.make_request(
            'POST',
            url,
            headers={
                'Content-Type': 'application/json',
            },
            data=json.dumps({
                'parentReference': {
                    'id': dest_path.parent.identifier,
                },
                'name': dest_path.name,
                '@microsoft.graph.conflictBehavior': 'replace',
            }),
            expects=(202, ),
            throws=exceptions.IntraCopyError,
        )
        logger.debug('intra_copy resp::{}'.format(repr(resp)))

        monitor_url = resp.headers['Location']
        copied_id = await self._wait_for_api_action(monitor_url, exceptions.IntraCopyError)

        url = self._build_item_url(copied_id)
        logger.debug("intra_copy metadata url::{}".format(repr(url)))
        resp = await self.make_request(
            'GET',
            url,
            expects=(200, ),
            throws=exceptions.MetadataError
        )
        logger.debug("intra_copy metadata resp::{}".format(repr(resp)))
        data = await resp.json()
        logger.debug("intra_copy metadata data::{}".format(json.dumps(data)))

        dest_path.parts[-1]._id = data['id']
        if dest_path.is_dir:
            metadata = OneDriveFolderMetadata(data, dest_path, self.NAME)
            metadata._children = await self.metadata(dest_path)
            return metadata, True
        else:
            return OneDriveFileMetadata(data, dest_path, self.NAME), True

    async def create_folder(self,
                            path: OneDrivePath,
                            folder_precheck: bool=True,
                            **kwargs) -> OneDriveFolderMetadata:
        """Create a folder identified by ``path``.

        API docs: https://docs.microsoft.com/en-us/graph/api/driveitem-post-children?view=graph-rest-1.0

        :param OneDrivePath path: The folder path to create on OneDrive
        :param bool folder_precheck:
        :param dict \*\*kwargs: Ignored
        :rtype: :class:`.BaseFileMetadata`
        :raises: :class:`waterbutler.core.exceptions.CreateFolderError`
        """
        OneDrivePath.validate_folder(path)

        if folder_precheck:
            if (await self.exists(path)):
                raise exceptions.FolderNamingConflict(path.name)

        logger.debug('create_folder path::{}'.format(repr(path)))

        url = self._build_drive_url(*path.parent.api_identifier, 'children')
        logger.debug("create_folder url::{}".format(repr(url)))
        resp = await self.make_request(
            'POST',
            url,
            headers={
                'Content-Type': 'application/json',
            },
            data=json.dumps({
                'name': path.name,
                'folder': {},
            }),
            expects=(201, ),
            throws=exceptions.CreateFolderError,
        )
        logger.debug('create_folder resp::{}'.format(repr(resp)))
        data = await resp.json()
        logger.debug('create_folder data::{}'.format(json.dumps(data)))

        return OneDriveFolderMetadata(data, path, self.NAME)

    async def upload(self,
                     stream: streams.BaseStream,
                     path: OneDrivePath,
                     *args,
                     **kwargs) -> Tuple[OneDriveFileMetadata, bool]:
        """Upload the stream as the file identified by ``path``.
        Upload the file at once if the file is empty, otherwise, resumed upload the file.

        API docs: https://docs.microsoft.com/en-us/graph/api/driveitem-put-content?view=graph-rest-1.0
        API docs: https://docs.microsoft.com/en-us/graph/api/driveitem-createuploadsession?view=graph-rest-1.0

        :param streams.BaseStream stream: The content to be uploaded
        :param OneDrivePath path: The path to upload the file on OneDrive
        :param args: Ignored
        :param kwargs: Ignored
        :rtype: (:class:`OneDriveFileMetadata`, :class:`bool`)
        :raises: :class:`waterbutler.core.exceptions.UploadError`
        """
        logger.debug('upload path::{} stream.size::{}'.format(repr(path), stream.size))

        if stream.size == 0:
            metadata = await self._upload_empty_file(path)
        else:
            metadata = await self._resumed_upload(stream, path)

        return metadata, True

    async def delete(self,
                     path: OneDrivePath,
                     confirm_delete: int=0,
                     **kwargs) -> None:
        """Delete the file or directory identified by ``path``.

        API docs: https://docs.microsoft.com/en-us/graph/api/driveitem-delete?view=graph-rest-1.0

        :param OneDrivePath path: The path to delete the file or directory on OneDrive
        :param int confirm_delete:
        :param kwargs: Ignored
        :raises: :class:`waterbutler.core.exceptions.DeleteError`
        """
        if not path.identifier:
            raise exceptions.NotFoundError(str(path))

        if path.is_root:
            if not confirm_delete == 1:
                raise exceptions.DeleteError(
                    'confirm_delete=1 is required for deleting root provider folder',
                    code=400
                )

        url = self._build_drive_url(*path.api_identifier)
        logger.debug("delete url::{}".format(repr(url)))
        resp = await self.make_request(
            'DELETE',
            url,
            expects=(204, ),
            throws=exceptions.DeleteError
        )
        logger.debug("delete resp::{}".format(repr(resp)))

    # ========== utility methods ==========

    def _build_drive_url(self, *segments, **query) -> str:
        return provider.build_url(settings.BASE_DRIVE_URL, *segments, **query)

    def _build_item_url(self, *segments, **query) -> str:
        return provider.build_url(settings.BASE_DRIVE_URL, 'items', *segments, **query)

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
                        ret.append(OneDriveFolderMetadata(item, child_path, self.NAME))  # type: ignore
                    else:
                        ret.append(OneDriveFileMetadata(item, child_path, self.NAME))  # type: ignore
            return ret

        return OneDriveFileMetadata(data, path, self.NAME)

    async def _revisions_json(self, path: OneDrivePath, **kwargs) -> dict:
        """Fetch a list of revisions for the file at ``path``.

        API docs: https://docs.microsoft.com/en-us/graph/api/driveitem-list-versions?view=graph-rest-1.0

        :param OneDrivePath path: the path of the file to get revisions for
        :rtype: dict
        :return: list of revision metadata under a ``value`` key
        """
        try:
            resp = await self.make_request(
                'GET',
                self._build_drive_url(*path.api_identifier, 'versions'),
                expects=(200, ),
                throws=exceptions.RevisionsError
            )
        except exceptions.RevisionsError as exc:
            if (
                    exc.code == 405 and
                    exc.data['error']['message'] == 'Item does not match expected type'
            ):
                # OneNote versioning not supported, instead return a lone revision called 'current'
                url = self._build_drive_url(*path.api_identifier)
                resp = await self.make_request(
                    'GET',
                    url,
                    expects=(200,),
                    throws=exceptions.MetadataError
                )
                logger.debug("metadata resp::{}".format(repr(resp)))
                data = await resp.json()
                return {'value': [
                    {'id': 'current', 'lastModifiedDateTime': data['lastModifiedDateTime']},
                ]}  # fudge a fake revision response
            else:
                raise exc

        logger.debug('_revisions_json: resp::{}'.format(repr(resp)))
        data = await resp.json()
        logger.debug('_revisions_json: data::{}'.format(json.dumps(data)))

        return data

    async def _upload_empty_file(self, path: OneDrivePath) -> OneDriveFileMetadata:
        """Upload empty file identified by ``path``.

        API docs: https://docs.microsoft.com/en-us/graph/api/driveitem-put-content?view=graph-rest-1.0

        :param OneDrivePath path: The path to upload the file on OneDrive
        :rtype: OneDriveFileMetadata
        :raises: :class:`waterbutler.core.exceptions.UploadError`
        """
        url = '{}:/{}:/content'.format(
            self._build_drive_url(*path.parent.api_identifier),
            path.name,
        )
        logger.debug("_upload_empty_file url::{}".format(url))
        resp = await self.make_request(
            'PUT',
            url,
            expects=(201, ),
            throws=exceptions.UploadError,
        )
        logger.debug('_upload_empty_file resp::{}'.format(repr(resp)))
        data = await resp.json()
        logger.debug('_upload_empty_file data::{}'.format(json.dumps(data)))

        return OneDriveFileMetadata(data, path, self.NAME)

    async def _resumed_upload(self,
                              stream: streams.BaseStream,
                              path: OneDrivePath) -> OneDriveFileMetadata:
        """Upload the stream as the file identified by ``path``.

        API docs: https://docs.microsoft.com/en-us/graph/api/driveitem-createuploadsession?view=graph-rest-1.0

        :param streams.BaseStream stream: The content to be uploaded
        :param OneDrivePath path: The path to upload the file on OneDrive
        :rtype: OneDriveFileMetadata
        :raises: :class:`waterbutler.core.exceptions.UploadError`
        """
        url = '{}:/{}:/createUploadSession'.format(
            self._build_drive_url(*path.parent.api_identifier),
            path.name,
        )
        logger.debug("_resumed_upload start url::{}".format(url))
        resp = await self.make_request(
            'POST',
            url,
            headers={
                'Content-Type': 'application/json',
            },
            data=json.dumps({
                'name': path.name
            }),
            expects=(200, ),
            throws=exceptions.UploadError,
        )
        logger.debug('_resumed_upload start resp::{}'.format(repr(resp)))
        data = await resp.json()
        logger.debug('_resumed_upload start data::{}'.format(json.dumps(data)))
        upload_url = data['uploadUrl']
        logger.debug("_resumed_upload upload_url::{}".format(upload_url))

        # upload data at least once.
        # data must be sent in order.
        all_size = stream.size
        start_byte = 0
        while True:
            chunk_size = min(settings.ONEDRIVE_MAX_UPLOAD_CHUNK_SIZE, all_size - start_byte)
            chunk = CutoffStream(stream, cutoff=chunk_size)
            headers = {
                'Content-Length': str(chunk_size),
                'Content-Range': 'bytes {}-{}/{}'.format(
                    start_byte,
                    start_byte + chunk_size - 1,
                    all_size
                ),
            }
            logger.debug('_resumed_upload put headers::{}'.format(json.dumps(headers)))
            resp = await self.make_request(
                'PUT',
                upload_url,
                headers=headers,
                data=chunk,
                expects=(201, 202, ),
                throws=exceptions.UploadError,
                no_auth_header=True,
            )
            logger.debug('_resumed_upload put resp::{}'.format(repr(resp)))
            data = await resp.json()
            logger.debug('_resumed_upload put data::{}'.format(json.dumps(data)))
            start_byte += chunk_size
            if start_byte == all_size:
                break

        return OneDriveFileMetadata(data, path, self.NAME)

    async def _wait_for_api_action(self, monitor_url: str, exception: exceptions.WaterButlerError) -> str:
        """Wait for the API Action to finish.

        API docs:  https://docs.microsoft.com/en-us/graph/long-running-actions-overview

        :param str monitor_url: The monitor URL provided by API
        :param waterbutler.core.exceptions.WaterButlerError exception: The exception
        :return: Resource ID
        :rtype: str
        """
        logger.debug('_wait_for_api_action monitor url::{}'.format(monitor_url))

        while True:
            resp = await self.make_request(
                'GET',
                monitor_url,
                expects=(200, 202, ),
                throws=exception,
                no_auth_header=True,
            )
            logger.debug('_wait_for_api_action resp::{}'.format(repr(resp)))
            data = await resp.json()
            logger.debug('_wait_for_api_action data::{}'.format(json.dumps(data)))

            if data['status'] == 'completed':
                return data['resourceId']
            elif data['status'] == 'failed':
                raise exception('failed OneDrive API action.', code=data['status'])
            else:
                await sleep(settings.ONEDRIVE_COPY_SLEEP_INTERVAL)
