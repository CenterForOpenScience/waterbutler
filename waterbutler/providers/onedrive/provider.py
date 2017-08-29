import json
import typing
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
    """Provider for the Microsoft OneDrive cloud storage service.

    This provider is currently **read-only** and does not contain write support.

    This provider uses **ID-based paths**.

    Special drives: https://dev.onedrive.com/resources/drive.htm#tasks-on-drive-resources

    **Auth:**

    * auth: ``{"name": "username", "email": "username@example.com"}``

    * credentials: ``{"token": "EWaa932BEN32042094DNFWJ40234=="}``

    * settings: ``{"folder": "/foo/"}``

    **API:**

    * Docs:  https://dev.onedrive.com/README.htm

    * Get folder contents:  If folder is root, api path is ``/drive/root/children``. If folder
      is not root, api path is ``/drive/items/$item-id/children``.

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

    def __init__(self, auth, credentials, settings):
        logger.debug('__init__ auth::{} settings::{}'.format(auth, settings))
        super().__init__(auth, credentials, settings)
        self.token = self.credentials['token']
        self.folder = self.settings['folder']

    # ========== properties ==========

    @property
    def default_headers(self) -> dict:
        """Set Authorization header with access token from auth provider.

        API docs: https://dev.onedrive.com/auth/msa_oauth.htm
        """
        return {'Authorization': 'bearer {}'.format(self.token)}

    # ========== methods ==========

    async def validate_v1_path(self, path: str, **kwargs) -> OneDrivePath:
        """validate that ``path`` exists and matches the implicit semantics.

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

        Since the OneDrive provider is currently readonly, the only place that calls this is
        `core.provider._file_folder_op`.  The base object passed there will always have an
        identifier.  Once write support is added to this provider, that will no longer be the
        case.

        This probably isn't necessary for RO, and could probably be replaced by
        `path_from_metadata`.
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

        if child_id is None:
            raise exceptions.NotFoundError(path)

        return base.child(path, _id=child_id, folder=folder)

    async def metadata(self, path: OneDrivePath, **kwargs):  # type: ignore
        """Fetch metadata for the file or folder identified by ``path``.

        API docs: https://dev.onedrive.com/items/get.htm

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

        return self._construct_metadata(data)

    async def revisions(self,  # type: ignore
                        path: OneDrivePath,
                        **kwargs) -> typing.List[OneDriveRevisionMetadata]:
        """Get a list of revisions for the file identified by ``path``.

        API docs: https://dev.onedrive.com/items/view_delta.htm

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
            if not item.get('deleted')
        ]

    async def download(self,  # type: ignore
                       path: OneDrivePath,
                       revision: str=None,
                       range: typing.Tuple[int, int]=None,
                       **kwargs) -> streams.ResponseStreamReader:
        """Download the file identified by ``path``.  If ``revision`` is not ``None``, get
        the file at the version identified by ``revision``.

        API docs: https://dev.onedrive.com/items/download.htm

        :param str path: The path to the file on OneDrive
        :param str revision: The revision of the file to download. If ``None``, download latest.
        :param dict \*\*kwargs: Ignored
        :raises: :class:`waterbutler.core.exceptions.DownloadError`
        :rtype: waterbutler.core.streams.ResponseStreamReader
        :return: a stream of the contents of the file
        """
        logger.debug('download path::{} path.identifier::{} revision::{} range::{} '
                     'kwargs::{}'.format(path, path.identifier, revision, range, kwargs))

        if path.identifier is None:
            raise exceptions.DownloadError('"{}" not found'.format(str(path)), code=404)

        download_url = None
        if revision:
            items = await self._revisions_json(path)
            for item in items['value']:
                if item['eTag'] == revision:
                    try:
                        download_url = item['@content.downloadUrl']
                    except KeyError:
                        raise exceptions.UnexportableFileTypeError(str(path))
                    break
        else:
            # TODO: we should be able to get the download url from validate_v1_path
            metadata_resp = await self.make_request(
                'GET',
                self._build_drive_url(*path.api_identifier),
                expects=(200, ),
                throws=exceptions.MetadataError
            )
            logger.debug('download metadata_resp::{}'.format(repr(metadata_resp)))
            metadata = await metadata_resp.json()
            logger.debug('download metadata::{}'.format(json.dumps(metadata)))

            try:
                package_type = metadata['package']['type']
            except KeyError:
                pass
            else:
                if package_type == 'oneNote':
                    raise exceptions.UnexportableFileTypeError(str(path))

            download_url = metadata.get('@content.downloadUrl', None)

        if download_url is None:
            raise exceptions.NotFoundError(str(path))

        logger.debug('download download_url::{}'.format(download_url))
        download_resp = await self.make_request(
            'GET',
            download_url,
            range=range,
            expects=(200, 206),
            headers={'accept-encoding': ''},
            throws=exceptions.DownloadError,
        )
        logger.debug('download download_resp::{}'.format(repr(download_resp)))

        return streams.ResponseStreamReader(download_resp)

    def can_duplicate_names(self) -> bool:
        return False

    def can_intra_move(self, other, path=None) -> bool:
        return False

    def can_intra_copy(self, other, path=None) -> bool:
        return False

    async def upload(self, *args, **kwargs):
        raise exceptions.ReadOnlyProviderError(self.NAME)

    async def delete(self, *args, **kwargs):
        raise exceptions.ReadOnlyProviderError(self.NAME)

    async def move(self, *args, **kwargs):
        raise exceptions.ReadOnlyProviderError(self.NAME)

    # copy is okay if source is foobar and destination is not
    async def copy(self, dest_provider, *args, **kwargs):
        if dest_provider.NAME == self.NAME:
            raise exceptions.ReadOnlyProviderError(self.NAME)
        return await super().copy(dest_provider, *args, **kwargs)  # TESTME

    # ========== utility methods ==========

    def _build_drive_url(self, *segments, **query) -> str:
        return provider.build_url(settings.BASE_DRIVE_URL, *segments, **query)

    def _build_item_url(self, *segments, **query) -> str:
        return provider.build_url(settings.BASE_DRIVE_URL, 'items', *segments, **query)

    def _construct_metadata(self, data: dict):
        """Take a file/folder metadata response from OneDrive and return a `OneDriveFileMetadata`
        object if the repsonse represents a file or a list of `OneDriveFileMetadata` and
        `OneDriveFolderMetadata` objects if the response represents a folder. """
        if 'folder' in data.keys():
            ret = []
            if 'children' in data.keys():
                for item in data['children']:
                    if 'folder' in item.keys():
                        ret.append(OneDriveFolderMetadata(item, self.folder))  # type: ignore
                    else:
                        ret.append(OneDriveFileMetadata(item, self.folder))  # type: ignore
            return ret

        return OneDriveFileMetadata(data, self.folder)

    async def _revisions_json(self, path: OneDrivePath, **kwargs) -> dict:
        """Fetch a list of revisions for the file at ``path``.

        API docs: https://dev.onedrive.com/items/view_delta.htm

        :param OneDrivePath path: the path of the file to get revisions for
        :rtype: dict
        :return: list of revision metadata under a ``value`` key
        """

        resp = await self.make_request(
            'GET',
            self._build_drive_url(*path.api_identifier, 'view.delta', top=self.MAX_REVISIONS),
            expects=(200, ),
            throws=exceptions.RevisionsError
        )
        logger.debug('_revisions_json: resp::{}'.format(repr(resp)))
        data = await resp.json()
        logger.debug('_revisions_json: data::{}'.format(json.dumps(data)))

        return data
