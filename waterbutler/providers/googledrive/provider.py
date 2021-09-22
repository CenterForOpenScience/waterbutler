import os
import json
import hashlib
import functools
from urllib import parse
from http import HTTPStatus
from typing import List, Sequence, Tuple, Union

import furl

from waterbutler.core import exceptions, provider, streams
from waterbutler.core.path import WaterButlerPath, WaterButlerPathPart

from waterbutler.providers.googledrive import utils
from waterbutler.providers.googledrive import settings as pd_settings
from waterbutler.providers.googledrive.metadata import (GoogleDriveRevision,
                                                        BaseGoogleDriveMetadata,
                                                        GoogleDriveFileMetadata,
                                                        GoogleDriveFolderMetadata,
                                                        GoogleDriveFileRevisionMetadata, )


def clean_query(query: str):
    # Replace \ with \\ and ' with \'
    # Note only single quotes need to be escaped
    return query.replace('\\', r'\\').replace("'", r"\'")


class GoogleDrivePathPart(WaterButlerPathPart):
    DECODE = parse.unquote
    # TODO: mypy lacks a syntax to define kwargs for callables
    ENCODE = functools.partial(parse.quote, safe='')  # type: ignore


class GoogleDrivePath(WaterButlerPath):
    PART_CLASS = GoogleDrivePathPart


class GoogleDriveProvider(provider.BaseProvider):
    """Provider for Google's Drive cloud storage service.

    This provider uses the v2 Drive API.  A v3 API is available, but this provider has not yet
    been updated.

    API docs: https://developers.google.com/drive/v2/reference/

    Quirks:

    * Google doc files (``.gdoc``, ``.gsheet``, ``.gsheet``, ``.gdraw``) cannot be downloaded in
      their native format and must be exported to another format.  e.g. ``.gdoc`` to ``.docx``

    * Some Google doc files (currently ``.gform`` and ``.gmap``) do not have an available export
      format and cannot be downloaded at all.

    * Google Drive is not really a filesystem.  Folders are actually labels, meaning a file ``foo``
      could be in two folders (ex. ``A``, ``B``) at the same time.  Deleting ``/A/foo`` will
      cause ``/B/foo`` to be deleted as well.

    Revisions:

    Both Google Drive and WaterButler have weird behaviors wrt file revisions.  Google docs use a
    simple integer versioning system.  Non-Google doc files, like jpegs or text files, use strings
    that resemble the standard Google Drive file ID format (ex.
    ``0B74RCNS4TbRVTitFais4VzVmQlQ4S0docGlhelk5MXE3OFJnPQ``).  In addition, revision history is not
    available for any file that the user only has view or commenting permissions for.  In the past
    WB forged revision ids for these files by taking the etag of the file and appending a sentinel
    value (set in `googledrive.settings.DRIVE_IGNORE_VERSION`) to the end.  If WB receives a request
    to download a file with a revision ending with the sentinel string, we ignore the revision and
    return the latest version instead.  The file metadata endpoint will behave the same.  A metadata
    or download request for a readonly file with a revision value that doesn't end with the sentinel
    value will always return a 404 Not Found.
    """
    NAME = 'googledrive'
    BASE_URL = pd_settings.BASE_URL
    FOLDER_MIME_TYPE = 'application/vnd.google-apps.folder'

    # https://developers.google.com/drive/v2/web/about-permissions#roles
    # 'reader' and 'commenter' are not authorized to access the revisions list
    ROLES_ALLOWING_REVISIONS = ['owner', 'organizer', 'writer']

    def __init__(self, auth: dict, credentials: dict, settings: dict, **kwargs) -> None:
        super().__init__(auth, credentials, settings, **kwargs)
        self.token = self.credentials['token']
        self.folder = self.settings['folder']

    async def validate_v1_path(self, path: str, **kwargs) -> GoogleDrivePath:
        if path == '/':
            return GoogleDrivePath('/', _ids=[self.folder['id']], folder=True)

        implicit_folder = path.endswith('/')
        parts = await self._resolve_path_to_ids(path)
        explicit_folder = parts[-1]['mimeType'] == self.FOLDER_MIME_TYPE
        if parts[-1]['id'] is None or implicit_folder != explicit_folder:
            raise exceptions.NotFoundError(str(path))

        names, ids = zip(*[(parse.quote(x['title'], safe=''), x['id']) for x in parts])
        return GoogleDrivePath('/'.join(names), _ids=ids, folder='folder' in parts[-1]['mimeType'])

    async def validate_path(self, path: str, **kwargs) -> GoogleDrivePath:
        if path == '/':
            return GoogleDrivePath('/', _ids=[self.folder['id']], folder=True)

        parts = await self._resolve_path_to_ids(path)
        names, ids = zip(*[(parse.quote(x['title'], safe=''), x['id']) for x in parts])
        return GoogleDrivePath('/'.join(names), _ids=ids, folder='folder' in parts[-1]['mimeType'])

    async def revalidate_path(self,
                              base: WaterButlerPath,
                              name: str,
                              folder: bool=None) -> WaterButlerPath:
        # TODO Redo the logic here folders names ending in /s
        # Will probably break
        if '/' in name.lstrip('/') and '%' not in name:
            # DAZ and MnC may pass unquoted names which break
            # if the name contains a / in it
            name = parse.quote(name.lstrip('/'), safe='')

        if not name.endswith('/') and folder:
            name += '/'

        parts = await self._resolve_path_to_ids(name, start_at=[{
            'title': base.name,
            'mimeType': 'folder',
            'id': base.identifier,
        }])
        _id, name, mime = list(map(parts[-1].__getitem__, ('id', 'title', 'mimeType')))
        return base.child(name, _id=_id, folder='folder' in mime)

    def can_duplicate_names(self) -> bool:
        return True

    @property
    def default_headers(self) -> dict:
        return {'authorization': 'Bearer {}'.format(self.token)}

    def can_intra_move(self, other: provider.BaseProvider, path: WaterButlerPath=None) -> bool:
        return self == other

    def can_intra_copy(self, other: provider.BaseProvider, path=None) -> bool:
        # gdrive doesn't support intra-copy on folders
        return self == other and (path and path.is_file)

    async def intra_move(self,  # type: ignore
                         dest_provider: provider.BaseProvider,
                         src_path: WaterButlerPath,
                         dest_path: WaterButlerPath) -> Tuple[BaseGoogleDriveMetadata, bool]:
        self.metrics.add('intra_move.destination_exists', dest_path.identifier is not None)
        if dest_path.identifier:
            await dest_provider.delete(dest_path)

        resp = await self.make_request(
            'PATCH',
            self.build_url('files', src_path.identifier),
            headers={
                'Content-Type': 'application/json'
            },
            data=json.dumps({
                'parents': [{
                    'id': dest_path.parent.identifier
                }],
                'title': dest_path.name
            }),
            expects=(200, ),
            throws=exceptions.IntraMoveError,
        )
        data = await resp.json()

        created = dest_path.identifier is None
        dest_path.parts[-1]._id = data['id']

        if dest_path.is_dir:
            metadata = GoogleDriveFolderMetadata(data, dest_path)
            metadata._children = await self._folder_metadata(dest_path)
            return metadata, created
        else:
            return GoogleDriveFileMetadata(data, dest_path), created  # type: ignore

    async def intra_copy(self,
                         dest_provider: provider.BaseProvider,
                         src_path: WaterButlerPath,
                         dest_path: WaterButlerPath) -> Tuple[GoogleDriveFileMetadata, bool]:
        self.metrics.add('intra_copy.destination_exists', dest_path.identifier is not None)
        if dest_path.identifier:
            await dest_provider.delete(dest_path)

        resp = await self.make_request(
            'POST',
            self.build_url('files', src_path.identifier, 'copy'),
            headers={'Content-Type': 'application/json'},
            data=json.dumps({
                'parents': [{
                    'id': dest_path.parent.identifier
                }],
                'title': dest_path.name
            }),
            expects=(200, ),
            throws=exceptions.IntraMoveError,
        )
        data = await resp.json()

        # GoogleDrive doesn't support intra-copy for folders, so dest_path will always
        # be a file.  See can_intra_copy() for type check.
        return GoogleDriveFileMetadata(data, dest_path), dest_path.identifier is None

    async def download(self,  # type: ignore
                       path: GoogleDrivePath,
                       revision: str=None,
                       range: Tuple[int, int]=None,
                       **kwargs) -> streams.BaseStream:
        """Download the file at `path`.  If `revision` is present, attempt to download that revision
        of the file.  See **Revisions** in the class doctring for an explanation of this provider's
        revision handling.   The actual revision handling is done in `_file_metadata()`.

        Quirks:

        Google docs don't have a size until they're exported, so WB must download them, then
        re-stream them as a StringStream.

        :param GoogleDrivePath path: the file to download
        :param str revision: the id of a particular version to download
        :param tuple(int, int) range: range of bytes to download in this request
        :rtype: streams.ResponseStreamReader
        :rtype: streams.StringStream
        :returns: For GDocs, a StringStream.  All others, a ResponseStreamReader.
        """

        metadata = await self.metadata(path, revision=revision)

        download_resp = await self.make_request(
            'GET',
            metadata.raw.get('downloadUrl') or utils.get_export_link(metadata.raw),  # type: ignore
            range=range,
            expects=(200, 206),
            throws=exceptions.DownloadError,
        )

        if metadata.size is not None and not metadata.is_google_doc:  # type: ignore
            return streams.ResponseStreamReader(download_resp,
                                                size=metadata.size_as_int)  # type: ignore

        # google docs, not drive files, have no way to get the file size
        # must buffer the entire file into memory
        stream = streams.StringStream(await download_resp.read())
        if download_resp.headers.get('Content-Type'):
            # TODO: Add these properties to base class officially, instead of as one-off
            stream.content_type = download_resp.headers['Content-Type']  # type: ignore
        stream.name = metadata.export_name  # type: ignore
        return stream

    async def upload(self,
                     stream,
                     path: WaterButlerPath,
                     *args,
                     **kwargs) -> Tuple[GoogleDriveFileMetadata, bool]:
        assert path.is_file

        if path.identifier:
            segments = [path.identifier]
        else:
            segments = []

        stream.add_writer('md5', streams.HashStreamWriter(hashlib.md5))

        upload_metadata = self._build_upload_metadata(path.parent.identifier, path.name)
        upload_id = await self._start_resumable_upload(not path.identifier, segments, stream.size,
                                                       upload_metadata)
        data = await self._finish_resumable_upload(segments, stream, upload_id)

        if data['md5Checksum'] != stream.writers['md5'].hexdigest:
            raise exceptions.UploadChecksumMismatchError()

        return GoogleDriveFileMetadata(data, path), path.identifier is None

    async def delete(self,  # type: ignore
                     path: GoogleDrivePath,
                     confirm_delete: int=0,
                     **kwargs) -> None:
        """Given a WaterButlerPath, delete that path
        :param GoogleDrivePath path: Path to be deleted
        :param int confirm_delete: Must be 1 to confirm root folder delete
        :rtype: None
        :raises: :class:`waterbutler.core.exceptions.NotFoundError`
        :raises: :class:`waterbutler.core.exceptions.DeleteError`

        Quirks:
            If the WaterButlerPath given is for the provider root path, then
            the contents of provider root path will be deleted. But not the
            provider root itself.
        """
        if not path.identifier:
            raise exceptions.NotFoundError(str(path))

        self.metrics.add('delete.is_root_delete', path.is_root)
        if path.is_root:
            self.metrics.add('delete.root_delete_confirmed', confirm_delete == 1)
            if confirm_delete == 1:
                await self._delete_folder_contents(path)
                return
            else:
                raise exceptions.DeleteError(
                    'confirm_delete=1 is required for deleting root provider folder',
                    code=400
                )

        await self.make_request(
            'PUT',
            self.build_url('files', path.identifier),
            data=json.dumps({'labels': {'trashed': 'true'}}),
            headers={'Content-Type': 'application/json'},
            expects=(200, ),
            throws=exceptions.DeleteError,
        )
        return

    def _build_query(self, folder_id: str, title: str=None) -> str:
        queries = [
            "'{}' in parents".format(folder_id),
            'trashed = false',
            "mimeType != 'application/vnd.google-apps.form'",
            "mimeType != 'application/vnd.google-apps.map'",
        ]
        if title:
            queries.append("title = '{}'".format(clean_query(title)))
        return ' and '.join(queries)

    async def metadata(self,  # type: ignore
                       path: GoogleDrivePath,
                       raw: bool=False,
                       revision=None,
                       **kwargs) -> Union[dict, BaseGoogleDriveMetadata,
                                          List[Union[BaseGoogleDriveMetadata, dict]]]:
        if path.identifier is None:
            raise exceptions.MetadataError('{} not found'.format(str(path)), code=404)

        if path.is_dir:
            return await self._folder_metadata(path, raw=raw)

        return await self._file_metadata(path, revision=revision, raw=raw)

    async def revisions(self, path: GoogleDrivePath,  # type: ignore
                        **kwargs) -> List[GoogleDriveRevision]:
        """Returns list of revisions for the file at ``path``.

        Google Drive will not allow a user to view the revision list of a file if they only have
        view or commenting permissions.  It will return a 403 Unathorized.  If that happens, then
        we construct a recognizable dummy revision based off of the metadata of the current file
        version.

        Note: though we explicitly support the case where the revision list is empty, I have yet to
        see it in practice.  The current handling is based on historical behavior.

        :param GoogleDrivePath path: the path of the file to fetch revisions for
        :rtype: `list(GoogleDriveRevision)`
        :return: list of `GoogleDriveRevision` objects representing revisions of the file
        """
        if path.identifier is None:
            raise exceptions.NotFoundError(str(path))

        resp = await self.make_request(
            'GET',
            self.build_url('files', path.identifier, 'revisions'),
            expects=(200, 403, ),
            throws=exceptions.RevisionsError,
        )
        data = await resp.json()
        has_revisions = resp.status == 200

        if has_revisions and data['items']:
            return [
                GoogleDriveRevision(item)
                for item in reversed(data['items'])
            ]

        # Use dummy ID if no revisions found
        metadata = await self.metadata(path, raw=True)
        revision = {
            'modifiedDate': metadata['modifiedDate'],  # type: ignore
            'id': metadata['etag'] + pd_settings.DRIVE_IGNORE_VERSION,  # type: ignore
        }
        return [GoogleDriveRevision(revision), ]

    async def create_folder(self,
                            path: WaterButlerPath,
                            folder_precheck: bool=True,
                            **kwargs) -> GoogleDriveFolderMetadata:
        GoogleDrivePath.validate_folder(path)

        if folder_precheck:
            if path.identifier:
                raise exceptions.FolderNamingConflict(path.name)

        resp = await self.make_request(
            'POST',
            self.build_url('files'),
            headers={
                'Content-Type': 'application/json',
            },
            data=json.dumps({
                'title': path.name,
                'parents': [{
                    'id': path.parent.identifier
                }],
                'mimeType': self.FOLDER_MIME_TYPE,
            }),
            expects=(200, ),
            throws=exceptions.CreateFolderError,
        )
        return GoogleDriveFolderMetadata(await resp.json(), path)

    def path_from_metadata(self, parent_path, metadata):
        """ Unfortunately-named method, currently only used to get path name for zip archives. """
        return parent_path.child(metadata.export_name, _id=metadata.id, folder=metadata.is_folder)

    def _build_upload_url(self, *segments, **query):
        return provider.build_url(pd_settings.BASE_UPLOAD_URL, *segments, **query)

    def _serialize_item(self,
                        path: WaterButlerPath,
                        item: dict,
                        raw: bool=False) -> Union[BaseGoogleDriveMetadata, dict]:
        if raw:
            return item
        if item['mimeType'] == self.FOLDER_MIME_TYPE:
            return GoogleDriveFolderMetadata(item, path)
        return GoogleDriveFileMetadata(item, path)

    def _build_upload_metadata(self, folder_id: str, name: str) -> dict:
        return {
            'parents': [
                {
                    'kind': 'drive#parentReference',
                    'id': folder_id,
                },
            ],
            'title': name,
        }

    async def _start_resumable_upload(self,
                                      created: bool,
                                      segments: Sequence[str],
                                      size,
                                      metadata: dict) -> str:
        resp = await self.make_request(
            'POST' if created else 'PUT',
            self._build_upload_url('files', *segments, uploadType='resumable'),
            headers={
                'Content-Type': 'application/json',
                'X-Upload-Content-Length': str(size),
            },
            data=json.dumps(metadata),
            expects=(200, ),
            throws=exceptions.UploadError,
        )
        location = furl.furl(resp.headers['LOCATION'])
        return location.args['upload_id']

    async def _finish_resumable_upload(self, segments: Sequence[str], stream, upload_id):
        resp = await self.make_request(
            'PUT',
            self._build_upload_url('files', *segments, uploadType='resumable', upload_id=upload_id),
            headers={'Content-Length': str(stream.size)},
            data=stream,
            expects=(200, ),
            throws=exceptions.UploadError,
        )
        return await resp.json()

    async def _resolve_path_to_ids(self, path, start_at=None):
        """Takes a path and traverses the file tree (ha!) beginning at ``start_at``, looking for
        something that matches ``path``.  Returns a list of dicts for each part of the path, with
        ``title``, ``mimeType``, and ``id`` keys.
        """
        self.metrics.incr('called_resolve_path_to_ids')
        ret = start_at or [{
            'title': '',
            'mimeType': 'folder',
            'id': self.folder['id'],
        }]
        item_id = ret[0]['id']
        # parts is list of [path_part_name, is_folder]
        parts = [[parse.unquote(x), True] for x in path.strip('/').split('/')]

        if not path.endswith('/'):
            parts[-1][1] = False
        while parts:
            current_part = parts.pop(0)
            part_name, part_is_folder = current_part[0], current_part[1]
            name, ext = os.path.splitext(part_name)
            if not part_is_folder and ext in ('.gdoc', '.gdraw', '.gslides', '.gsheet'):
                gd_ext = utils.get_mimetype_from_ext(ext)
                query = "title = '{}' " \
                        "and trashed = false " \
                        "and mimeType = '{}'".format(clean_query(name), gd_ext)
            else:
                query = "title = '{}' " \
                        "and trashed = false " \
                        "and mimeType != 'application/vnd.google-apps.form' " \
                        "and mimeType != 'application/vnd.google-apps.map' " \
                        "and mimeType != 'application/vnd.google-apps.document' " \
                        "and mimeType != 'application/vnd.google-apps.drawing' " \
                        "and mimeType != 'application/vnd.google-apps.presentation' " \
                        "and mimeType != 'application/vnd.google-apps.spreadsheet' " \
                        "and mimeType {} '{}'".format(
                            clean_query(part_name),
                            '=' if part_is_folder else '!=',
                            self.FOLDER_MIME_TYPE
                        )
            resp = await self.make_request(
                'GET',
                self.build_url('files', item_id, 'children', q=query, fields='items(id)'),
                expects=(200, ),
                throws=exceptions.MetadataError,
            )
            data = await resp.json()

            try:
                item_id = data['items'][0]['id']
            except (KeyError, IndexError):
                if parts:
                    # if we can't find an intermediate path part, that's an error
                    raise exceptions.MetadataError('{} not found'.format(str(path)),
                                                   code=HTTPStatus.NOT_FOUND)
                return ret + [{
                    'id': None,
                    'title': part_name,
                    'mimeType': 'folder' if part_is_folder else '',
                }]

            resp = await self.make_request(
                'GET',
                self.build_url('files', item_id, fields='id,title,mimeType'),
                expects=(200, ),
                throws=exceptions.MetadataError,
            )
            ret.append(await resp.json())
        return ret

    async def _handle_docs_versioning(self, path: GoogleDrivePath, item: dict, raw: bool=True):
        """Sends an extra request to GDrive to fetch revision information for Google Docs. Needed
        because Google Docs use a different versioning system from regular files.

        I've been unable to replicate the case where revisions_data['items'] is None.  I'm leaving
        it in for now and adding a metric to see if we ever actually encounter this case.  If not,
        we should probably remove it to simplify this method.

        This method does not handle the case of read-only google docs, which will return a 403.
        Other methods should check the ``userPermission.role`` field of the file metadata before
        calling this.  If the value of that field is ``"reader"`` or ``"commenter"``, this method
        will error.

        :param GoogleDrivePath path: the path of the google doc to get version information for
        :param dict item: a raw response object from the GDrive file metadata endpoint
        :param bool raw: should we return the raw response object from the GDrive API?
        :rtype: GoogleDriveFileMetadata
        :rtype: dict
        :return: a metadata for the googledoc or the raw response object from the GDrive API
        """
        resp = await self.make_request(
            'GET',
            self.build_url('files', item['id'], 'revisions'),
            expects=(200, ),
            throws=exceptions.RevisionsError,
        )
        revisions_data = await resp.json()
        has_revisions = revisions_data['items'] is not None

        # Revisions are not available for some sharing configurations. If revisions list is empty,
        # use the etag of the file plus a sentinel string as a dummy revision ID.
        self.metrics.add('handle_docs_versioning.empty_revision_list', not has_revisions)
        if has_revisions:
            item['version'] = revisions_data['items'][-1]['id']
        else:
            # If there are no revisions use etag as vid
            item['version'] = item['etag'] + pd_settings.DRIVE_IGNORE_VERSION

        return self._serialize_item(path, item, raw=raw)

    async def _folder_metadata(self,
                               path: WaterButlerPath,
                               raw: bool=False) -> List[Union[BaseGoogleDriveMetadata, dict]]:
        query = self._build_query(path.identifier)
        built_url = self.build_url('files', q=query, alt='json', maxResults=1000)
        full_resp = []
        while built_url:
            resp = await self.make_request(
                'GET',
                built_url,
                expects=(200, ),
                throws=exceptions.MetadataError,
            )
            resp_json = await resp.json()
            full_resp.extend([
                self._serialize_item(path.child(item['title']), item, raw=raw)
                for item in resp_json['items']
            ])
            built_url = resp_json.get('nextLink', None)
        return full_resp

    async def _file_metadata(self,
                             path: GoogleDrivePath,
                             revision: str=None,
                             raw: bool=False) -> Union[dict, BaseGoogleDriveMetadata]:
        """ Returns metadata for the file identified by `path`.  If the `revision` arg is set,
        will attempt to return metadata for the given revision of the file.  If the revision does
        not exist, ``_file_metadata`` will throw a 404.

        This method used to error with a 500 when metadata was requested for a file that the
        authorizing user only had view or commenting permissions for.  The GDrive revisions
        endpoint returns a 403, which was not being handled.  WB postpends a sentinel value to the
        revisions for these files.  If a revision ending with this sentinel value is detected, this
        method will return metadata for the latest revision of the file.  If a revision NOT ending
        in the sentinel value is requested for a read-only file, this method will return a 404 Not
        Found instead.

        Metrics:

        ``_file_metadata.got_revision``: did this request include a revision parameter?

        ``_file_metadata.revision_is_valid``: if a revision was given, was it valid? A revision is
        "valid" if it doesn't end with our sentinal string (`settings.DRIVE_IGNORE_VERSION`).

        ``_file_metadata.user_role``: What role did the user possess? Helps identify other roles
        for which revision information isn't available.

        :param GoogleDrivePath path: the path of the file whose metadata is being requested
        :param str revision: a string representing the ID of the revision (default: `None`)
        :param bool raw: should we return the raw response object from the GDrive API?
        :rtype: GoogleDriveFileMetadata
        :rtype: dict
        :return: a metadata for the googledoc or the raw response object from the GDrive API
        """

        self.metrics.add('_file_metadata.got_revision', revision is not None)

        valid_revision = revision and not revision.endswith(pd_settings.DRIVE_IGNORE_VERSION)
        if revision:
            self.metrics.add('_file_metadata.revision_is_valid', valid_revision)

        if revision and valid_revision:
            url = self.build_url('files', path.identifier, 'revisions', revision)
        else:
            url = self.build_url('files', path.identifier)

        resp = await self.make_request(
            'GET', url,
            expects=(200, 403, 404, ),
            throws=exceptions.MetadataError,
        )
        try:
            data = await resp.json()
        except Exception:  # some 404s return a string instead of json
            data = await resp.read()

        if resp.status != 200:
            raise exceptions.NotFoundError(path)

        if revision and valid_revision:
            return GoogleDriveFileRevisionMetadata(data, path)

        user_role = data['userPermission']['role']
        self.metrics.add('_file_metadata.user_role', user_role)
        can_access_revisions = user_role in self.ROLES_ALLOWING_REVISIONS
        if utils.is_docs_file(data):
            if can_access_revisions:
                return await self._handle_docs_versioning(path, data, raw=raw)
            else:
                # Revisions are not available for some sharing configurations. If revisions list is
                # empty, use the etag of the file plus a sentinel string as a dummy revision ID.
                data['version'] = data['etag'] + pd_settings.DRIVE_IGNORE_VERSION

        return data if raw else GoogleDriveFileMetadata(data, path)

    async def _delete_folder_contents(self, path: WaterButlerPath) -> None:
        """Given a WaterButlerPath, delete all contents of folder

        :param WaterButlerPath path: Folder to be emptied
        :rtype: None
        :raises: :class:`waterbutler.core.exceptions.NotFoundError`
        :raises: :class:`waterbutler.core.exceptions.MetadataError`
        :raises: :class:`waterbutler.core.exceptions.DeleteError`
        """
        file_id = path.identifier
        if not file_id:
            raise exceptions.NotFoundError(str(path))
        resp = await self.make_request(
            'GET',
            self.build_url('files',
                           q="'{}' in parents".format(file_id),
                           fields='items(id)'),
            expects=(200, ),
            throws=exceptions.MetadataError)

        try:
            child_ids = (await resp.json())['items']
        except (KeyError, IndexError):
            raise exceptions.MetadataError('{} not found'.format(str(path)),
                                           code=HTTPStatus.NOT_FOUND)

        for child in child_ids:
            await self.make_request(
                'PUT',
                self.build_url('files', child['id']),
                data=json.dumps({'labels': {'trashed': 'true'}}),
                headers={'Content-Type': 'application/json'},
                expects=(200, ),
                throws=exceptions.DeleteError)
