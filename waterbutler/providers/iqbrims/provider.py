import os
import typing
import functools
from urllib import parse
from http import HTTPStatus

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
from waterbutler.core import path as wb_path

from waterbutler.providers.iqbrims import settings
from waterbutler.providers.iqbrims import utils as drive_utils
from waterbutler.providers.iqbrims.metadata import (BaseIQBRIMSMetadata,
                                                        IQBRIMSFileMetadata,
                                                        IQBRIMSFileRevisionMetadata,
                                                        IQBRIMSFolderMetadata,
                                                        IQBRIMSRevision)


def clean_query(query: str):
    # Replace \ with \\ and ' with \'
    # Note only single quotes need to be escaped
    return query.replace('\\', r'\\').replace("'", r"\'")


class IQBRIMSPathPart(wb_path.WaterButlerPathPart):
    DECODE = parse.unquote
    # TODO: mypy lacks a syntax to define kwargs for callables (same todo with GoogleDrive provider)
    ENCODE = functools.partial(parse.quote, safe='')  # type: ignore


class IQBRIMSPath(wb_path.WaterButlerPath):
    PART_CLASS = IQBRIMSPathPart


class IQBRIMSProvider(provider.BaseProvider):
    """Provider for Google's Drive cloud storage service.

    This provider uses the v2 Drive API.  A v3 API is available, but this provider has not yet
    been updated.

    API docs: https://developers.google.com/drive/v2/reference/

    Quirks:

    * Google doc files (``.gdoc``, ``.gsheet``, ``.gsheet``, ``.gdraw``) cannot be downloaded in
      their native format and must be exported to another format.  e.g. ``.gdoc`` to ``.docx``

    * Some Google doc files (currently ``.gform`` and ``.gmap``) do not have an available export
      format and cannot be downloaded at all.

    * IQB-RIMS is not really a filesystem.  Folders are actually labels, meaning a file ``foo``
      could be in two folders (ex. ``A``, ``B``) at the same time.  Deleting ``/A/foo`` will
      cause ``/B/foo`` to be deleted as well.

    Revisions:

    Both IQB-RIMS and WaterButler have weird behaviors wrt file revisions.  Google docs use a
    simple integer versioning system.  Non-Google doc files, like jpegs or text files, use strings
    that resemble the standard IQB-RIMS file ID format (ex.
    ``0B74RCNS4TbRVTitFais4VzVmQlQ4S0docGlhelk5MXE3OFJnPQ``).  In addition, revision history is not
    available for any file that the user only has view or commenting permissions for.  In the past
    WB forged revision ids for these files by taking the etag of the file and appending a sentinel
    value (set in `iqbrims.settings.DRIVE_IGNORE_VERSION`) to the end.  If WB receives a request
    to download a file with a revision ending with the sentinel string, we ignore the revision and
    return the latest version instead.  The file metadata endpoint will behave the same.  A metadata
    or download request for a readonly file with a revision value that doesn't end with the sentinel
    value will always return a 404 Not Found.
    """
    NAME = 'iqbrims'
    BASE_URL = settings.BASE_URL
    FOLDER_MIME_TYPE = 'application/vnd.google-apps.folder'

    # https://developers.google.com/drive/v2/web/about-permissions#roles
    # 'reader' and 'commenter' are not authorized to access the revisions list
    ROLES_ALLOWING_REVISIONS = ['owner', 'organizer', 'writer']

    def __init__(self, auth: dict, credentials: dict, settings: dict) -> None:
        super().__init__(auth, credentials, settings)
        self.token = self.credentials['token']
        self.folder = self.settings['folder']

    async def validate_v1_path(self, path: str, **kwargs) -> IQBRIMSPath:
        if path == '/':
            return IQBRIMSPath('/', _ids=[self.folder['id']], folder=True)

        implicit_folder = path.endswith('/')
        parts = await self._resolve_path_to_ids(path)
        explicit_folder = parts[-1]['mimeType'] == self.FOLDER_MIME_TYPE
        if parts[-1]['id'] is None or implicit_folder != explicit_folder:
            raise exceptions.NotFoundError(str(path))

        names, ids = zip(*[(parse.quote(x['title'], safe=''), x['id']) for x in parts])
        return IQBRIMSPath('/'.join(names), _ids=ids, folder='folder' in parts[-1]['mimeType'])

    async def validate_path(self, path: str, **kwargs) -> IQBRIMSPath:
        if path == '/':
            return IQBRIMSPath('/', _ids=[self.folder['id']], folder=True)

        parts = await self._resolve_path_to_ids(path)
        names, ids = zip(*[(parse.quote(x['title'], safe=''), x['id']) for x in parts])
        return IQBRIMSPath('/'.join(names), _ids=ids, folder='folder' in parts[-1]['mimeType'])

    async def revalidate_path(self,
                              base: wb_path.WaterButlerPath,
                              name: str,
                              folder: bool = None) -> wb_path.WaterButlerPath:
        # TODO Redo the logic here folders names ending in /s (same todo with GoogleDrive provider)
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

    @property
    def default_headers(self) -> dict:
        return {'authorization': 'Bearer {}'.format(self.token)}

    async def download(self,  # type: ignore
                       path: IQBRIMSPath,
                       revision: str=None,
                       range: typing.Tuple[int, int]=None,
                       **kwargs) -> streams.BaseStream:  # type: ignore
        """Download the file at `path`.  If `revision` is present, attempt to download that revision
        of the file.  See **Revisions** in the class doctring for an explanation of this provider's
        revision handling.   The actual revision handling is done in `_file_metadata()`.

        Quirks:

        Google docs don't have a size until they're exported, so WB must download them, then
        re-stream them as a StringStream.

        :param IQBRIMSPath path: the file to download
        :param str revision: the id of a particular version to download
        :param tuple(int, int) range: range of bytes to download in this request
        :rtype: streams.ResponseStreamReader
        :rtype: streams.StringStream
        :returns: For GDocs, a StringStream.  All others, a ResponseStreamReader.
        """

        metadata = await self.metadata(path, revision=revision)

        download_resp = await self.make_request(
            'GET',
            metadata.raw.get('downloadUrl') or drive_utils.get_export_link(metadata.raw),  # type: ignore
            range=range,
            expects=(200, 206),
            throws=exceptions.DownloadError,
        )

        if metadata.size is not None:  # type: ignore
            return streams.ResponseStreamReader(download_resp, size=metadata.size)  # type: ignore

        # google docs, not drive files, have no way to get the file size
        # must buffer the entire file into memory
        stream = streams.StringStream(await download_resp.read())
        if download_resp.headers.get('Content-Type'):
            # TODO: Add these properties to base class officially, instead of as one-off (same todo with GoogleDrive provider)
            stream.content_type = download_resp.headers['Content-Type']  # type: ignore
        stream.name = metadata.export_name  # type: ignore
        return stream

    def can_duplicate_names(self) -> bool:
        return True

    def can_intra_move(self, other, path=None) -> bool:
        return False

    def can_intra_copy(self, other, path=None) -> bool:
        return False

    async def upload(self, *args, **kwargs):
        raise exceptions.ReadOnlyProviderError(self.NAME)

    async def create_folder(self, *args, **kwargs):
        raise exceptions.ReadOnlyProviderError(self.NAME)

    async def delete(self, *args, **kwargs):
        raise exceptions.ReadOnlyProviderError(self.NAME)

    async def move(self, *args, **kwargs):
        raise exceptions.ReadOnlyProviderError(self.NAME)

    async def copy(self, *args, **kwargs):
        raise exceptions.ReadOnlyProviderError(self.NAME)

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
                       path: IQBRIMSPath,
                       raw: bool = False,
                       revision=None,
                       **kwargs) -> typing.Union[dict, BaseIQBRIMSMetadata, typing.List[typing.Union[BaseIQBRIMSMetadata, dict]]]:
        if path.identifier is None:
            raise exceptions.MetadataError('{} not found'.format(str(path)), code=404)

        if path.is_dir:
            return await self._folder_metadata(path, raw=raw)

        return await self._file_metadata(path, revision=revision, raw=raw)

    async def revisions(self, path: IQBRIMSPath, **kwargs) -> typing.List[IQBRIMSRevision]:  # type: ignore
        """Returns list of revisions for the file at ``path``.

        IQB-RIMS will not allow a user to view the revision list of a file if they only have
        view or commenting permissions.  It will return a 403 Unathorized.  If that happens, then
        we construct a recognizable dummy revision based off of the metadata of the current file
        version.

        Note: though we explicitly support the case where the revision list is empty, I have yet to
        see it in practice.  The current handling is based on historical behavior.

        :param IQBRIMSPath path: the path of the file to fetch revisions for
        :rtype: `list(IQBRIMSRevision)`
        :return: list of `IQBRIMSRevision` objects representing revisions of the file
        """
        if path.identifier is None:
            raise exceptions.NotFoundError(str(path))

        async with self.request(
            'GET',
            self.build_url('files', path.identifier, 'revisions'),
            expects=(200, 403, ),
            throws=exceptions.RevisionsError,
        ) as resp:
            data = await resp.json()
            has_revisions = resp.status == 200

        if has_revisions and data['items']:
            return [
                IQBRIMSRevision(item)
                for item in reversed(data['items'])
            ]

        # Use dummy ID if no revisions found
        metadata = await self.metadata(path, raw=True)
        return [IQBRIMSRevision({
            'modifiedDate': metadata['modifiedDate'],  # type: ignore
            'id': metadata['etag'] + settings.DRIVE_IGNORE_VERSION,
        })]

    def path_from_metadata(self, parent_path, metadata):
        """ Unfortunately-named method, currently only used to get path name for zip archives. """
        return parent_path.child(metadata.export_name, _id=metadata.id, folder=metadata.is_folder)

    def _serialize_item(self,
                        path: wb_path.WaterButlerPath,
                        item: dict,
                        raw: bool=False) -> typing.Union[BaseIQBRIMSMetadata, dict]:
        if raw:
            return item
        if item['mimeType'] == self.FOLDER_MIME_TYPE:
            return IQBRIMSFolderMetadata(item, path)
        return IQBRIMSFileMetadata(item, path)

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
                gd_ext = drive_utils.get_mimetype_from_ext(ext)
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
            async with self.request(
                'GET',
                self.build_url('files', item_id, 'children', q=query, fields='items(id)'),
                expects=(200, ),
                throws=exceptions.MetadataError,
            ) as resp:
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

            async with self.request(
                'GET',
                self.build_url('files', item_id, fields='id,title,mimeType'),
                expects=(200, ),
                throws=exceptions.MetadataError,
            ) as resp:
                ret.append(await resp.json())
        return ret

    async def _handle_docs_versioning(self, path: IQBRIMSPath, item: dict, raw: bool=True):
        """Sends an extra request to GDrive to fetch revision information for Google Docs. Needed
        because Google Docs use a different versioning system from regular files.

        I've been unable to replicate the case where revisions_data['items'] is None.  I'm leaving
        it in for now and adding a metric to see if we ever actually encounter this case.  If not,
        we should probably remove it to simplify this method.

        This method does not handle the case of read-only google docs, which will return a 403.
        Other methods should check the ``userPermission.role`` field of the file metadata before
        calling this.  If the value of that field is ``"reader"`` or ``"commenter"``, this method
        will error.

        :param IQBRIMSPath path: the path of the google doc to get version information for
        :param dict item: a raw response object from the GDrive file metadata endpoint
        :param bool raw: should we return the raw response object from the GDrive API?
        :rtype: IQBRIMSFileMetadata
        :rtype: dict
        :return: a metadata for the googledoc or the raw response object from the GDrive API
        """
        async with self.request(
            'GET',
            self.build_url('files', item['id'], 'revisions'),
            expects=(200, ),
            throws=exceptions.RevisionsError,
        ) as resp:
            revisions_data = await resp.json()
            has_revisions = revisions_data['items'] is not None

        # Revisions are not available for some sharing configurations. If revisions list is empty,
        # use the etag of the file plus a sentinel string as a dummy revision ID.
        self.metrics.add('handle_docs_versioning.empty_revision_list', not has_revisions)
        if has_revisions:
            item['version'] = revisions_data['items'][-1]['id']
        else:
            # If there are no revisions use etag as vid
            item['version'] = item['etag'] + settings.DRIVE_IGNORE_VERSION

        return self._serialize_item(path, item, raw=raw)

    async def _folder_metadata(self, path: wb_path.WaterButlerPath, raw: bool=False) \
            -> typing.List[typing.Union[BaseIQBRIMSMetadata, dict]]:
        query = self._build_query(path.identifier)
        built_url = self.build_url('files', q=query, alt='json', maxResults=1000)
        full_resp = []
        while built_url:
            async with self.request(
                'GET',
                built_url,
                expects=(200, ),
                throws=exceptions.MetadataError,
            ) as resp:
                resp_json = await resp.json()
                full_resp.extend([
                    self._serialize_item(path.child(item['title']), item, raw=raw)
                    for item in resp_json['items']
                ])
                built_url = resp_json.get('nextLink', None)
        return full_resp

    async def _file_metadata(self,
                             path: IQBRIMSPath,
                             revision: str=None,
                             raw: bool=False):
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

        :param IQBRIMSPath path: the path of the file whose metadata is being requested
        :param str revision: a string representing the ID of the revision (default: `None`)
        :param bool raw: should we return the raw response object from the GDrive API?
        :rtype: IQBRIMSFileMetadata
        :rtype: dict
        :return: a metadata for the googledoc or the raw response object from the GDrive API
        """

        self.metrics.add('_file_metadata.got_revision', revision is not None)

        valid_revision = revision and not revision.endswith(settings.DRIVE_IGNORE_VERSION)
        if revision:
            self.metrics.add('_file_metadata.revision_is_valid', valid_revision)

        if revision and valid_revision:
            url = self.build_url('files', path.identifier, 'revisions', revision)
        else:
            url = self.build_url('files', path.identifier)

        async with self.request(
            'GET', url,
            expects=(200, 403, 404, ),
            throws=exceptions.MetadataError,
        ) as resp:
            try:
                data = await resp.json()
            except:  # some 404s return a string instead of json
                data = await resp.read()

        if resp.status != 200:
            raise exceptions.NotFoundError(path)

        if revision and valid_revision:
            return IQBRIMSFileRevisionMetadata(data, path)

        user_role = data['userPermission']['role']
        self.metrics.add('_file_metadata.user_role', user_role)
        can_access_revisions = user_role in self.ROLES_ALLOWING_REVISIONS
        if drive_utils.is_docs_file(data):
            if can_access_revisions:
                return await self._handle_docs_versioning(path, data, raw=raw)
            else:
                # Revisions are not available for some sharing configurations. If revisions list is
                # empty, use the etag of the file plus a sentinel string as a dummy revision ID.
                data['version'] = data['etag'] + settings.DRIVE_IGNORE_VERSION

        return data if raw else IQBRIMSFileMetadata(data, path)
