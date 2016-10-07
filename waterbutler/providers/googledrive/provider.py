import os
import http
import json
import functools
from urllib import parse

import furl

from waterbutler.core import path
from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions

from waterbutler.providers.googledrive import settings
from waterbutler.providers.googledrive import utils as drive_utils
from waterbutler.providers.googledrive.metadata import GoogleDriveRevision
from waterbutler.providers.googledrive.metadata import GoogleDriveFileMetadata
from waterbutler.providers.googledrive.metadata import GoogleDriveFolderMetadata
from waterbutler.providers.googledrive.metadata import GoogleDriveFileRevisionMetadata


def clean_query(query):
    # Replace \ with \\ and ' with \'
    # Note only single quotes need to be escaped
    return query.replace('\\', r'\\').replace("'", r"\'")


class GoogleDrivePathPart(path.WaterButlerPathPart):
    DECODE = parse.unquote
    ENCODE = functools.partial(parse.quote, safe='')


class GoogleDrivePath(path.WaterButlerPath):
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
    """
    NAME = 'googledrive'
    BASE_URL = settings.BASE_URL
    FOLDER_MIME_TYPE = 'application/vnd.google-apps.folder'

    def __init__(self, auth, credentials, settings):
        super().__init__(auth, credentials, settings)
        self.token = self.credentials['token']
        self.folder = self.settings['folder']

    async def validate_v1_path(self, path, **kwargs):
        if path == '/':
            return GoogleDrivePath('/', _ids=[self.folder['id']], folder=True)

        implicit_folder = path.endswith('/')
        parts = await self._resolve_path_to_ids(path)
        explicit_folder = parts[-1]['mimeType'] == self.FOLDER_MIME_TYPE
        if parts[-1]['id'] is None or implicit_folder != explicit_folder:
            raise exceptions.NotFoundError(str(path))

        names, ids = zip(*[(parse.quote(x['title'], safe=''), x['id']) for x in parts])
        return GoogleDrivePath('/'.join(names), _ids=ids, folder='folder' in parts[-1]['mimeType'])

    async def validate_path(self, path, **kwargs):
        if path == '/':
            return GoogleDrivePath('/', _ids=[self.folder['id']], folder=True)

        parts = await self._resolve_path_to_ids(path)
        names, ids = zip(*[(parse.quote(x['title'], safe=''), x['id']) for x in parts])
        return GoogleDrivePath('/'.join(names), _ids=ids, folder='folder' in parts[-1]['mimeType'])

    async def revalidate_path(self, base, name, folder=None):
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

    def can_duplicate_names(self):
        return True

    @property
    def default_headers(self):
        return {'authorization': 'Bearer {}'.format(self.token)}

    def can_intra_move(self, other, path=None):
        return self == other

    def can_intra_copy(self, other, path=None):
        return self == other and (path and path.is_file)

    async def intra_move(self, dest_provider, src_path, dest_path):
        if dest_path.identifier:
            await dest_provider.delete(dest_path)

        async with self.request(
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
        ) as resp:
            data = await resp.json()

        return GoogleDriveFileMetadata(data, dest_path), dest_path.identifier is None

    async def intra_copy(self, dest_provider, src_path, dest_path):
        if dest_path.identifier:
            await dest_provider.delete(dest_path)

        async with self.request(
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
        ) as resp:
            data = await resp.json()
        return GoogleDriveFileMetadata(data, dest_path), dest_path.identifier is None

    async def download(self, path, revision=None, range=None, **kwargs):
        if revision and not revision.endswith(settings.DRIVE_IGNORE_VERSION):
            metadata = await self.metadata(path, revision=revision)
        else:
            metadata = await self.metadata(path)

        download_resp = await self.make_request(
            'GET',
            metadata.raw.get('downloadUrl') or drive_utils.get_export_link(metadata.raw),
            range=range,
            expects=(200, 206),
            throws=exceptions.DownloadError,
        )

        if metadata.size is not None:
            return streams.ResponseStreamReader(download_resp, size=metadata.size)

        # google docs, not drive files, have no way to get the file size
        # must buffer the entire file into memory
        stream = streams.StringStream(await download_resp.read())
        if download_resp.headers.get('Content-Type'):
            stream.content_type = download_resp.headers['Content-Type']
        stream.name = metadata.export_name
        return stream

    async def upload(self, stream, path, **kwargs):
        assert path.is_file

        if path.identifier:
            segments = (path.identifier, )
        else:
            segments = ()

        upload_metadata = self._build_upload_metadata(path.parent.identifier, path.name)
        upload_id = await self._start_resumable_upload(not path.identifier, segments, stream.size, upload_metadata)
        data = await self._finish_resumable_upload(segments, stream, upload_id)

        return GoogleDriveFileMetadata(data, path), path.identifier is None

    async def delete(self, path, confirm_delete=0, **kwargs):
        """Given a WaterButlerPath, delete that path

        :param WaterButlerPath: Path to be deleted
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

        if path.is_root:
            if confirm_delete == 1:
                await self._delete_folder_contents(path)
                return
            else:
                raise exceptions.DeleteError(
                    'confirm_delete=1 is required for deleting root provider folder',
                    code=400
                )

        async with self.request(
            'PUT',
            self.build_url('files', path.identifier),
            data=json.dumps({'labels': {'trashed': 'true'}}),
            headers={'Content-Type': 'application/json'},
            expects=(200, ),
            throws=exceptions.DeleteError,
        ):
            return

    def _build_query(self, folder_id, title=None):
        queries = [
            "'{}' in parents".format(folder_id),
            'trashed = false',
            "mimeType != 'application/vnd.google-apps.form'",
            "mimeType != 'application/vnd.google-apps.map'",
        ]
        if title:
            queries.append("title = '{}'".format(clean_query(title)))
        return ' and '.join(queries)

    async def metadata(self, path, raw=False, revision=None, **kwargs):
        if path.identifier is None:
            raise exceptions.MetadataError('{} not found'.format(str(path)), code=404)

        if path.is_dir:
            return await self._folder_metadata(path, raw=raw)

        return await self._file_metadata(path, revision=revision, raw=raw)

    async def revisions(self, path, **kwargs):
        if path.identifier is None:
            raise exceptions.NotFoundError(str(path))

        async with self.request(
            'GET',
            self.build_url('files', path.identifier, 'revisions'),
            expects=(200, ),
            throws=exceptions.RevisionsError,
        ) as resp:
            data = await resp.json()
        if data['items']:
            return [
                GoogleDriveRevision(item)
                for item in reversed(data['items'])
            ]

        metadata = await self.metadata(path, raw=True)

        # Use dummy ID if no revisions found
        return [GoogleDriveRevision({
            'modifiedDate': metadata['modifiedDate'],
            'id': data['etag'] + settings.DRIVE_IGNORE_VERSION,
        })]

    async def create_folder(self, path, folder_precheck=True, **kwargs):
        GoogleDrivePath.validate_folder(path)

        if folder_precheck:
            if path.identifier:
                raise exceptions.FolderNamingConflict(str(path))

        async with self.request(
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
        ) as resp:
            return GoogleDriveFolderMetadata(await resp.json(), path)

    def path_from_metadata(self, parent_path, metadata):
        """ Unfortunately-named method, currently only used to get path name for zip archives. """
        return parent_path.child(metadata.export_name, _id=metadata.id, folder=metadata.is_folder)

    def _build_upload_url(self, *segments, **query):
        return provider.build_url(settings.BASE_UPLOAD_URL, *segments, **query)

    def _serialize_item(self, path, item, raw=False):
        if raw:
            return item
        if item['mimeType'] == self.FOLDER_MIME_TYPE:
            return GoogleDriveFolderMetadata(item, path)
        return GoogleDriveFileMetadata(item, path)

    def _build_upload_metadata(self, folder_id, name):
        return {
            'parents': [
                {
                    'kind': 'drive#parentReference',
                    'id': folder_id,
                },
            ],
            'title': name,
        }

    async def _start_resumable_upload(self, created, segments, size, metadata):
        async with self.request(
            'POST' if created else 'PUT',
            self._build_upload_url('files', *segments, uploadType='resumable'),
            headers={
                'Content-Type': 'application/json',
                'X-Upload-Content-Length': str(size),
            },
            data=json.dumps(metadata),
            expects=(200, ),
            throws=exceptions.UploadError,
        ) as resp:
            location = furl.furl(resp.headers['LOCATION'])
        return location.args['upload_id']

    async def _finish_resumable_upload(self, segments, stream, upload_id):
        async with self.request(
            'PUT',
            self._build_upload_url('files', *segments, uploadType='resumable', upload_id=upload_id),
            headers={'Content-Length': str(stream.size)},
            data=stream,
            expects=(200, ),
            throws=exceptions.UploadError,
        ) as resp:
            return await resp.json()

    async def _materialized_path_to_id(self, path, parent_id=None):
        parts = path.parts
        item_id = parent_id or self.folder['id']

        while parts:
            query = self._build_query(path.identifier)
            async with self.request(
                'GET',
                self.build_url('files', item_id, 'children', q=query),
                expects=(200, ),
                throws=exceptions.MetadataError,
            ) as resp:
                try:
                    item_id = (await resp.json())['items'][0]['id']
                except (KeyError, IndexError):
                    raise exceptions.MetadataError('{} not found'.format(str(path)), code=http.client.NOT_FOUND)

        return item_id

    async def _resolve_path_to_ids(self, path, start_at=None):
        """Takes a path and traverses the file tree (ha!) beginning at ``start_at``, looking for
        something that matches ``path``.  Returns a list of dicts for each part of the path, with
        ``title``, ``mimeType``, and ``id`` keys.
        """
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
                    raise exceptions.MetadataError('{} not found'.format(str(path)), code=http.client.NOT_FOUND)
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

    async def _resolve_id_to_parts(self, _id, accum=None):
        if _id == self.folder['id']:
            return [{
                'title': '',
                'mimeType': 'folder',
                'id': self.folder['id'],
            }] + (accum or [])

        if accum is None:
            async with self.request(
                'GET',
                self.build_url('files', _id, fields='id,title,mimeType'),
                expects=(200, ),
                throws=exceptions.MetadataError,
            ) as resp:
                accum = [await resp.json()]

        for parent in await self._get_parent_ids(_id):
            if self.folder['id'] == parent['id']:
                return [parent] + (accum or [])
                try:
                    return await self._resolve_id_to_parts(
                        self, parent['id'],
                        [parent] + (accum or [])
                    )
                except exceptions.MetadataError:
                    pass

        # TODO Custom exception here
        raise exceptions.MetadataError('ID is out of scope')

    async def _get_parent_ids(self, _id):
        async with self.request(
            'GET',
            self.build_url('files', _id, 'parents', fields='items(id)'),
            expects=(200, ),
            throws=exceptions.MetadataError,
        ) as resp:
            parents_data = await resp.json()

        parents = []
        for parent in parents_data['items']:
            async with self.request(
                'GET',
                self.build_url('files', parent['id'], fields='id,title,labels/trashed'),
                expects=(200, ),
                throws=exceptions.MetadataError,
            ) as p_resp:
                parents.append(await p_resp.json())
        return parents

    async def _handle_docs_versioning(self, path, item, raw=True):
        async with self.request(
            'GET',
            self.build_url('files', item['id'], 'revisions'),
            expects=(200, ),
            throws=exceptions.RevisionsError,
        ) as resp:
            revisions_data = await resp.json()

        # Revisions are not available for some sharing configurations. If
        # revisions list is empty, use the etag of the file plus a sentinel
        # string as a dummy revision ID.
        if not revisions_data['items']:
            # If there are no revisions use etag as vid
            item['version'] = revisions_data['etag'] + settings.DRIVE_IGNORE_VERSION
        else:
            item['version'] = revisions_data['items'][-1]['id']

        return self._serialize_item(path, item, raw=raw)

    async def _folder_metadata(self, path, raw=False):
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

    async def _file_metadata(self, path, revision=None, raw=False):
        if revision:
            url = self.build_url('files', path.identifier, 'revisions', revision)
        else:
            url = self.build_url('files', path.identifier)

        async with self.request(
            'GET', url,
            expects=(200, ),
            throws=exceptions.MetadataError,
        ) as resp:
            data = await resp.json()

        if revision:
            return GoogleDriveFileRevisionMetadata(data, path)

        if drive_utils.is_docs_file(data):
            return await self._handle_docs_versioning(path, data, raw=raw)

        return self._serialize_item(path, data, raw=raw)

    async def _delete_folder_contents(self, path):
        """Given a WaterButlerPath, delete all contents of folder

        :param WaterButlerPath: Folder to be emptied
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
            raise exceptions.MetadataError('{} not found'.format(str(path)), code=http.client.NOT_FOUND)

        for child in child_ids:
            await self.make_request(
                'PUT',
                self.build_url('files', child['id']),
                data=json.dumps({'labels': {'trashed': 'true'}}),
                headers={'Content-Type': 'application/json'},
                expects=(200, ),
                throws=exceptions.DeleteError)
