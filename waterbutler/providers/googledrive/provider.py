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

    async def validate_v1_path(self, path, **kwargs):

        parts = await self._resolve_path_to_ids(path)

        is_folder = parts[-1]['mimeType'] == self.FOLDER_MIME_TYPE

        if is_folder and not path.endswith('/'):
            raise exceptions.NotFoundError(str(path))
        elif not is_folder and path.endswith('/'):
            raise exceptions.NotFoundError(str(path))

        ids = [x['id'] for x in parts]
        return GoogleDrivePath(path, _ids=ids, folder=self.FOLDER_MIME_TYPE in parts[-1]['mimeType'])

    def __init__(self, auth, credentials, settings):
        super().__init__(auth, credentials, settings)
        self.token = self.credentials['token']
        self.folder = self.settings['folder']

    async def validate_path(self, path, **kwargs):
        if path == '/':
            return GoogleDrivePath('/', _ids=[self.folder['id']], folder=True)

        parts = await self._resolve_path_to_ids(path)
        names, ids = zip(*[(parse.quote(x['title'], safe=''), x['id']) for x in parts])
        return GoogleDrivePath('/'.join(names), _ids=ids, folder=self.FOLDER_MIME_TYPE in parts[-1]['mimeType'])

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
            'mimeType': self.FOLDER_MIME_TYPE,
            'id': base.identifier,
        }])
        _id, name, mime = list(map(parts[-1].__getitem__, ('id', 'title', 'mimeType')))
        return base.child(name, _id=_id, folder=self.FOLDER_MIME_TYPE in mime)

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
        self.metrics.add('intra_move.destination_exists', dest_path.identifier is not None)
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
        self.metrics.add('intra_copy.destination_exists', dest_path.identifier is not None)
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
            self.metrics.add('download.got_supported_revision', True)
        else:
            metadata = await self.metadata(path)
            self.metrics.add('download.got_supported_revision', False)

        if metadata.has_view_only_permission:
            raise exceptions.AuthError('You don\'t have permission to download this file through '
                                       'the Open Science Framework', code=403)

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

        print(self.build_url('files', path.identifier, 'revisions'))
        metadata = await self.metadata(path)

        if metadata.has_view_only_permission:
            raise exceptions.AuthError('You don\'t have permission to download this file through '
                                       'the Open Science Framework', code=403)

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

        # Use dummy ID if no revisions found
        return [GoogleDriveRevision({
            'modifiedDate': metadata.raw['modifiedDate'],
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
        the file metadata.

        GFiles can only be called without an extension, this causes a great degree of ambiguity as
        to which files files are being called when multiple files of the same name can exist in
        the same directory. This function makes sure in cases where a non-GFiles and a GFile have
        the same name the non-GFile is called by the API (because you still retrieve the GFile
        by including the extension, but not the Non-GFile which may not even have a recognized format.)
        """
        self.metrics.incr('called_resolve_path_to_ids')

        item_id = self.folder['id']
        root = [{
            'title': '',
            'mimeType': self.FOLDER_MIME_TYPE,
            'id': item_id,
        }]
        ret = start_at or root

        parts = [parse.unquote(x) for x in path.strip('/').split('/') if x != '']

        for name_with_ext in parts:
            name_without_ext, ext = os.path.splitext(name_with_ext)
            query_name = name_without_ext if drive_utils.ext_is_gfile(ext) else name_with_ext

            async with self.request(
                    'GET',
                    self.build_url('files', q="title = '{}' "
                                              "and trashed = false "
                                              "and '{}' in parents".format(query_name, item_id)),
                    expects=(200, ),
                    throws=exceptions.MetadataError,
            ) as resp:
                data = await resp.json()
                results = data['items']

            if len(results) == 0:
                raise exceptions.NotFoundError(str(path))
            elif len(results) == 1:
                item = results[0]
            else:
                item = drive_utils.disambiguate_files(ext, results, path)

            item_id = item['id']

            ret.append(item)

        return ret

    async def _resolve_id_to_parts(self, _id, accum=None):
        self.metrics.incr('called_resolve_id_to_parts')
        if _id == self.folder['id']:
            return [{
                'title': '',
                'mimeType': self.FOLDER_MIME_TYPE,
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
        self.metrics.add('handle_docs_versioning.revisions_not_supported', not revisions_data['items'])
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
