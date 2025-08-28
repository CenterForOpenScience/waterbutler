import base64
import hashlib
import asyncio
import logging
import datetime
import xml.etree.ElementTree as ET
import uuid

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath
from waterbutler.core.streams import StringStream

from waterbutler.providers.azureblobstorage import settings
from waterbutler.providers.azureblobstorage.metadata import AzureBlobStorageFileMetadata
from waterbutler.providers.azureblobstorage.metadata import AzureBlobStorageFolderMetadata
from waterbutler.providers.azureblobstorage.metadata import AzureBlobStorageFileMetadataHeaders


logger = logging.getLogger(__name__)


class AzureBlobStorageProvider(provider.BaseProvider):
    """Provider for Microsoft Azure Blob Storage cloud storage service.

    API docs: https://docs.microsoft.com/en-us/rest/api/storageservices/blob-service-rest-api

    Quirks:

    * Empty directories are maintained using .osfkeep marker files, as Azure Blob Storage
      does not natively support empty folders. These marker files are automatically created
      when folders are created and filtered out during directory listings.
    """

    NAME = 'azureblobstorage'
    API_VERSION = '2025-07-05'
    CHUNK_SIZE = settings.CHUNK_SIZE
    CONTIGUOUS_UPLOAD_SIZE_LIMIT = settings.CONTIGUOUS_UPLOAD_SIZE_LIMIT

    def __init__(self, auth, credentials, settings, **kwargs):
        """
        :param dict auth: Not used
        :param dict credentials: Dict containing OAuth2 'token'
        :param dict settings: Dict containing 'container', 'account_name', and optional 'base_folder'
        """
        super().__init__(auth, credentials, settings, **kwargs)

        self.account_name = settings.get('account_name')
        self.container = settings.get('container')
        self.base_folder = settings.get('base_folder', '/')
        self.auth_token = credentials.get('token')

        # Set BASE_URL for the parent class build_url method
        self.BASE_URL = f"https://{self.account_name}.blob.core.windows.net"

        if not self.account_name:
            raise ValueError("account_name is required")
        if not self.container:
            raise ValueError("container is required")
        if not self.auth_token:
            raise ValueError("token is required")

    def _get_blob_path(self, blob_name=None):
        if blob_name:
            return f"{self.base_folder}{blob_name}" if self.base_folder else blob_name
        return self.base_folder or '/'

    def build_url(self, *segments, **query):
        processed_segments = []

        for segment in segments:
            if isinstance(segment, str) and segment and segment != '/':
                if '/' in segment:
                    components = [comp for comp in segment.split('/') if comp]
                    processed_segments.extend(components)
                else:
                    processed_segments.append(segment)

        return super().build_url(*processed_segments, **query)

    @property
    def default_headers(self):
        return {
            'Authorization': f'Bearer {self.auth_token}',
            'x-ms-version': self.API_VERSION,
            'x-ms-client-request-id': str(uuid.uuid4()),
            'User-Agent': 'WaterButler-AzureBlobStorage/1.0',
            'x-ms-date': datetime.datetime.now().strftime('%a, %d %b %Y %H:%M:%S GMT')
        }

    async def validate_v1_path(self, path, **kwargs):
        """Validate path exists in storage."""
        if path == '/':
            return WaterButlerPath(path)

        implicit_folder = path.endswith('/')

        if implicit_folder:
            url = self.build_url(self.container)
            params = {
                'restype': 'container',
                'comp': 'list',
                'prefix': f"{self.base_folder + path.lstrip('/')}",
                'delimiter': '/',
                'maxresults': '1'  # String to avoid aiohttpretty TypeError with int params
            }

            resp = await self.make_request(
                'GET', url, params=params,
                expects=(200,),
                throws=exceptions.MetadataError
            )

            body = await resp.text()
            parsed = self._convert_xml_to_blob_list(body)
            if len(parsed['Blob']) == 0 and len(parsed['BlobPrefix']) == 0:
                raise exceptions.NotFoundError(str(path))
        else:
            url = self.build_url(self.container, self.base_folder, path)
            logger.info(f"Validating path: {path} with URL: {url}")
            resp = await self.make_request(
                'HEAD', url,
                expects=(200, 404),
                throws=exceptions.MetadataError
            )

            if resp.status == 404:
                raise exceptions.NotFoundError(str(path))

        return WaterButlerPath(path)

    async def validate_path(self, path, **kwargs):
        """Simple path validation."""
        return WaterButlerPath(path)

    def can_duplicate_names(self):
        return True

    def can_intra_copy(self, dest_provider, path=None):
        return False

    def can_intra_move(self, dest_provider, path=None):
        return False

    async def intra_copy(self, dest_provider, source_path, dest_path):
        raise NotImplementedError()

    async def download(self, path, accept_url=False, version=None, range=None, **kwargs):
        """Download a blob from Azure Storage."""
        if not path.is_file:
            raise exceptions.DownloadError('No file specified for download', code=400)

        clean_path = path.path[1:] if path.path.startswith('/') else path.path
        url = self.build_url(self.container, self._get_blob_path(clean_path))

        resp = await self.make_request(
            'GET', url,
            expects=(200, 206),
            range=range,
            throws=exceptions.DownloadError
        )

        return streams.ResponseStreamReader(resp)

    async def upload(self, stream, path, conflict='replace', block_id_prefix=None, **kwargs):
        """Upload a stream to Azure Blob Storage."""
        path, exists = await self.handle_name_conflict(path, conflict=conflict)

        if block_id_prefix is None:
            block_id_prefix = str(uuid.uuid4())

        # Use simple upload for small files, block upload for large files
        if stream.size <= self.CONTIGUOUS_UPLOAD_SIZE_LIMIT:
            await self._contiguous_upload(stream, path)
        else:
            await self._chunked_upload(stream, path, block_id_prefix)

        metadata = await self.metadata(path, **kwargs)
        return metadata, not exists

    async def _contiguous_upload(self, stream, path):
        """Upload small file in one request."""
        clean_path = path.path[1:] if path.path.startswith('/') else path.path
        url = self.build_url(self.container, self._get_blob_path(clean_path))

        stream.add_writer('md5', streams.HashStreamWriter(hashlib.md5))
        headers = {
            'Content-Length': str(stream.size),
            'x-ms-blob-type': 'BlockBlob'
        }

        await self.make_request(
            'PUT', url, headers=headers, data=stream,
            expects=(201,), throws=exceptions.UploadError
        )

    async def _chunked_upload(self, stream, path, block_id_prefix):
        """Upload large file using block upload."""
        block_id_list = []
        parts = [self.CHUNK_SIZE for i in range(0, stream.size // self.CHUNK_SIZE)]
        if stream.size % self.CHUNK_SIZE:
            parts.append(stream.size - (len(parts) * self.CHUNK_SIZE))

        # Upload each block
        for chunk_number, chunk_size in enumerate(parts):
            cutoff_stream = streams.CutoffStream(stream, cutoff=chunk_size)
            block_id = self._format_block_id(block_id_prefix, chunk_number)
            block_id_list.append(block_id)
            await self._put_block(cutoff_stream, path, block_id)

        # Commit block list
        await self._put_block_list(path, block_id_list)

    async def _put_block(self, stream, path, block_id):
        """Upload a single block."""
        clean_path = path.path[1:] if path.path.startswith('/') else path.path
        url = self.build_url(self.container, self._get_blob_path(clean_path))

        stream.add_writer('md5', streams.HashStreamWriter(hashlib.md5))
        params = {'comp': 'block', 'blockid': block_id}
        headers = {'Content-Length': str(stream.size)}

        await self.make_request(
            'PUT', url, headers=headers, params=params, data=stream,
            expects=(201,), throws=exceptions.UploadError
        )

    async def _put_block_list(self, path, block_id_list):
        """Commit block list to create blob."""
        xml_data = '<?xml version="1.0" encoding="utf-8"?><BlockList>'
        for block_id in block_id_list:
            xml_data += f'<Uncommitted>{block_id}</Uncommitted>'
        xml_data += '</BlockList>'

        clean_path = path.path[1:] if path.path.startswith('/') else path.path
        url = self.build_url(self.container, self._get_blob_path(clean_path))

        stream = StringStream(xml_data)
        params = {'comp': 'blocklist'}
        headers = {
            'Content-Length': str(stream.size),
            'Content-Type': 'application/xml'
        }

        await self.make_request(
            'PUT', url, headers=headers, params=params, data=stream,
            expects=(201,), throws=exceptions.UploadError
        )

    @staticmethod
    def _format_block_id(prefix, index):
        """Format block ID for Azure Storage."""
        block_string = f"{prefix}_{index:05d}"
        return base64.urlsafe_b64encode(block_string.encode('utf-8')).decode('utf-8')

    async def delete(self, path, confirm_delete=0, **kwargs):
        if path.is_root and confirm_delete != 1:
            raise exceptions.DeleteError(
                'confirm_delete=1 is required for deleting root provider folder',
                code=400
            )

        if path.is_file:
            await self._delete_blob(path)
        else:
            await self._delete_folder(path)

    async def _delete_blob(self, path):
        clean_path = path.path[1:] if path.path.startswith('/') else path.path
        url = self.build_url(self.container, self._get_blob_path(clean_path))

        await self.make_request(
            'DELETE', url,
            expects=(202, 404), throws=exceptions.DeleteError
        )

    async def _delete_folder(self, path, **kwargs):
        # List all blobs in the folder
        url = self.build_url(self.container)
        prefix = self._get_blob_path(path.path if not path.is_root else '')
        params = {
            'restype': 'container',
            'comp': 'list',
            'prefix': prefix
        }

        resp = await self.make_request(
            'GET', url, params=params,
            expects=(200,), throws=exceptions.DeleteError
        )

        body = await resp.text()
        blob_names = self._parse_blob_list(body)

        if not blob_names and not path.is_root:
            raise exceptions.DeleteError('Folder not found', code=404)

        # Delete each blob
        for blob_name in blob_names:
            url = self.build_url(self.container, blob_name)
            await self.make_request(
                'DELETE', url,
                expects=(202, 404), throws=exceptions.DeleteError
            )

    async def metadata(self, path, revision=None, **kwargs):
        if path.is_dir:
            metadata = await self._metadata_folder(path)
            for item in metadata:
                item.raw['base_folder'] = self.base_folder
        else:
            metadata = await self._metadata_file(path, revision=revision)
            metadata.raw['base_folder'] = self.base_folder

        return metadata

    async def _metadata_file(self, path, revision=None):
        if revision == 'Latest':
            revision = None

        clean_path = path.path[1:] if path.path.startswith('/') else path.path
        url = self.build_url(self.container, self._get_blob_path(clean_path))

        resp = await self.make_request(
            'HEAD', url,
            expects=(200,), throws=exceptions.MetadataError
        )

        return AzureBlobStorageFileMetadataHeaders(path.path, resp.headers)

    async def _metadata_folder(self, path):
        """Get metadata for a folder (list contents)."""
        url = self.build_url(self.container)
        prefix = self._get_blob_path(path.path if not path.is_root else '')
        items = []
        marker = None
        while True:
            params = {
                'restype': 'container',
                'comp': 'list',
                'prefix': prefix,
                'delimiter': '/',
            }
            if marker:
                params['marker'] = marker

            resp = await self.make_request(
                'GET', url, params=params,
                expects=(200,), throws=exceptions.MetadataError
            )

            body = await resp.text()
            parsed = self._convert_xml_to_blob_list(body)
            blobs = parsed.get('Blob', [])
            prefixes = parsed.get('BlobPrefix', [])

            if not blobs and not prefixes and marker is None and not path.is_root:
                raise exceptions.NotFoundError(str(path))

            for folder_md in [
                AzureBlobStorageFolderMetadata(item)
                for item in prefixes if item['Name'] != path.path
            ]:
                folder_md.raw['base_folder'] = self.base_folder
                items.append(folder_md)

            for blob in blobs:
                if blob['Name'].endswith('.osfkeep'):
                    continue
                file_md = AzureBlobStorageFileMetadata(blob)
                file_md.raw['base_folder'] = self.base_folder
                items.append(file_md)

            marker = parsed.get('NextMarker')
            if not marker:
                break

        return items

    async def create_folder(self, path, folder_precheck=True, **kwargs):
        """Create a folder by uploading a marker file."""
        WaterButlerPath.validate_folder(path)

        if folder_precheck:
            folder_exists = await self.exists(path)
            if folder_exists is not False:  # exists returns False for non-existent, [] for empty folder, or metadata for non-empty
                raise exceptions.FolderNamingConflict(path.name)
            file_exists = await self.exists(await self.validate_path('/' + path.path[:-1]))
            if file_exists is not False:
                raise exceptions.FolderNamingConflict(path.name)

        # Create .osfkeep file to represent the folder
        marker_path = path.path + '.osfkeep'
        clean_path = marker_path[1:] if marker_path.startswith('/') else marker_path
        url = self.build_url(self.container, self._get_blob_path(clean_path))

        headers = {
            'Content-Length': '0',
            'x-ms-blob-type': 'BlockBlob'
        }

        await self.make_request(
            'PUT', url, headers=headers, data=b'',
            expects=(201,), throws=exceptions.CreateFolderError
        )

        metadata = AzureBlobStorageFolderMetadata({'Name': path.path})
        metadata.raw['base_folder'] = self.base_folder
        return metadata

    async def revisions(self, path, **kwargs):
        return []

    def _parse_blob_list(self, xml_body, prefix=None):
        try:
            root = ET.fromstring(xml_body)
            blobs = []

            for blob_elem in root.findall('.//Blob'):
                name_elem = blob_elem.find('Name')
                if name_elem is not None:
                    blob_name = name_elem.text
                    if not prefix or blob_name.startswith(prefix):
                        blobs.append(blob_name)

            return blobs
        except ET.ParseError as e:
            logger.error(f"Failed to parse blob list XML: {e}")
            return []

    def _convert_xml_to_blob_list(self, xml_body):
        try:
            root = ET.fromstring(xml_body)

            blobs_elem = root.find('.//Blobs')
            if blobs_elem is None:
                return {}

            result = {
                'Prefix': root.find('.//Prefix').text if root.find('.//Prefix') is not None else '',
                'Delimiter': root.find('.//Delimiter').text if root.find('.//Delimiter') is not None else '',
                'Blob': [],
                'BlobPrefix': [],
                'NextMarker': root.find('.//NextMarker').text if root.find('.//NextMarker') is not None else None
            }

            for blob in blobs_elem.findall('Blob'):
                blob_data = {'Name': blob.find('Name').text if blob.find('Name') is not None else ''}

                properties = blob.find('Properties')
                if properties is not None:
                    props_dict = {}
                    for prop in properties:
                        if prop.text is not None:
                            props_dict[prop.tag] = prop.text
                    blob_data['Properties'] = props_dict

                result['Blob'].append(blob_data)

            for prefix in blobs_elem.findall('BlobPrefix'):
                name_elem = prefix.find('Name')
                if name_elem is not None:
                    result['BlobPrefix'].append({'Name': name_elem.text})
            logger.info(f'Parsed XML to blob list: {result}')
            return result

        except ET.ParseError as e:
            logger.error(f"Failed to parse XML with native parser: {e}")
            return {}
