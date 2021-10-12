import hashlib
import re
import logging

import xmltodict

import boto3
from botocore.exceptions import ClientError
# from boto3 import exception

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.s3compatb3 import settings
from waterbutler.providers.s3compatb3.metadata import S3CompatB3Revision
from waterbutler.providers.s3compatb3.metadata import S3CompatB3FileMetadata
from waterbutler.providers.s3compatb3.metadata import S3CompatB3FolderMetadata
from waterbutler.providers.s3compatb3.metadata import S3CompatB3FolderKeyMetadata
from waterbutler.providers.s3compatb3.metadata import S3CompatB3FileMetadataHeaders

logger = logging.getLogger(__name__)


class S3CompatB3Connection:
    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None,
                endpoint_url=None, region_name=None):
        self.s3 = boto3.resource(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
            endpoint_url=endpoint_url
        )
        self.endpoint_url = endpoint_url
        self.region_name = region_name

    def generate_presigned_url(self, ClientMethod, Params=None, ExpiresIn=3600, HttpMethod=None):
        return self.s3.meta.client.generate_presigned_url(ClientMethod, Params=Params, ExpiresIn=ExpiresIn, HttpMethod=HttpMethod)


class S3CompatB3Provider(provider.BaseProvider):
    """Provider for S3 Compatible Storage service.

    API docs: http://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html

    Quirks:

    * On S3, folders are not first-class objects, but are instead inferred
      from the names of their children.  A regular DELETE request issued
      against a folder will not work unless that folder is completely empty.
      To fully delete an occupied folder, we must delete all of the comprising
      objects.  Amazon provides a bulk delete operation to simplify this.

    * A GET prefix query against a non-existent path returns 200
    """
    NAME = 's3compatb3'

    def __init__(self, auth, credentials, settings, **kwargs):
        """
        .. note::

            Neither `S3CompatB3Connection#__init__` nor `S3CompatB3Connection#get_bucket`
            sends a request.

        :param dict auth: Not used
        :param dict credentials: Dict containing `access_key` and `secret_key`
        :param dict settings: Dict containing `bucket`
        """
        super().__init__(auth, credentials, settings)

        host = credentials['host']
        port = 443
        m = re.match(r'^(.+)\:([0-9]+)$', host)
        if m is not None:
            host = m.group(1)
            port = int(m.group(2))
        region = ''
        if host.endswith('.oraclecloud.com'):
            region = host.split('.')[-3]
        endpoint_url = ('https://' if port == 443 else 'http://') + host
        self.connection = S3CompatB3Connection(credentials['access_key'],
                                             credentials['secret_key'],
                                             endpoint_url, region)
        self.bucket = self.connection.s3.Bucket(settings['bucket'])
        self.encrypt_uploads = self.settings.get('encrypt_uploads', False)
        self.prefix = settings.get('prefix', '')

    async def validate_v1_path(self, path, **kwargs):
        wbpath = WaterButlerPath(path, prepend=self.prefix)
        if path == '/':
            return wbpath

        implicit_folder = path.endswith('/')

        if implicit_folder:
            objects = self.bucket.objects.filter(Prefix=wbpath.full_path, Delimiter='/')
            if len(list(objects)) == 0:
                raise exceptions.NotFoundError(str(wbpath.full_path))

        else:
            try:
                self.bucket.Object(wbpath.full_path).metadata
            except ClientError:
                raise exceptions.NotFoundError(str(wbpath.full_path))

        return wbpath

    async def validate_path(self, path, **kwargs):
        return WaterButlerPath(path, prepend=self.prefix)

    def can_duplicate_names(self):
        return True

    def can_intra_copy(self, dest_provider, path=None):
        return type(self) == type(dest_provider) and not getattr(path, 'is_dir', False)

    def can_intra_move(self, dest_provider, path=None):
        return type(self) == type(dest_provider) and not getattr(path, 'is_dir', False)

    async def intra_copy(self, dest_provider, source_path, dest_path):
        """Copy key from one S3 Compatible Storage bucket to another. The credentials specified in
        `dest_provider` must have read access to `source.bucket`.
        """
        exists = await dest_provider.exists(dest_path)
        copy_source = {'Bucket': self.bucket.name, 'Key': source_path.full_path}

        dest_provider.bucket.Object(dest_path.full_path).copy_from(CopySource=copy_source)

        return (await dest_provider.metadata(dest_path)), not exists

    async def download(self, path, accept_url=False, version=None, range=None, **kwargs):
        """Returns a ResponseWrapper (Stream) for the specified path
        raises FileNotFoundError if the status from S3 is not 200

        :param str path: Path to the key you want to download
        :param dict \*\*kwargs: Additional arguments that are ignored
        :rtype: :class:`waterbutler.core.streams.ResponseStreamReader`
        :raises: :class:`waterbutler.core.exceptions.DownloadError`
        """
        if not path.is_file:
            raise exceptions.DownloadError('No file specified for download', code=400)

        # if kwargs.get('displayName'):
        #     response_headers = {'response-content-disposition': 'attachment; filename*=UTF-8\'\'{}'.format(parse.quote(kwargs['displayName']))}
        # else:
        #     response_headers = {'response-content-disposition': 'attachment'}

        headers = {}
        query_parameters = {'Bucket': self.bucket.name, 'Key': path.full_path}
        if version and version.lower() != 'latest':
            query_parameters['VersionId'] = version

        raw_url = self.connection.generate_presigned_url('get_object', Params=query_parameters, ExpiresIn=settings.TEMP_URL_SECS, HttpMethod='GET')

        resp = await self.make_request(
            'GET',
            raw_url,
            range=range,
            headers=headers,
            expects=(200, 206),
            throws=exceptions.DownloadError,
        )

        return streams.ResponseStreamReader(resp)

    async def upload(self, stream, path, conflict='replace', **kwargs):
        """Uploads the given stream to S3 Compatible Storage

        :param waterbutler.core.streams.RequestWrapper stream: The stream to put to S3 Compatible Storage
        :param str path: The full path of the key to upload to/into

        :rtype: dict, bool
        """
        path, exists = await self.handle_name_conflict(path, conflict=conflict)
        stream.add_writer('md5', streams.HashStreamWriter(hashlib.md5))

        headers = {'Content-Length': str(stream.size)}

        # this is usually set in boto.s3.key.generate_url, but do it here
        # do be explicit about our header payloads for signing purposes
        if self.encrypt_uploads:
            headers['x-amz-server-side-encryption'] = 'AES256'

        query_parameters = {'Bucket': self.bucket.name, 'Key': path.full_path}
        upload_url = self.connection.generate_presigned_url('put_object', Params=query_parameters, ExpiresIn=settings.TEMP_URL_SECS, HttpMethod='PUT')

        resp = await self.make_request(
            'PUT',
            upload_url,
            data=stream,
            skip_auto_headers={'CONTENT-TYPE'},
            headers=headers,
            expects=(200, 201, ),
            throws=exceptions.UploadError,
        )
        # md5 is returned as ETag header as long as server side encryption is not used.
        # TODO: nice assertion error goes here
        # assert resp.headers['ETag'].replace('"', '') == stream.writers['md5'].hexdigest

        await resp.release()
        return (await self.metadata(path, **kwargs)), not exists

    async def delete(self, path, confirm_delete=0, **kwargs):
        """Deletes the key at the specified path

        :param str path: The path of the key to delete
        :param int confirm_delete: Must be 1 to confirm root folder delete
        """
        if path.is_root:
            if not confirm_delete == 1:
                raise exceptions.DeleteError(
                    'confirm_delete=1 is required for deleting root provider folder',
                    code=400
                )

        if path.is_file:
            query_parameters = {'Bucket': self.bucket.name, 'Key': path.full_path}
            delete_url = self.connection.generate_presigned_url('delete_object', Params=query_parameters, ExpiresIn=settings.TEMP_URL_SECS, HttpMethod='DELETE')
            resp = await self.make_request(
                'DELETE',
                delete_url,
                expects=(200, 204, ),
                throws=exceptions.DeleteError,
            )
            await resp.release()
        else:
            await self._delete_folder(path, **kwargs)

    async def _folder_prefix_exists(self, folder_prefix):
        objects = self.bucket.objects.filter(Prefix=folder_prefix.rstrip('/')).limit(1)
        object_count = len(list(objects))
        is_exists = True if object_count > 0 else False
        return is_exists

    async def _delete_folder(self, path, **kwargs):
        """Query for recursive contents of folder and delete in batches of 1000

        Called from: func: delete if not path.is_file

        Calls: func: self.make_request
               func: self.bucket.generate_url

        :param *ProviderPath path: Path to be deleted

        On S3, folders are not first-class objects, but are instead inferred
        from the names of their children.  A regular DELETE request issued
        against a folder will not work unless that folder is completely empty.
        To fully delete an occupied folder, we must delete all of the comprising
        objects.  Amazon provides a bulk delete operation to simplify this.
        """
        if not path.full_path.endswith('/'):
            raise exceptions.InvalidParameters('not a folder: {}'.format(str(path)))

        prefix = path.full_path.lstrip('/')
        contents = self.bucket.objects.filter(Prefix=path.full_path)
        content_keys = [content.key for content in contents]

        # Query against non-existant folder does not return 404
        if len(content_keys) == 0:
            # MinIO cannot return Contents with a leaf folder itself.
            if await self._folder_prefix_exists(prefix):
                content_keys = [prefix]
            else:
                raise exceptions.NotFoundError(str(path))

        for content_key in content_keys[::-1]:
            query_parameters = {'Bucket': self.bucket.name, 'Key': content_key}
            delete_url = self.connection.generate_presigned_url('delete_object', Params=query_parameters, ExpiresIn=settings.TEMP_URL_SECS, HttpMethod='DELETE')
            resp = await self.make_request(
                'DELETE',
                delete_url,
                expects=(200, 204, ),
                throws=exceptions.DeleteError,
            )
            await resp.release()

    async def revisions(self, path, **kwargs):
        """Get past versions of the requested key

        :param str path: The path to a key
        :rtype list:
        """
        prefix = path.full_path.lstrip('/')  # '/' -> '', '/A/B' -> 'A/B'

        query_parameters = {'Bucket': self.bucket.name, 'Prefix': prefix, 'Delimiter': '/'}
        url = self.connection.generate_presigned_url('list_object_versions', Params=query_parameters, ExpiresIn=settings.TEMP_URL_SECS, HttpMethod='GET')
        try:
            resp = await self.make_request(
                'GET',
                url,
                expects=(200, ),
                throws=exceptions.MetadataError,
            )
        except exceptions.MetadataError as e:
            # MinIO may not support "versions" from generate_url() of boto2.
            # (And, MinIO does not support ListObjectVersions yet.)
            logger.info('ListObjectVersions may not be supported: url={}: {}'.format(url(), str(e)))
            return []

        content = await resp.read()
        xml = xmltodict.parse(content)
        versions = xml['ListVersionsResult'].get('Version') or []

        if isinstance(versions, dict):
            versions = [versions]

        return [
            S3CompatB3Revision(item)
            for item in versions
            if item['Key'] == prefix
        ]

    async def metadata(self, path, revision=None, **kwargs):
        """Get Metadata about the requested file or folder

        :param WaterButlerPath path: The path to a key or folder
        :rtype: dict or list
        """
        if path.is_dir:
            return (await self._metadata_folder(path))

        return (await self._metadata_file(path, revision=revision))

    async def create_folder(self, path, folder_precheck=True, **kwargs):
        """
        :param str path: The path to create a folder at
        """
        WaterButlerPath.validate_folder(path)

        if folder_precheck:
            if (await self.exists(path)):
                raise exceptions.FolderNamingConflict(path.name)

        self.bucket.put_object(Key=path.full_path, Body='')
        return S3CompatB3FolderMetadata(self, {'Prefix': path.full_path})

    async def _metadata_file(self, path, revision=None):
        if revision is None or revision == 'Latest':
            revision = 'null'
        try:
            resp = self.connection.s3.meta.client.head_object(Bucket=self.bucket.name, Key=path.full_path)
        except ClientError as e:
            raise exceptions.MetadataError(str(path.full_path), code=int(e.response['Error']['Code']))

        return S3CompatB3FileMetadataHeaders(self, path.full_path, resp)

    async def _metadata_folder(self, path):
        logger.info('_metadata_folder: {}:'.format(path.full_path))
        prefix = path.full_path.lstrip('/')  # '/' -> '', '/A/B' -> 'A/B'

        resp = self.connection.s3.meta.client.list_objects_v2(Bucket=self.bucket.name, Prefix=prefix, Delimiter='/')
        contents = resp.get('Contents', [])
        prefixes = resp.get('CommonPrefixes', [])

        if len(list(contents)) == 0 and path.full_path != '/':
            raise exceptions.NotFoundError(str(path.full_path))

        items = [
            S3CompatB3FolderMetadata(self, item)
            for item in prefixes
        ]

        for content in contents:
            logger.info('_metadata_folder: content: {}: {}'.format(content['Key'], prefix))
            if content['Key'].lstrip('/') == prefix:  # self
                logger.info('_metadata_folder: self: ---')
                continue

            if content['Key'].endswith('/'):
                logger.info('_metadata_folder: folder: ---')
                items.append(S3CompatB3FolderKeyMetadata(self, content))
            else:
                logger.info('_metadata_folder: file: ---')
                items.append(S3CompatB3FileMetadata(self, content))

        return items
