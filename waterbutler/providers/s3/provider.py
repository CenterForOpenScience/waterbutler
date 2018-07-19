import os
import itertools
import hashlib
import functools
from urllib import parse
import logging

import xmltodict

import xml.sax.saxutils

from boto import config as boto_config
from boto.compat import BytesIO  # type: ignore
from boto.utils import compute_md5
from boto.auth import get_auth_handler
from boto.s3.connection import S3Connection
from boto.s3.connection import OrdinaryCallingFormat

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.s3 import settings
from waterbutler.providers.s3.streams import S3ResponseBodyStream
from waterbutler.providers.s3.metadata import S3Revision
from waterbutler.providers.s3.metadata import S3FileMetadata
from waterbutler.providers.s3.metadata import S3FolderMetadata
from waterbutler.providers.s3.metadata import S3FolderKeyMetadata
from waterbutler.providers.s3.metadata import S3FileMetadataHeaders

logger = logging.getLogger(__name__)



class S3Provider(provider.BaseProvider):
    """Provider for Amazon's S3 cloud storage service.

    API docs: http://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html

    Quirks:

    * On S3, folders are not first-class objects, but are instead inferred
      from the names of their children.  A regular DELETE request issued
      against a folder will not work unless that folder is completely empty.
      To fully delete an occupied folder, we must delete all of the comprising
      objects.  Amazon provides a bulk delete operation to simplify this.

    * A GET prefix query against a non-existent path returns 200
    """
    NAME = 's3'

    def __init__(self, auth, credentials, settings):
        """Initialize S3Provider
        .. note::

            Neither `S3Connection#__init__` nor `S3Connection#get_bucket`
            sends a request.

        :param dict auth: Not used
        :param dict credentials: Dict containing `access_key` and `secret_key`
        :param dict settings: Dict containing `bucket`
        """
        logger.info("__init__")
        super().__init__(auth, credentials, settings)

        logger.info("About to create resource")
        self.s3 = boto3.resource(
            's3',
            endpoint_url='http{}://{}:{}'.format(
                's' if credentials['encrypted'] else '',
                credentials['host'],
                credentials['port']
            ),
            aws_access_key_id=credentials['access_key'],
            aws_secret_access_key=credentials['secret_key'],
        )
        logger.info("Resource created")

        logger.info("About to create bucket")
        self.bucket = self.s3.Bucket(settings['bucket'])
        logger.info("About to load bucket")
        self.bucket.load()
        logger.info("Bucket loaded")

        self.bucket_name = settings['bucket']
        self.encrypt_uploads = self.settings.get('encrypt_uploads', False)
        self.encrypt_uploads = False
        self.region = None

    async def validate_v1_path(self, path, **kwargs):

        logger.info("validate_v1_path")
        wb_path = WaterButlerPath(path)

        if path == '/':
            return wb_path

        implicit_folder = path.endswith('/')

        if implicit_folder:
            await self._metadata_folder(wb_path.path)
        else:
            await self._metadata_file(wb_path.path)

        return wb_path

    async def validate_path(self, path, **kwargs):
        logger.info("validate_path")
        return WaterButlerPath(path)

    def can_duplicate_names(self):
        logger.info("can_duplicate_names")
        return True

    def can_intra_copy(self, dest_provider, path=None):
        logger.info("can_intra_copy")
        return type(self) == type(dest_provider) and not getattr(path, 'is_dir', False)

    def can_intra_move(self, dest_provider, path=None):
        logger.info("can_intra_move")
        return type(self) == type(dest_provider) and not getattr(path, 'is_dir', False)

    async def intra_copy(self, dest_provider, source_path, dest_path):
        """Copy key from one S3 bucket to another. The credentials specified in
        `dest_provider` must have read access to `source.bucket`.
        """
        logger.info("intra_copy")
        exists = await dest_provider.exists(dest_path)

        dest_key = dest_provider.bucket.new_key(dest_path.path)

        # ensure no left slash when joining paths
        source_path = '/' + os.path.join(self.settings['bucket'], source_path.path)
        headers = {'x-amz-copy-source': parse.quote(source_path)}
        url = functools.partial(
            dest_key.generate_url,
            settings.TEMP_URL_SECS,
            'PUT',
            headers=headers,
        )
        resp = await self.make_request(
            'PUT', url,
            skip_auto_headers={'CONTENT-TYPE'},
            headers=headers,
            expects=(200, ),
            throws=exceptions.IntraCopyError,
        )
        await resp.release()
        return (await dest_provider.metadata(dest_path)), not exists

    async def download(self, path, accept_url=False, revision=None, range=None, **kwargs):
        """Returns a ResponseWrapper (Stream) for the specified path
        raises FileNotFoundError if the status from S3 is not 200

        :param str path: Path to the key you want to download
        :param dict \*\*kwargs: Additional arguments that are ignored
        :rtype: :class:`waterbutler.core.streams.ResponseStreamReader`
        :raises: :class:`waterbutler.core.exceptions.DownloadError`
        """
        logger.info("download")
        get_kwargs = {}

        if not path.is_file:
            raise exceptions.DownloadError('No file specified for download', code=400)

        if range:
            get_kwargs['Range'] = 'bytes={}-{}'.format('', '')

        if kwargs.get('displayName'):
            get_kwargs['ResponseContentDisposition'] = 'attachment; filename*=UTF-8\'\'{}'.format(parse.quote(kwargs['displayName']))
        else:
            get_kwargs['ResponseContentDisposition'] = 'attachment'

        if revision:
            get_kwargs['VersionId'] = revision

        try:
            res = self.s3.Object(
                self.bucket.name,
                path.path
            ).get(**get_kwargs)
        except:
            raise exceptions.DownloadError()

        return S3ResponseBodyStream(res)

    async def upload(self, stream, path, conflict='replace', **kwargs):
        """Uploads the given stream to S3

        :param waterbutler.core.streams.RequestWrapper stream: The stream to put to S3
        :param str path: The full path of the key to upload to/into

        :rtype: dict, bool
        """
        logger.info("upload")
        path, exists = await self.handle_name_conflict(path, conflict=conflict)
        stream.add_writer('md5', streams.HashStreamWriter(hashlib.md5))

        headers = {'Content-Length': str(stream.size)}

        # this is usually set in boto.s3.key.generate_url, but do it here
        # do be explicit about our header payloads for signing purposes
        if self.encrypt_uploads:
            headers['x-amz-server-side-encryption'] = 'AES256'

        resp = self.s3.Object(
            self.bucket_name,
            path.path
        ).put(
            Body=(await stream.read())  # Needs to calculate hash inside boto so can't do a request manually? - some issue with not implementing buffer api.
        )

        # md5 is returned as ETag header as long as server side encryption is not used.
        if stream.writers['md5'].hexdigest != resp['ETag'].replace('"', ''):
            raise exceptions.UploadChecksumMismatchError()

        return (await self.metadata(path, **kwargs)), not exists

    async def delete(self, path, confirm_delete=0, **kwargs):
        """Deletes the key at the specified path

        :param str path: The path of the key to delete
        :param int confirm_delete: Must be 1 to confirm root folder delete
        """
        logger.info("delete")
        if path.is_root:
            if not confirm_delete == 1:
                raise exceptions.DeleteError(
                    'confirm_delete=1 is required for deleting root provider folder',
                    code=400
                )

        if path.is_file:
            await self._delete_file(path, **kwargs)
        else:
            await self._delete_folder(path, **kwargs)

    async def _delete_file(self, path, **kwargs):
        """Deletes a single object located at a certain key.

        Called from: func: delete if path.is_file

        """
        logger.info("_delete_file")
        sign_url = lambda: self.bucket.meta.client.generate_presigned_url(
            'delete_object',
            Params={
                'Bucket': self.bucket.name,
                'Key': path.path
            },
            ExpiresIn=settings.TEMP_URL_SECS,
            HttpMethod='DELETE',
        )
        resp = await self.make_request(
            'DELETE',
            sign_url,
            expects={200, 204},
            throws=exceptions.DeleteError,
        )
        await resp.release()

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
        logger.info("_delete_folder")
        for page in self.s3.meta.client.get_paginator('list_objects').paginate(
            Bucket=self.bucket.name,
            Prefix=path.path
        ):
            self.s3.meta.client.delete_objects(  # Signing a delete_objects url with boto3 requires witchcraft
                Bucket=self.bucket.name,
                Delete={
                    'Objects': [{'Key': item['Key']} for item in page['Contents']]
                }
            )

    async def revisions(self, path, **kwargs):
        """Get past versions of the requested key

        :param str path: The path to a key
        :rtype list:
        """
        logger.info("revisions")
        try:
            resp = self.bucket.meta.client.list_object_versions(
                Bucket=self.bucket.name,
                Delimiter='/',
                Prefix=path.path
            )
            versions = resp['Versions']

            return [
                S3Revision(item)
                for item in versions
                if item['Key'] == path.path
            ]

        except Exception as err:
            logger.info(err)
            return []

    async def metadata(self, path, revision=None, **kwargs):
        """Get Metadata about the requested file or folder

        :param WaterButlerPath path: The path to a key or folder
        :rtype: dict or list
        """
        logger.info("metadata")

        if path.is_dir:
            return (await self._metadata_folder(path.path))
            #    #  store a hash of these args and the result in redis?

        return (await self._metadata_file(path.path, revision=revision))

    async def create_folder(self, path, folder_precheck=True, **kwargs):
        """
        :param str path: The path to create a folder at
        """
        logger.info("create_folder")
        WaterButlerPath.validate_folder(path)

        if folder_precheck:
            # We should have already validated the path at this point? - we
            # should store the value so when we're here we dont make an extra
            # request.
            if (await self.exists(path)):
                raise exceptions.FolderNamingConflict(path.name)

        self.bucket.meta.client.put_object(
            Bucket=self.bucket.name,
            Key=path.path
        )
        return S3FolderMetadata({'Prefix': path.path})

    async def _metadata_file(self, path, revision=None):
        """Load metadata for a single object in the bucket.
        """
        logger.info("_metadata_file")
        if (
            revision == 'Latest' or
            revision == '' or
            not revision
        ):
            obj = self.s3.Object(
                self.bucket.name,
                path
            )
        else:
            obj = self.s3.ObjectVersion(
                self.bucket.name,
                path,
                revision
            )
        try:
            logger.info("About to load")
            obj.load()
            logger.info("After load")
        except ClientError as err:
            if err.response['Error']['Code'] == '404':
                raise exceptions.NotFoundError(path)
            else:
                raise err

        return S3FileMetadataHeaders(path, obj)

    async def _metadata_folder(self, path):
        """Get metadata about the contents of a bucket. This is either the
        contents at the root of the bucket, or a folder has
        been selected as a prefix by the user
        """
        logger.info("_metadata_folder")
        result = self.bucket.meta.client.list_objects(Bucket=self.bucket.name, Prefix=path, Delimiter='/')
        prefixes = result.get('CommonPrefixes', [])
        contents = result.get('Contents', [])
        if not contents and not prefixes and not path == "":
            # If contents and prefixes are empty then this "folder"
            # must exist as a key with a / at the end of the name
            # if the path is root there is no need to test if it exists

            obj = self.s3.Object(self.bucket.name, path)
            try:
                obj.load()
            except ClientError as err:
                if err.response['Error']['Code'] == '404':
                    raise exceptions.NotFoundError(path)
                else:
                    raise err
        if isinstance(contents, dict):
            contents = [contents]

        if isinstance(prefixes, dict):
            prefixes = [prefixes]

        items = [S3FolderMetadata(item) for item in prefixes]

        for content in contents:
            if content['Key'] == path:
                continue

            if content['Key'].endswith('/'):
                items.append(S3FolderKeyMetadata(content))
            else:
                items.append(S3FileMetadata(content))

        return items
