import asyncio
import os
import itertools
import hashlib
import functools
from urllib import parse
import logging

import xmltodict

import xml.sax.saxutils

#import aioboto3
from boto import config as boto_config
from boto.compat import BytesIO  # type: ignore
from boto.utils import compute_md5
from boto.auth import get_auth_handler
from boto.s3.connection import S3Connection
from boto.s3.connection import OrdinaryCallingFormat


import boto3
from botocore.awsrequest import prepare_request_dict
from botocore.client import Config
from botocore.exceptions import (
    ClientError,
    UnknownClientMethodError
)
from botocore.signers import _should_use_global_endpoint


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


def generate_presigned_url(self, ClientMethod, Params=None, Headers=None, ExpiresIn=3600,
                           HttpMethod=None):
    """Generate a presigned url given a client, its method, and arguments
    :type ClientMethod: string
    :param ClientMethod: The client method to presign for
    :type Params: dict
    :param Params: The parameters normally passed to
        ``ClientMethod``.
    :type ExpiresIn: int
    :param ExpiresIn: The number of seconds the presigned url is valid
        for. By default it expires in an hour (3600 seconds)
    :type HttpMethod: string
    :param HttpMethod: The http method to use on the generated url. By
        default, the http method is whatever is used in the method's model.
    :returns: The presigned url
    """
    client_method = ClientMethod
    params = Params
    if params is None:
        params = {}
    # <patch>
    headers = Headers
    # </patch>
    expires_in = ExpiresIn
    http_method = HttpMethod
    context = {
        'is_presign_request': True,
        'use_global_endpoint': _should_use_global_endpoint(self),
    }

    request_signer = self._request_signer
    serializer = self._serializer

    try:
        operation_name = self._PY_TO_OP_NAME[client_method]
    except KeyError:
        raise UnknownClientMethodError(method_name=client_method)

    operation_model = self.meta.service_model.operation_model(
        operation_name)

    params = self._emit_api_params(params, operation_model, context)

    # Create a request dict based on the params to serialize.
    request_dict = serializer.serialize_to_request(
        params, operation_model)

    logger.info(headers)
    logger.info(request_dict)

    # Switch out the http method if user specified it.
    if http_method is not None:
        request_dict['method'] = http_method

    # <patch>
    if headers is not None:
        request_dict['headers'].update(headers)
    # </patch>
    # Prepare the request dict by including the client's endpoint url.
    prepare_request_dict(
        request_dict, endpoint_url=self.meta.endpoint_url, context=context)

    # Generate the presigned url.
    return request_signer.generate_presigned_url(
        request_dict=request_dict, expires_in=expires_in,
        operation_name=operation_name)


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
    _s3 = None
    _client = None
    _region = None

    def __init__(self, auth, credentials, settings):
        """Initialize S3Provider
        .. note::

            Neither `S3Connection#__init__` nor `S3Connection#get_bucket`
            sends a request.

        :param dict auth: Not used
        :param dict credentials: Dict containing `access_key` and `secret_key`
        :param dict settings: Dict containing `bucket`
        """
        super().__init__(auth, credentials, settings)

        self.credentials = credentials
        self.bucket_name = settings['bucket']
        self.encrypt_uploads = self.settings.get('encrypt_uploads', False)

    # TODO Move client creaation to `__aenter__`
    @property
    async def client(self):
        if self._client is None:
            # In order to make a client that we can use on any region, we need
            # to supply the client with a string of the region name. First we
            # make a temporary client in order to get that string. We put the
            # client creation inside a lmabda so its easier to call twice.
            # This must be a lambda an *not* a partial, because we want the
            # expression reevaluated each time.
            _make_client = lambda: boto3.client(
                's3',
                region_name=self._region,
                aws_access_key_id=self.credentials['access_key'],
                aws_secret_access_key=self.credentials['secret_key'],
                endpoint_url='http{}://{}:{}'.format(
                    's' if self.credentials['encrypted'] else '',
                    self.credentials['host'],
                    self.credentials['port']
                ) if self.credentials['host'] != 's3.amazonaws.com' else None
            )
            self._region = _make_client().get_bucket_location(
                Bucket=self.bucket_name
            ).get('LocationConstraint', None)
            # Remake client after getting bucket location
            self._client = _make_client()
            # Put the patched version of the url signer on the client.
            self._client.__class__.generate_presigned_url = generate_presigned_url
        return self._client

    @property
    def s3(self):
        if self._s3 is None:
            self._s3 = boto3.resource('s3')
        return self._s3

    @property
    async def region(self):
        # Awaiting self.client ensures the region is set properly; if we have a
        # client set on our provider, we know the region is correct because we
        # need the region in order to make the client.
        await self.client
        return self._region

    async def validate_v1_path(self, path, **kwargs):
        """Validates a waterbutler path
        """
        wb_path = WaterButlerPath(path)

        if path == '/':
            return wb_path

        implicit_folder = path.endswith('/')

        if implicit_folder:
            await self._metadata_folder(wb_path.path)
        else:
            await self._metadata_file(wb_path.path)

        return wb_path

    # Do we call this anywhere, and why can't we just use the constructor?
    async def validate_path(self, path, **kwargs):
        return WaterButlerPath(path)

    def can_duplicate_names(self):
        # TODO This should be a class attribute
        return True

    def can_intra_copy(self, dest_provider, path=None):
        return type(self) == type(dest_provider) and not getattr(path, 'is_dir', False)

    def can_intra_move(self, dest_provider, path=None):
        return type(self) == type(dest_provider) and not getattr(path, 'is_dir', False)

    async def intra_copy(self, dest_provider, source_path, dest_path):
        """Copy key from one S3 bucket to another. The credentials specified in
        `dest_provider` must have read access to `source.bucket`.
        """
        exists = await dest_provider.exists(dest_path)

        # TODO move this to `__aenter__`
        client = await self.client

        # ensure no left slash when joining paths
        source_path = '/' + os.path.join(self.settings['bucket'], source_path.path)
        headers = {'x-amz-copy-source': parse.quote(source_path)}

        sign_url = lambda: client.generate_presigned_url(
            'copy_object',
            Params={
                'Bucket': self.bucket_name,
                'CopySource': source_path,
                'Key': dest_path.path
            },
            ExpiresIn=settings.TEMP_URL_SECS,
        )
        response = await self.make_request(
            'PUT',
            sign_url,
            expects={200},
            skip_auto_headers={'CONTENT-TYPE'},
            headers=headers,
            throws=exceptions.IntraCopyError
        )
        await response.release()

        return (await dest_provider.metadata(dest_path)), not exists

    async def download(self, path, accept_url=False, revision=None, range=None, **kwargs):
        """Returns a ResponseWrapper (Stream) for the specified path
        raises FileNotFoundError if the status from S3 is not 200

        :param str path: Path to the key you want to download
        :param dict \*\*kwargs: Additional arguments that are ignored
        :rtype: :class:`waterbutler.core.streams.ResponseStreamReader`
        :raises: :class:`waterbutler.core.exceptions.DownloadError`
        """
        get_kwargs = {}

        if not path.is_file:
            raise exceptions.DownloadError('No file specified for download', code=400)

        if range:
            get_kwargs['Range'] = 'bytes={}-{}'.format('', '')

        # if kwargs.get('displayName'):
        #     get_kwargs['ResponseContentDisposition'] = 'attachment; filename*=UTF-8\'\'{}'.format(parse.quote(kwargs['displayName']))
        # else:
        #     get_kwargs['ResponseContentDisposition'] = 'attachment'

        if revision:
            get_kwargs['VersionId'] = revision

        # TODO move this to `__aenter__`
        client = await self.client

        sign_url = lambda: client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': self.bucket_name,
                'Key': path.path
            },
            ExpiresIn=settings.TEMP_URL_SECS,
            HttpMethod='GET'
        )

        response = await self.make_request(
            'GET',
            sign_url,
            range=range,
            expects={200, 206},
            throws=exceptions.DownloadError
        )
        return streams.ResponseStreamReader(response)

    async def upload(self, stream, path, conflict='replace', **kwargs):
        """Uploads the given stream to S3

        :param waterbutler.core.streams.RequestWrapper stream: The stream to put to S3
        :param str path: The full path of the key to upload to/into

        :rtype: dict, bool
        """
        path, exists = await self.handle_name_conflict(path, conflict=conflict)
        stream.add_writer('md5', streams.HashStreamWriter(hashlib.md5))

        # TODO move this to `__aenter__`
        client = await self.client
        headers = {'Content-Length': str(stream.size)}

        # this is usually set in boto.s3.key.generate_url, but do it here
        # do be explicit about our header payloads for signing purposes
        if self.encrypt_uploads:
            headers['x-amz-server-side-encryption'] = 'AES256'

        sign_url = lambda: client.generate_presigned_url(
            'put_object',
            Params={
                'Bucket': self.bucket_name,
                'Key': path.path,
                'ContentLength': stream.size,
                **({'ServerSideEncryption': 'AES256'} if self.encrypt_uploads else {})
            },
            ExpiresIn=settings.TEMP_URL_SECS,
        )

        response = await self.make_request(
            'PUT',
            sign_url,
            data=stream,
            skip_auto_headers={'CONTENT-TYPE'},
            headers=headers,
            expects={200, 206},
            throws=exceptions.DownloadError
        )
        await response.release()

        # md5 is returned as ETag header as long as server side encryption is not used.
        if stream.writers['md5'].hexdigest != response.headers['ETag'].replace('"', ''):
            raise exceptions.UploadChecksumMismatchError()

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
            await self._delete_file(path, **kwargs)
        else:
            await self._delete_folder(path, **kwargs)

    async def _delete_file(self, path, **kwargs):
        """Deletes a single object located at a certain key.

        Called from: func: delete if path.is_file
        """
        # TODO move this to `__aenter__`
        client = await self.client
        sign_url = lambda: client.generate_presigned_url(
            'delete_object',
            Params={
                'Bucket': self.bucket_name,
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
        # TODO move this to `__aenter__`
        client = await self.client

        # Needs to be a lambda; *not* partial, so offset is reevaluated
        # each time it's called. This is done so that we don't need to
        # create a new callable object each request we make.

        # The wierdness with using ** on the Params is because boto3 is
        # rather draconian about arguments; passing None for this param results
        # in an error that the None object is not of type string.

        # `marker` needs to be defined before the url signer so that it exists
        # when the url signer is defined. It is used for pagination, to
        # determine which page is returned by the request.
        marker = None
        sign_list_url = lambda: client.generate_presigned_url(
            'list_objects_v2',
            Params={
                'Bucket': self.bucket_name,
                'Prefix': path.path,
                'Delimiter': '/',
                **({'Marker': marker} if marker is not None else {})
            },
            ExpiresIn=settings.TEMP_URL_SECS
        )

        objects_to_delete = []

        delete_payload = ''
        sign_delete_url = lambda: client.generate_presigned_url(
            'delete_objects',
            Params={
                'Bucket': self.bucket_name,
                'Delete': {
                    'Objects': [{'Key': object['Key']} for object in objects_to_delete]
                }
            },
            Headers={
                'Content-Length': str(len(delete_payload)),
                'Content-MD5': compute_md5(BytesIO(delete_payload))[1],
                'Content-Type': 'text/xml'
            }
        )

        # S3 'truncates' responses that would list over 1000 objects. The
        # response will contain a key, 'IsTruncated', if there were more than
        # 1000 objects. Before the first request, we assume the list is
        # truncated, so that at least one request will be made.
        truncated = True
        while truncated:
            list_response = await self.make_request(
                'GET',
                sign_list_url,
                expects={200, 204},
                throws=exceptions.MetadataError,
            )
            page = xmltodict.parse(
                await list_response.read(),
                strip_whitespace=False,
                force_list={'Contents'}
            )
            marker = page['ListBucketResult'].get('NextMarker', None)
            truncated = page['ListBucketResult'].get('IsTruncated', 'false') != 'false'

            objects_to_delete = page['ListBucketResult'].get('Contents', [])

            delete_payload = '<?xml version="1.0" encoding="UTF-8"?><Delete>{}</Delete>'.format(
                ''.join([
                    '<Object><Key>{}</Key></Object>'.format(object['Key'])
                    for object in objects_to_delete
                ])
            ).encode('utf-8')

            # TODO Don't wait for the delete to finish before requesting the
            # next batch, or sending that delete request.
            delete_response = await self.make_request(
                'POST',
                sign_delete_url,
                data=delete_payload,
                headers={
                    'Content-Length': str(len(delete_payload)),
                    'Content-MD5': compute_md5(BytesIO(delete_payload))[1],
                    'Content-Type': 'text/xml'
                }
            )
            await delete_response.release()
            del delete_response
            del list_response

        # TODO Put the delete requests in a list of tasks and wait for all of them to
        # finish here, before returning

    async def revisions(self, path, **kwargs):
        """Get past versions of the requested key

        :param str path: The path to a key
        :rtype list:
        """
        # TODO move this to `__aenter__`
        client = await self.client
        sign_url = lambda: client.generate_signed_url(
            'list_object_versions',
            Params={
                'Bucket': self.bucket_name,
                'Delimiter': '/',
                'Prefix': path.path
            }
        )

        response = await self.make_request(
            'POST',
            sign_url,
            expects={200},
            throws=exceptions.MetadataError
        )

        versions = xmltodict.parse(
            await response.release(),
            force_list={'Version'}
        )['ListVersionsResult'].get('Version', [])

        return [
            S3Revision(item)
            for item in versions
            if item['Key'] == path.path
        ]

    async def create_folder(self, path, folder_precheck=True, **kwargs):
        """Create an empty object on the bucket that contains a trailing slash
        :param str path: The path to create a folder at
        """
        # TODO move this to `__aenter__`
        client = await self.client
        WaterButlerPath.validate_folder(path)

        if folder_precheck:
            if (await self.exists(path)):
                raise exceptions.FolderNamingConflict(path.name)

        sign_url = lambda: client.generate_presigned_url(
            'put_object',
            Params={
                'Bucket': self.bucket_name,
                'Key': path,
            },
            ExpiresIn=settings.TEMP_URL_SECS,
        )
        async with self.request(
            'PUT',
            sign_url,
            skip_auto_headers={'CONTENT-TYPE'},
            expects={200, 201},
            throws=exceptions.CreateFolderError,
        ):
            return S3FolderMetadata({'Prefix': path.path})

    async def metadata(self, path, revision=None, **kwargs):
        """Get Metadata about the requested file or folder

        :param WaterButlerPath path: The path to a key or folder
        :rtype: dict or list
        """
        if path.is_dir:
            return (await self._metadata_folder(path.path))
            #  store a hash of these args and the result in redis?

        return (await self._metadata_file(path.path, revision=revision))

    async def _metadata_file(self, path, revision=None):
        """Load metadata for a single object in the bucket.
        """
        # TODO move this to `__aenter__`
        client = await self.client

        # Homogenise any weird version ids
        if any({
            revision == 'Latest',
            revision == '',
            not revision
        }):
            revision = None

        sign_url = lambda: client.generate_presigned_url(
            'head_object',
            Params={
                'Bucket': self.bucket_name,
                'Key': path,
                **({'VersionId': revision} if revision is not None else {})
            },
            ExpiresIn=settings.TEMP_URL_SECS,
        )
        response = await self.make_request(
            'HEAD',
            sign_url,
            expects={200, 204},
            throws=exceptions.MetadataError,
        )
        await response.release()

        return S3FileMetadataHeaders(
            path,
            headers=response.headers  # TODO Fix S3MetadataFileHeaders
        )

    async def _metadata_folder(self, path):
        """Get metadata about the contents of a bucket. This is either the
        contents at the root of the bucket, or a folder has
        been selected as a prefix by the user
        """
        # TODO move this to `__aenter__`
        client = await self.client

        # Needs to be a lambda; *not* partial, so offset is reevaluated
        # each time it's called. This is done so that we don't need to
        # create a new callable object each request we make.

        # The wierdness with using ** on the Params is because boto3 is
        # rather draconian about arguments; passing None for this param results
        # in an error that the None object is not of type string.

        # `marker` needs to be defined before the url signer so that it exists
        # when the url signer is defined. It is used for pagination, to
        # determine which page is returned by the request.
        marker = None
        sign_url = lambda: client.generate_presigned_url(
            'list_objects_v2',
            Params={
                'Bucket': self.bucket_name,
                'Prefix': path,
                'Delimiter': '/',
                **({'Marker': marker} if marker is not None else {})
            },
            ExpiresIn=settings.TEMP_URL_SECS
        )

        # S3 'truncates' responses that would list over 1000 objects. The
        # response will contain a key, 'IsTruncated', if there were more than
        # 1000 objects. Before the first request, we assume the list is
        # truncated, so that at least one request will be made.
        truncated = True

        # Each request will return 0 or more 'contents' and 'common prefixes'.
        # Contents contains keys that begin with 'prefix' and contain no
        # delimiter characters after the characters that match the prefix.
        # Common prefixes match any keys that do contain a delimiter after the
        # characters that match the prefix. Each request extends the `contents`
        # and `prefixes` arrays with the respective contents and prefixes that
        # were returned in the request.
        contents = []
        prefixes = []

        while truncated:
            response = await self.make_request(
                'GET',
                sign_url,
                expects={200, 204},
                throws=exceptions.MetadataError,
            )
            page = xmltodict.parse(
                await response.read(),
                strip_whitespace=False,
                force_list={'CommonPrefixes', 'Contents'}
            )
            prefixes.extend(page['ListBucketResult'].get('CommonPrefixes', []))
            contents.extend(page['ListBucketResult'].get('Contents', []))

            marker = page['ListBucketResult'].get('NextMarker', None)
            truncated = page['ListBucketResult'].get('IsTruncated', 'false') != 'false'
            del response

        del sign_url

        items = []
        # If there are keys that have the provided prefix...
        if contents or prefixes:
            # Prefixes represent 'folders'
            items.extend([S3FolderMetadata(prefix) for prefix in prefixes])

            for content in contents:
                # Only care about items that are not the same as where the
                # addon is mounted.
                if content['Key'] != path:
                    items.append(
                        S3FolderKeyMetadata(content)
                        if content['Key'].endswith('/')
                        else S3FileMetadata(content)
                    )

        # If contents and prefixes are empty, but this is not the root
        # path, then this "folder" must exist as a key with a / at the
        # end of the name.
        elif not path == "":
            sign_url = lambda: client.generate_presigned_url(
                'head_object',
                Params={
                    'Bucket': self.bucket_name,
                    'Key': path,
                },
                ExpiresIn=settings.TEMP_URL_SECS
            )
            response = await self.make_request(
                'HEAD',
                sign_url,
                expects={200, 204},
                throws=exceptions.MetadataError,
            )
            del sign_url
            del response

        return items


