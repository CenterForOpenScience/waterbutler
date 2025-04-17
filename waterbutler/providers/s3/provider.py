import base64
import os
import hashlib
import logging
import functools
from urllib import parse

import xmltodict
import xml.sax.saxutils
from aiobotocore.session import get_session # type: ignore
from waterbutler.providers.s3 import settings
from waterbutler.core.path import WaterButlerPath
from waterbutler.core.utils import make_disposition
from waterbutler.core import streams, provider, exceptions
from waterbutler.providers.s3.metadata import (S3Revision,
                                               S3FileMetadata,
                                               S3FolderMetadata,
                                               S3FolderKeyMetadata,
                                               S3FileMetadataHeaders,
                                               )

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
    CHUNK_SIZE = settings.CHUNK_SIZE
    CONTIGUOUS_UPLOAD_SIZE_LIMIT = settings.CONTIGUOUS_UPLOAD_SIZE_LIMIT

    def __init__(self, auth, credentials, settings, **kwargs):
        """
        .. note::

            Neither `S3Connection#__init__` nor `S3Connection#get_bucket`
            sends a request.

        :param dict auth: Not used
        :param dict credentials: Dict containing `access_key` and `secret_key`
        :param dict settings: Dict containing `bucket`
        """
        super().__init__(auth, credentials, settings, **kwargs)

        self.aws_secret_access_key = credentials['secret_key']
        self.aws_access_key_id = credentials['access_key']
        self.bucket_name = settings['bucket']
        self.base_folder = self.settings.get('id', ':/').split(':/')[1]
        self.encrypt_uploads = self.settings.get('encrypt_uploads', False)
        self.region = None


    async def check_key_existence(self, path, query_parameters=None):
        try:
            session = get_session()
            region_name = {"region_name": self.region} if self.region else {}
            query_parameters = query_parameters or {}
            async with session.create_client(
                    's3',
                    aws_secret_access_key=self.aws_secret_access_key,
                    aws_access_key_id=self.aws_access_key_id,
                    **region_name
            ) as s3_client:
                # Docs: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/head_object.html#
                return (await s3_client.head_object(
                    Bucket=self.bucket_name,
                    Key=path,
                    **query_parameters
                ))
        except s3_client.exceptions.ClientError as e:
            if e.response.get('Error', {}).get('Code') == '404':
                raise exceptions.NotFoundError(str(path))

    async def get_s3_bucket_object(self, path, query_parameters=None):
        try:
            session = get_session()
            region_name = {"region_name": self.region} if self.region else {}
            query_parameters = query_parameters or {}
            async with session.create_client(
                    's3',
                    aws_secret_access_key=self.aws_secret_access_key,
                    aws_access_key_id=self.aws_access_key_id,
                    **region_name
            ) as s3_client:
                # Docs: https://boto3.amazonaws.com/v1/documentation/api/1.28.0/reference/services/s3/client/get_object.html
                return (await s3_client.get_object(
                    Bucket=self.bucket_name,
                    Key=path,
                    **query_parameters
                ))
        except s3_client.exceptions.ClientError as e:
            if e.response.get('Error', {}).get('Code') == '404':
                raise exceptions.NotFoundError(str(path))

    async def get_s3_bucket_object_location(self):
        try:
            session = get_session()
            async with session.create_client(
                    's3',
                    aws_secret_access_key=self.aws_secret_access_key,
                    aws_access_key_id=self.aws_access_key_id,
            ) as s3_client:
                # Docs: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_bucket_location.html#
                return (await s3_client.get_bucket_location(
                    Bucket=self.bucket_name
                ))
        except s3_client.exceptions.ClientError as e:
            if e.response.get('Error', {}).get('Code') == '404':
                raise exceptions.NotFoundError(str(path))

    async def get_folder_metadata(self, path, params):
        try:
            contents, prefixes = [], []
            session = get_session()
            region_name = {"region_name": self.region} if self.region else {}
            async with session.create_client(
                    's3',
                    aws_secret_access_key=self.aws_secret_access_key,
                    aws_access_key_id=self.aws_access_key_id,
                    **region_name
            ) as s3_client:
                # Docs: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_paginator.html
                # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_objects_v2.html#list-objects-v2
                paginator = s3_client.get_paginator('list_objects_v2')
                pages = paginator.paginate(
                    Bucket=self.bucket_name,
                    **params
                )

                async for page in pages:
                    contents.extend(page.get('Contents', []))
                    prefixes.extend(page.get('CommonPrefixes', []))

            return contents, prefixes
        except s3_client.exceptions.ClientError as e:
            if e.response.get('Error', {}).get('Code') == '404':
                raise exceptions.NotFoundError(str(path))

    async def create_s3_bucket_object(self, path, query_parameters=None):
        try:
            session = get_session()
            region_name = {"region_name": self.region} if self.region else {}
            query_parameters = query_parameters or {}
            async with session.create_client(
                    's3',
                    aws_secret_access_key=self.aws_secret_access_key,
                    aws_access_key_id=self.aws_access_key_id,
                    **region_name
            ) as s3_client:
                # Docs: https://boto3.amazonaws.com/v1/documentation/api/1.28.0/reference/services/s3/client/put_object.html
                return (await s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=path,
                    **query_parameters
                ))
        except Exception as e:
            raise exceptions.UploadFailedError(str(path))


    async def delete_s3_bucket_object(self, path):
        try:
            session = get_session()
            region_name = {"region_name": self.region} if self.region else {}
            async with session.create_client(
                    's3',
                    aws_secret_access_key=self.aws_secret_access_key,
                    aws_access_key_id=self.aws_access_key_id,
                    **region_name
            ) as s3_client:
                # Docs: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/delete_object.html
                await s3_client.delete_object(
                    Bucket=self.bucket_name,
                    Key=path
                )
        except Exception as e:
            raise exceptions.DeleteError(str(path))

    async def delete_s3_bucket_folder_objects(self, path):
        try:
            session = get_session()
            region_name = {"region_name": self.region} if self.region else {}
            async with session.create_client(
                    's3',
                    aws_secret_access_key=self.aws_secret_access_key,
                    aws_access_key_id=self.aws_access_key_id,
                    **region_name
            ) as s3_client:
                params = {'Prefix': path}
                # Docs: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_paginator.html
                # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_objects_v2.html#list-objects-v2
                paginator = s3_client.get_paginator('list_objects_v2')

                pages = paginator.paginate(
                    Bucket=self.bucket_name,
                    **params
                )

                contents = []
                prefixes = []

                async for page in pages:
                    contents = page.get('Contents', [])
                    delete_requests = [{"Key": obj["Key"]} for obj in contents]

                    for index in range(0, len(delete_requests), 1000):
                        chunk = delete_requests[index:index + 1000]
                        await s3_client.delete_objects(
                            Bucket=self.bucket_name,
                            Delete={"Objects": chunk}
                        )
        except Exception as e:
            raise exceptions.DeleteError(str(path))

    async def get_object_versions(self, query_parameters):
        try:
            session = get_session()
            region_name = {"region_name": self.region} if self.region else {}
            async with session.create_client(
                    's3',
                    aws_secret_access_key=self.aws_secret_access_key,
                    aws_access_key_id=self.aws_access_key_id,
                    **region_name
            ) as s3_client:
                paginator = s3_client.get_paginator('list_object_versions')
                pages = paginator.paginate(
                    Bucket=self.bucket_name,
                    **query_parameters
                )
                all_versions = []
                async for page in pages:
                    all_versions.extend(page.get('Versions', []))
                return all_versions
        except Exception as e:
            raise Exception(f"Failed to fetch versions: {e}")

    async def create_multipart_upload(self,query_parameters):
        try:
            session = get_session()
            region_name = {"region_name": self.region} if self.region else {}
            async with session.create_client(
                    's3',
                    aws_secret_access_key=self.aws_secret_access_key,
                    aws_access_key_id=self.aws_access_key_id,
                    **region_name
            ) as s3_client:
                # https://boto3.amazonaws.com/v1/documentation/api/1.28.0/reference/services/s3/client/create_multipart_upload.html
                return await s3_client.create_multipart_upload(
                    Bucket=self.bucket_name,
                    **query_parameters
                )
        except Exception as e:
            raise Exception(f"Failed to fetch versions: {e}")

    async def validate_v1_path(self, path, **kwargs):
        await self._check_region()

        path = f"/{self.base_folder + path.lstrip('/')}"

        implicit_folder = path.endswith('/')

        if implicit_folder:
            params = {'Prefix': path, 'Delimiter': '/'}
            await self.get_folder_metadata(path,params)
        else:
            await self.check_key_existence(path[1:])

        return WaterButlerPath(path)

    async def validate_path(self, path, **kwargs):
        # The user selected base folder, the root of the where that user's node is connected.
        return WaterButlerPath(f"/{self.base_folder + path.lstrip('/')}")

    def can_duplicate_names(self):
        return True

    def can_intra_copy(self, dest_provider, path=None):
        return isinstance(self, type(dest_provider)) and not getattr(path, 'is_dir', False)

    def can_intra_move(self, dest_provider, path=None):
        return isinstance(self, type(dest_provider)) and not getattr(path, 'is_dir', False)

    async def intra_copy(self, dest_provider, source_path, dest_path):
        """Copy key from one S3 bucket to another. The credentials specified in
        `dest_provider` must have read access to `source.bucket`.
        """
        await self._check_region()
        exists = await dest_provider.exists(dest_path)

        # ensure no left slash when joining paths

        # TODO: need to find UI option for testing it out
        region_name = {"region_name": self.region} if self.region else {}
        async with session.create_client(
                's3',
                aws_secret_access_key=self.aws_secret_access_key,
                aws_access_key_id=self.aws_access_key_id,
                **region_name
        ) as s3_client:
            copy_source = {
                'Bucket': self.bucket_name,
                'Key': source_path,
            }
            try:
                await s3_client.copy_object(
                    Bucket=dest_provider.bucket_name,
                    Key=dest_path,
                    CopySource=copy_source,
                )
            except Exception as e:
                raise exceptions.IntraCopyError(f"IntraCopyError {e}")

        return (await dest_provider.metadata(dest_path)), not exists


    async def download(self, path, accept_url=False, revision=None, range=None, **kwargs):
        r"""Returns a ResponseWrapper (Stream) for the specified path
        raises FileNotFoundError if the status from S3 is not 200

        :param str path: Path to the key you want to download
        :param dict \*\*kwargs: Additional arguments that are ignored
        :rtype: :class:`waterbutler.core.streams.ResponseStreamReader`
        :raises: :class:`waterbutler.core.exceptions.DownloadError`
        """

        await self._check_region()

        if not path.is_file:
            raise exceptions.DownloadError('No file specified for download', code=400)

        display_name = kwargs.get('display_name') or path.name
        query_parameters = {
            'ResponseContentDisposition': make_disposition(display_name)
        }

        if revision and revision.lower() != 'latest':
            query_parameters = {'VersionId': revision}


        resp = await self.get_s3_bucket_object(path.path, query_parameters)
        return streams.ResponseStreamReader(resp['Body'])

    async def upload(self, stream, path, conflict='replace', **kwargs):
        """Uploads the given stream to S3

        :param waterbutler.core.streams.RequestWrapper stream: The stream to put to S3
        :param str path: The full path of the key to upload to/into
        :rtype: dict, bool
        """

        await self._check_region()

        path, exists = await self.handle_name_conflict(path, conflict=conflict)

        if stream.size < self.CONTIGUOUS_UPLOAD_SIZE_LIMIT:
            await self._contiguous_upload(stream, path)
        else:
            await self._chunked_upload(stream, path)

        return (await self.metadata(path, **kwargs)), not exists

    async def _contiguous_upload(self, stream, path):
        """Uploads the given stream in one request.
        """

        stream.add_writer('md5', streams.HashStreamWriter(hashlib.md5))
        data = await stream.read()
        md5_digest = stream.writers['md5'].hexdigest
        content_md5 = base64.b64encode(bytes.fromhex(md5_digest)).decode('utf-8')

        query_parameters = {
            'ContentLength': len(data),
            'Body': data,
            'ContentMD5': content_md5,
        }
        # this is usually set in boto.s3.key.generate_url, but do it here
        # do be explicit about our header payloads for signing purposes
        if self.encrypt_uploads:
            query_parameters['ServerSideEncryption'] = 'AES256'

        logger.error(f"query_parameters {query_parameters}")

        resp = await self.create_s3_bucket_object(path.path, query_parameters=query_parameters)

        logger.error(f"_contiguous_upload {resp}")

        # md5 is returned as ETag header as long as server side encryption is not used.
        if md5_digest != resp.get('ETag', '').replace('"', ''):
            raise exceptions.UploadChecksumMismatchError()

    async def _chunked_upload(self, stream, path):
        """Uploads the given stream to S3 over multiple chunks
        """

        # Step 1. Create a multi-part upload session
        session_upload_id = await self._create_upload_session(path)

        try:
            # Step 2. Break stream into chunks and upload them one by one
            parts_metadata = await self._upload_parts(stream, path, session_upload_id)
            # Step 3. Commit the parts and end the upload session
            await self._complete_multipart_upload(path, session_upload_id, parts_metadata)
        except Exception as err:
            msg = 'An unexpected error has occurred during the multi-part upload.'
            logger.error(f'{msg} upload_id={session_upload_id} error={err!r}')
            aborted = await self._abort_chunked_upload(path, session_upload_id)
            if not aborted:
                msg += '  The abort action failed to clean up the temporary file parts generated ' \
                       'during the upload process.  Please manually remove them.'
            else:
                msg += ' The upload is aborted.'
            raise exceptions.UploadError(msg)

    async def _create_upload_session(self, path):
        """This operation initiates a multipart upload and returns an upload ID. This upload ID is
        used to associate all of the parts in the specific multipart upload. You specify this upload
        ID in each of your subsequent upload part requests (see Upload Part). You also include this
        upload ID in the final request to either complete or abort the multipart upload request.

        Docs: https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadInitiate.html
        """
        # import pydevd_pycharm
        # pydevd_pycharm.settrace('host.docker.internal', port=1236, stdoutToServer=True, stderrToServer=True)
        query_parameters = {'Key': path.path}
        # "Initiate Multipart Upload" supports AWS server-side encryption
        if self.encrypt_uploads:
            query_parameters['ServerSideEncryption'] = 'AES256'

        resp = await self.create_multipart_upload(query_parameters)

        return resp['UploadId']

    async def _upload_parts(self, stream, path, session_upload_id):
        """Uploads all parts/chunks of the given stream to S3 one by one.
        """

        metadata = []
        parts = [self.CHUNK_SIZE for i in range(0, stream.size // self.CHUNK_SIZE)]
        if stream.size % self.CHUNK_SIZE:
            parts.append(stream.size - (len(parts) * self.CHUNK_SIZE))
        logger.debug(f'Multipart upload segment sizes: {parts}')
        for chunk_number, chunk_size in enumerate(parts):
            logger.debug(f'  uploading part {chunk_number + 1} with size {chunk_size}')
            metadata.append(await self._upload_part(stream, path, session_upload_id,
                                                    chunk_number + 1, chunk_size))
        return metadata

    async def _upload_part(self, stream, path, session_upload_id, chunk_number, chunk_size):
        """Uploads a single part/chunk of the given stream to S3.

        :param int chunk_number: sequence number of chunk. 1-indexed.
        """
        cutoff_stream = streams.CutoffStream(stream, cutoff=chunk_size)
        body = await cutoff_stream.read()

        params = {
            'ContentLength': chunk_size,
            'PartNumber': chunk_number,
            'UploadId': session_upload_id,
            "Key": path.path,
            'Body': body,
        }

        resp = None
        try:
            session = get_session()
            region_name = {"region_name": self.region} if self.region else {}
            async with session.create_client(
                    's3',
                    aws_secret_access_key=self.aws_secret_access_key,
                    aws_access_key_id=self.aws_access_key_id,
                    **region_name
            ) as s3_client:
                # Docs: https://boto3.amazonaws.com/v1/documentation/api/1.28.0/reference/services/s3/client/upload_part.html
                resp = await s3_client.upload_part(
                    Bucket=self.bucket_name,
                    **params
                )
        except Exception as e:
            raise Exception(f"Failed to fetch versions: {e}")

        logger.error(f'resp {resp}')
        return resp

    async def _abort_chunked_upload(self, path, session_upload_id):
        """Abort a multipart upload and verify it with retries."""
        session = get_session()
        region_name = {"region_name": self.region} if self.region else {}

        async with session.create_client(
                's3',
                aws_secret_access_key=self.aws_secret_access_key,
                aws_access_key_id=self.aws_access_key_id,
                **region_name
        ) as s3_client:
            is_aborted = False
            retries = 0

            while retries <= settings.CHUNKED_UPLOAD_MAX_ABORT_RETRIES:
                try:
                    logger.error(f"bucket_name {self.bucket_name} path.path {path.path} s3_client {s3_client}")
                    # Docs: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/abort_multipart_upload.html
                    await s3_client.abort_multipart_upload(
                        Bucket=self.bucket_name,
                        Key=path.path,
                        UploadId=session_upload_id,
                    )
                    logger.error('end')
                except s3_client.exceptions.NoSuchUpload:
                    is_aborted = True
                    break
                except Exception as e:
                    logger.error(f"Abort attempt {retries} failed: {e}")

                parts, is_aborted = await self._list_uploaded_chunks(s3_client, path, session_upload_id, retries)

                if is_aborted:
                    break

                retries += 1

            if is_aborted:
                logger.error(f"Multipart upload successfully aborted after {retries} retries: {session_upload_id}")
                return True

            logger.error(f"Failed to abort multipart upload after {retries} retries: {session_upload_id}")
            return False

    async def _list_uploaded_chunks(self, s3_client, path, session_upload_id, retries):
        """Lists the parts that have been uploaded for a specific multipart upload using boto3 S3 client.

        Docs: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.list_parts
        """
        session = get_session()

        try:
            response = await s3_client.list_parts(
                Bucket=self.bucket_name,
                Key=path.path,
                UploadId=session_upload_id
            )
            return  response.get("Parts", []), False
        except s3_client.exceptions.NoSuchUpload:
            return [], True
        except Exception as e:
            logger.error(f"List parts attempt {retries} failed: {e}")

    async def _complete_multipart_upload(self, path, session_upload_id, parts_metadata):
        """This operation completes a multipart upload by assembling previously uploaded parts.

        Docs: https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadComplete.html
        """
        session = get_session()
        region_name = {"region_name": self.region} if self.region else {}

        # boto3 requires part numbers to be ints and ETags to be strings
        parts = [
            {'PartNumber': i + 1, 'ETag': part['ETag']}
            for i, part in enumerate(parts_metadata)
        ]

        try:
            async with session.create_client(
                's3',
                aws_secret_access_key=self.aws_secret_access_key,
                aws_access_key_id=self.aws_access_key_id,
                **region_name
            ) as s3_client:
                await s3_client.complete_multipart_upload(
                    Bucket=self.bucket_name,
                    Key=path.path,
                    UploadId=session_upload_id,
                    MultipartUpload={'Parts': parts}
                )
        except Exception as e:
            raise exceptions.UploadError(f"Failed to complete multipart upload: {e}")

    async def delete(self, path, confirm_delete=0, **kwargs):
        """Deletes the key at the specified path

        :param str path: The path of the key to delete
        :param int confirm_delete: Must be 1 to confirm root folder delete
        """
        await self._check_region()

        if path.is_root:
            if not confirm_delete == 1:
                raise exceptions.DeleteError(
                    'confirm_delete=1 is required for deleting root provider folder',
                    code=400
                )

        if path.is_file:
            await self.delete_s3_bucket_object(path.path)
        else:
            await self._delete_folder(path, **kwargs)

    async def _delete_folder(self, path, **kwargs):
        """Query for recursive contents of folder and delete in batches of 1000

        Called from: func: delete if not path.is_file

        Calls: func: self._check_region

        :param *ProviderPath path: Path to be deleted

        On S3, folders are not first-class objects, but are instead inferred
        from the names of their children.  A regular DELETE request issued
        against a folder will not work unless that folder is completely empty.
        To fully delete an occupied folder, we must delete all of the comprising
        objects.  Amazon provides a bulk delete operation to simplify this.
        # docs https://boto3.amazonaws.com/v1/documentation/api/1.28.0/reference/services/s3/client/delete_objects.html#delete-objects
        """
        await self._check_region()
        await self.delete_s3_bucket_folder_objects(path.path)

    async def revisions(self, path, **kwargs):
        """Get past versions of the requested key

        :param str path: The path to a key
        :rtype list:
        Docs: https://boto3.amazonaws.com/v1/documentation/api/1.28.0/reference/services/s3/client/list_object_versions.html
        """
        await self._check_region()

        query_params = {'Prefix': path.path, 'Delimiter': '/'}

        versions = await self.get_object_versions(query_params)

        return [
            S3Revision(item)
            for item in versions
            if item['Key'] == path.path
        ]

    async def metadata(self, path, revision=None, **kwargs):
        """Get Metadata about the requested file or folder

        :param WaterButlerPath path: The path to a key or folder
        :rtype: dict or list
        """
        await self._check_region()

        if path.is_dir:
            metadata = await self._metadata_folder(path)
            for item in metadata:
                item.raw['base_folder'] = self.base_folder
        else:
            metadata = await self._metadata_file(path, revision=revision)
            metadata.raw['base_folder'] = self.base_folder

        return metadata

    async def create_folder(self, path, folder_precheck=True, **kwargs):
        """
        :param str path: The path to create a folder at
        """
        await self._check_region()

        WaterButlerPath.validate_folder(path)

        if folder_precheck:
            if (await self.exists(path)):
                raise exceptions.FolderNamingConflict(path.name)
        path_prefix = path.path
        logger.error(f"path {path_prefix} folder_precheck {folder_precheck}")

        await self.create_s3_bucket_object(path_prefix)

        metadata = S3FolderMetadata({'Prefix': path_prefix})
        metadata.raw['base_folder'] = self.base_folder
        return metadata

    async def _metadata_file(self, path, revision=None):
        await self._check_region()

        if revision == 'Latest':
            revision = None
        path_prefix = path.path
        resp = await self.check_key_existence(path_prefix, query_parameters={'VersionId': revision} if revision else {})
        return S3FileMetadataHeaders(path_prefix, resp.get('ResponseMetadata', {}).get('HTTPHeaders'))

    async def _metadata_folder(self, path):
        await self._check_region()

        path_prefix = path.path
        params = {'Prefix': path_prefix, 'Delimiter': '/'}

        contents, prefixes = await self.get_folder_metadata(path_prefix, params)
        logger.error(f"contents {contents} prefixes {prefixes}")
        if not contents and not prefixes and not path.is_root:
            # If contents and prefixes are empty then this "folder"
            # must exist as a key with a / at the end of the name
            # if the path is root there is no need to test if it exists
            await self.check_key_existence(path_prefix)

        if isinstance(contents, dict):
            contents = [contents]

        if isinstance(prefixes, dict):
            prefixes = [prefixes]

        items = [
            S3FolderMetadata(item)
            for item in prefixes if item['Prefix'] != path_prefix
        ]

        for content in contents:
            if content['Key'] == params['Prefix']:
                continue

            if content['Key'].endswith('/'):
                items.append(S3FolderKeyMetadata(content))
            else:
                items.append(S3FileMetadata(content))

        return items

    async def _check_region(self):
        """
        Lookup the region via bucket name, then update the host to match.
        """

        if self.region is None:
            self.region = await self._get_bucket_region()
            if self.region == 'EU':
                self.region = 'eu-west-1'

        self.metrics.add('region', self.region)

    async def _get_bucket_region(self):
        """Bucket names are unique across all regions.

        Endpoint doc:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_bucket_location.html
        """
        resp = await self.get_s3_bucket_object_location()
        return resp.get('LocationConstraint')
