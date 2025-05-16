import hashlib
import logging

import xmltodict
import xml.sax.saxutils
from aiobotocore.config import AioConfig
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

    async def generate_generic_presigned_url(self, path, method='head_object', query_parameters=None, default_params=True):
        try:
            session = get_session()
            region_name = {'region_name': self.region} if self.region else {}
            config = AioConfig(signature_version='s3v4')

            async with session.create_client(
                    's3',
                    aws_secret_access_key=self.aws_secret_access_key,
                    aws_access_key_id=self.aws_access_key_id,
                    config=config,
                    **region_name
            ) as s3_client:
                params = {'Bucket': self.bucket_name, 'Key': path} if default_params else {}
                if query_parameters:
                    params.update(query_parameters)

                return await s3_client.generate_presigned_url(method,  Params=params,  ExpiresIn=settings.TEMP_URL_SECS)
        except Exception as e:
            raise exceptions.NotFoundError(f"{path} {e}")

    async def check_key_existence(self, path, expects=(200, ), query_parameters=None):
        try:
            session = get_session()
            region_name = {"region_name": self.region} if self.region else {}
            query_parameters = query_parameters or {}

            async with (session.create_client(
                    's3',
                    aws_secret_access_key=self.aws_secret_access_key,
                    aws_access_key_id=self.aws_access_key_id,
                    **region_name
            ) as s3_client):
                params = {'Bucket': self.bucket_name, 'Key': path}
                if query_parameters:
                    params.update(query_parameters)

                url = await s3_client.generate_presigned_url('head_object',  Params=params,  ExpiresIn=settings.TEMP_URL_SECS)

                return await self.make_request(
                    'HEAD',
                    url,
                    is_async=True,
                    expects=expects,
                    throws=exceptions.MetadataError,
                )
        except Exception as e:
            raise exceptions.NotFoundError(f"{path} {e}")

    async def get_s3_bucket_object_location(self):
        session = get_session()
        config = AioConfig(signature_version='s3v4')
        async with session.create_client(
                's3',
                aws_secret_access_key=self.aws_secret_access_key,
                aws_access_key_id=self.aws_access_key_id,
                config=config
        ) as s3_client:
            # Docs: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_bucket_location.html#
            url = await s3_client.generate_presigned_url('get_bucket_location',  Params={'Bucket': self.bucket_name},  ExpiresIn=settings.TEMP_URL_SECS)
            return await self.make_request(
                'GET',
                url,
                is_async=True,
                expects=(200, ),
                throws=exceptions.MetadataError,
            )

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
        except Exception as e:
            raise exceptions.NotFoundError(f"{path} {e}")

    async def delete_s3_bucket_folder_objects(self, path):
        continuation_token = None
        delete_requests = []
        while True:
            list_params = {
                'Bucket': self.bucket_name,
                'Prefix': path,
            }
            if continuation_token:
                list_params['ContinuationToken'] = continuation_token

            # Docs: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_objects_v2.html
            list_url = await self.generate_generic_presigned_url(
                '', 'list_objects_v2',query_parameters=list_params, default_params=False
            )

            resp = await self.make_request(
                'GET', list_url,
                expects=(200, 206),
                throws=exceptions.DownloadError
            )
            xml_body = await resp.text()
            doc = xmltodict.parse(xml_body)
            result = doc.get('ListBucketResult', {})

            contents = result.get('Contents') or []

            if isinstance(contents, dict):
                contents = [contents]
            for o in contents:
                delete_requests.append({"Key":o['Key']})

            # handle pagination
            if result.get('IsTruncated') == 'true':
                continuation_token = result.get('NextContinuationToken')
            else:
                break

        session = get_session()
        region_name = {"region_name": self.region} if self.region else {}
        async with session.create_client(
                's3',
                aws_secret_access_key=self.aws_secret_access_key,
                aws_access_key_id=self.aws_access_key_id,
                **region_name
        ) as s3_client:
            for index in range(0, len(delete_requests), 1000):
                chunk = delete_requests[index:index + 1000]
                try:
                    await s3_client.delete_objects(
                        Bucket=self.bucket_name,
                        Delete={"Objects": chunk}
                    )
                except Exception as e:
                    raise exceptions.DeleteError(f"{path} {e}")

        # TODO: maybe there is a workaround for 'delete_objects' usage got the following for code below
        # json.decoder.JSONDecodeError: Expecting value: line 1 column 1  on resp = await self.make_request call

        # for index in range(0, len(delete_requests), 1000):
        #     chunk = delete_requests[index:index + 1000]
        #
        #     async with session.create_client(
        #             's3',
        #             aws_access_key_id=self.aws_access_key_id,
        #             aws_secret_access_key=self.aws_secret_access_key,
        #             config=config,
        #             **region_kwargs
        #     ) as s3:
        #         list_url = await s3.generate_presigned_url(
        #             ClientMethod='delete_objects',
        #             Params={'Bucket': self.bucket_name, 'Delete':{"Objects": chunk}},
        #             ExpiresIn=settings.TEMP_URL_SECS
        #         )
        #
        #         def _make_delete_xml(chunk):
        #             items = "".join(f"<Object><Key>{o['Key']}</Key></Object>" for o in chunk)
        #             return f"<?xml version='1.0' encoding='UTF-8'?><Delete>{items}</Delete>"
        #
        #         xml_body = _make_delete_xml(chunk)
        #
        #         resp = await self.make_request(
        #             'POST',
        #             list_url,
        #             data=xml_body,
        #             headers={'Content-Type': 'application/xml'},
        #             expects=(200, 204,),
        #             throws=exceptions.DeleteError,
        #         )
        #         await resp.release()


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
            raise exceptions.NotFoundError(f"Failed to fetch versions: {e}")

    async def validate_v1_path(self, path, **kwargs):
        await self._check_region()

        path = f"/{self.base_folder + path.lstrip('/')}"

        implicit_folder = path.endswith('/')

        if implicit_folder:

            query_parameters = {'Bucket': self.bucket_name, 'Prefix': path, 'Delimiter': '/', 'MaxKeys': 1}

            # Docs: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_objects_v2.html
            url = await self.generate_generic_presigned_url(path, method='list_objects_v2',
                                                            query_parameters=query_parameters, default_params=False)
            await self.make_request(
                'GET',
                url,
                expects=(200, 206,),
                throws=exceptions.NotFoundError,
            )
        else:
            await self.check_key_existence(path[1:], expects=(200, ))

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
        region_name = {"region_name": self.region} if self.region else {}


        #
        # # ensure no left slash when joining paths
        #

        # TODO:         # # TODO: 403, {"response": "<?xml version=\"1.0\" encoding=\"UTF-8\"?>
        #  \n<Error><Code>SignatureDoesNotMatch</Code><Message>The request signature we calculated does
        # query_parameters = {'CopySource': f"{self.bucket_name}/{source_path.path}"
        #
        #     # {
        #     #         'Bucket': self.bucket_name,
        #     #         'Key': source_path.path,
        #     # }
        # }
        #
        # url = await self.generate_generic_presigned_url(dest_path.path, 'copy_object', query_parameters=query_parameters)
        #
        # resp = await self.make_request(
        #     'PUT',
        #     url,
        #     headers={
        #         # this must match exactly what you passed into generate_presigned_url
        #         'x-amz-copy-source': f"/{self.bucket_name}/{source_path.path}"
        #     },
        #     skip_auto_headers={'CONTENT-TYPE'},
        #     expects=(200, ),
        #     throws=exceptions.DownloadError,
        # )
        # await resp.release()

        session = get_session()
        async with session.create_client(
                's3',
                aws_secret_access_key=self.aws_secret_access_key,
                aws_access_key_id=self.aws_access_key_id,
                **region_name
        ) as s3_client:
            copy_source = {
                'Bucket': self.bucket_name,
                'Key': source_path.path,
            }
            try:
                await s3_client.copy_object(
                    Bucket=dest_provider.bucket_name,
                    Key=dest_path.path,
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

        query_parameters = {}

        # Todo: don't see where it may be set from front end side
        if not revision or revision.lower() == 'latest':
            query_parameters = {}
        else:
            query_parameters['VersionId'] = revision

        display_name = kwargs.get('display_name') or path.name
        query_parameters['ResponseContentDisposition'] = make_disposition(display_name)


        url = await self.generate_generic_presigned_url(path.path, 'get_object', query_parameters=query_parameters)

        resp = await self.make_request(
            'GET',
            url,
            range=range,
            expects=(200, 206,),
            throws=exceptions.DownloadError,
        )

        return streams.ResponseStreamReader(resp)

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
            logger.error('upload')
            await self._chunked_upload(stream, path)

        return (await self.metadata(path, **kwargs)), not exists

    async def _contiguous_upload(self, stream, path):
        """Uploads the given stream in one request.
        """

        stream.add_writer('md5', streams.HashStreamWriter(hashlib.md5))

        headers = {'Content-Length': str(stream.size)}
        query_parameters = {}
        # this is usually set in boto.s3.key.generate_url, but do it here
        # do be explicit about our header payloads for signing purposes
        if self.encrypt_uploads:
            headers['x-amz-server-side-encryption'] = 'AES256'
            query_parameters['ServerSideEncryption'] = 'AES256'

        # Docs: https://boto3.amazonaws.com/v1/documentation/api/1.28.0/reference/services/s3/client/put_object.html
        upload_url = await self.generate_generic_presigned_url(path.path, method='put_object', query_parameters=query_parameters)

        resp = await self.make_request(
            'PUT',
            upload_url,
            data=stream,
            skip_auto_headers={'CONTENT-TYPE'},
            headers=headers,
            expects=(200, 201,),
            throws=exceptions.UploadError,
        )
        await resp.release()

        # md5 is returned as ETag header as long as server side encryption is not used.
        if stream.writers['md5'].hexdigest != resp.headers['ETag'].replace('"', ''):
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

        headers = {}
        kwargs = {}
        # "Initiate Multipart Upload" supports AWS server-side encryption
        if self.encrypt_uploads:
            headers = {'x-amz-server-side-encryption': 'AES256'}
            kwargs["ServerSideEncryption"] = "AES256"

        # Docs: # https://boto3.amazonaws.com/v1/documentation/api/1.28.0/reference/services/s3/client/create_multipart_upload.html
        upload_session_url = await self.generate_generic_presigned_url(path.path, method='create_multipart_upload', query_parameters=kwargs)
        resp = await self.make_request(
            'POST',
            upload_session_url,
            headers=headers,
            skip_auto_headers={'CONTENT-TYPE'},
            throws=exceptions.UploadError,
        )
        upload_session_metadata = await resp.read()
        session_data = xmltodict.parse(upload_session_metadata, strip_whitespace=False)
        # Session upload id is the only info we need
        return session_data['InitiateMultipartUploadResult']['UploadId']


    async def _upload_parts(self, stream, path, session_upload_id):
        """Uploads all parts/chunks of the given stream to S3 one by one.
        """
        logger.error('_upload_parts')
        metadata = []
        parts = [self.CHUNK_SIZE for i in range(0, stream.size // self.CHUNK_SIZE)]
        if stream.size % self.CHUNK_SIZE:
            parts.append(stream.size - (len(parts) * self.CHUNK_SIZE))
        logger.info(f'Multipart upload segment sizes: {parts}')

        for chunk_number, chunk_size in enumerate(parts):
            metadata.append(await self._upload_part(stream, path, session_upload_id,
                                                    chunk_number + 1, chunk_size))

        return metadata

    async def _upload_part(self, stream, path, session_upload_id, chunk_number, chunk_size):
        """Uploads a single part/chunk of the given stream to S3.

        :param int chunk_number: sequence number of chunk. 1-indexed.
        """

        cutoff_stream = streams.CutoffStream(stream, cutoff=chunk_size)

        # Docs: https://boto3.amazonaws.com/v1/documentation/api/1.28.0/reference/services/s3/client/upload_part.html
        upload_part_url = await self.generate_generic_presigned_url(
            path.path, method='upload_part',
            query_parameters={'ContentLength': chunk_size, 'PartNumber': chunk_number, 'UploadId': session_upload_id}
        )


        resp = await self.make_request(
            'PUT',
            upload_part_url,
            data=cutoff_stream,
            skip_auto_headers={'CONTENT-TYPE'},
            headers={'Content-Length': str(chunk_size)},
            params={'partNumber': str(chunk_number), 'uploadId': session_upload_id},
            expects=(200, 201,),
            throws=exceptions.UploadError,
        )

        await resp.release()
        return resp.headers

    async def _abort_chunked_upload(self, path, session_upload_id):
        """This operation aborts a multipart upload. After a multipart upload is aborted, no
        additional parts can be uploaded using that upload ID. The storage consumed by any
        previously uploaded parts will be freed. However, if any part uploads are currently in
        progress, those part uploads might or might not succeed. As a result, it might be necessary
        to abort a given multipart upload multiple times in order to completely free all storage
        consumed by all parts. To verify that all parts have been removed, so you don't get charged
        for the part storage, you should call the List Parts operation and ensure the parts list is
        empty.

        Docs: https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadAbort.html

        Quirks:

        If the ABORT request is successful, the session may be deleted when the LIST PARTS request
        is made.  The criteria for successful abort thus is ether LIST PARTS request returns 404 or
        returns 200 with an empty parts list.
        """

        headers = {}
        params = {'UploadId': session_upload_id}


        abort_url = await self.generate_generic_presigned_url(path.path, method='abort_multipart_upload', query_parameters=params)

        iteration_count = 0
        is_aborted = False

        while iteration_count <= settings.CHUNKED_UPLOAD_MAX_ABORT_RETRIES:

            # ABORT
            resp = await self.make_request(
                'DELETE',
                abort_url,
                skip_auto_headers={'CONTENT-TYPE'},
                headers=headers,
                params=headers,
                expects=(204,),
                throws=exceptions.UploadError,
            )

            await resp.release()

            # LIST PARTS
            resp_xml, session_deleted = await self._list_uploaded_chunks(path, session_upload_id)

            if session_deleted:
                # Abort is successful if the session has been deleted
                is_aborted = True
                break

            uploaded_chunks_list = xmltodict.parse(resp_xml, strip_whitespace=False)
            parsed_parts_list = uploaded_chunks_list['ListPartsResult'].get('Part', [])
            if len(parsed_parts_list) == 0:
                # Abort is successful when there is no part left
                is_aborted = True
                break

            iteration_count += 1

        if is_aborted:
            logger.debug('Multi-part upload has been successfully aborted: retries={} '
                         'upload_id={}'.format(iteration_count, session_upload_id))
            return True

        logger.error('Multi-part upload has failed to abort: retries={} '
                     'upload_id={}'.format(iteration_count, session_upload_id))
        return False

    async def _list_uploaded_chunks(self, path, session_upload_id):
        """This operation lists the parts that have been uploaded for a specific multipart upload.

        Docs: https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadListParts.html
        """

        headers = {}
        params = {'UploadId': session_upload_id}
        list_url = await self.generate_generic_presigned_url(path.path, method='list_parts', query_parameters=params)

        resp = await self.make_request(
            'GET',
            list_url,
            skip_auto_headers={'CONTENT-TYPE'},
            headers=headers,
            params=headers,
            expects=(200, 201, 404,),
            throws=exceptions.UploadError
        )
        session_deleted = resp.status == 404
        resp_xml = await resp.read()

        return resp_xml, session_deleted

    async def _complete_multipart_upload(self, path, session_upload_id, parts_metadata):

        """This operation completes a multipart upload by assembling previously uploaded parts.

        Docs: https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadComplete.html
        """

        payload = ''.join([
            '<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUpload>',
            ''.join(
                ['<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>'.format(
                    i + 1,
                    xml.sax.saxutils.escape(part['ETAG'])
                ) for i, part in enumerate(parts_metadata)]
            ),
            '</CompleteMultipartUpload>',
        ]).encode('utf-8')

        complete_url = await self.generate_generic_presigned_url(
            path.path, method='complete_multipart_upload', query_parameters={'UploadId': session_upload_id}
        )


        resp = await self.make_request(
            'POST',
            complete_url,
            data=payload,
            headers={
                'Content-Type': 'application/xml',
                'Content-Length': str(len(payload)),
            },
            expects=(200, 201,),
            throws=exceptions.UploadError,
        )
        await resp.release()

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
            # Docs: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/delete_object.html
            delete_url = await self.generate_generic_presigned_url(path.path, method='delete_object')

            resp = await self.make_request(
                'DELETE',
                delete_url,
                expects=(200, 204,),
                throws=exceptions.DeleteError,
            )

            await resp.release()
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

        # Docs: https://boto3.amazonaws.com/v1/documentation/api/1.28.0/reference/services/s3/client/put_object.html
        folder_url = await self.generate_generic_presigned_url(path_prefix, method='put_object')

        await self.make_request(
            'PUT',
            folder_url,
            skip_auto_headers={'CONTENT-TYPE'},
            expects=(200, 201,),
            throws=exceptions.CreateFolderError
        )

        metadata = S3FolderMetadata({'Prefix': path_prefix})
        metadata.raw['base_folder'] = self.base_folder
        return metadata

    async def _metadata_file(self, path, revision=None):
        await self._check_region()

        if revision == 'Latest':
            revision = None
        path_prefix = path.path

        resp = await self.check_key_existence(path_prefix, query_parameters={'VersionId': revision} if revision else {})
        await resp.release()
        return S3FileMetadataHeaders(path.path, resp.headers)

    async def _metadata_folder(self, path):
        await self._check_region()

        path_prefix = path.path
        params = {'Prefix': path_prefix, 'Delimiter': '/'}

        contents, prefixes = await self.get_folder_metadata(path_prefix, params)

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
        contents = await resp.read()
        parsed = xmltodict.parse(contents, strip_whitespace=False)
        return parsed['LocationConstraint'].get('#text', '')

