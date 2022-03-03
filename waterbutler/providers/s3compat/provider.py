import os
import hashlib
import functools
from urllib import parse
import re
import logging
import xml.sax.saxutils

import xmltodict

from boto.compat import BytesIO  # type: ignore
from boto.s3.connection import S3Connection, OrdinaryCallingFormat, NoHostProvided
from boto.connection import HTTPRequest
from boto.s3.bucket import Bucket
from boto.utils import compute_md5

from waterbutler.core import streams, provider, exceptions
from waterbutler.core.path import WaterButlerPath
from waterbutler.core.utils import make_disposition
from waterbutler.providers.s3compat import settings
from waterbutler.providers.s3compat.metadata import (S3CompatRevision,
                                                     S3CompatFileMetadata,
                                                     S3CompatFolderMetadata,
                                                     S3CompatFolderKeyMetadata,
                                                     S3CompatFileMetadataHeaders,
                                                     )

logger = logging.getLogger(__name__)


class S3CompatConnection(S3Connection):
    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None,
                 is_secure=True, port=None, proxy=None, proxy_port=None,
                 proxy_user=None, proxy_pass=None,
                 host=NoHostProvided, debug=0, https_connection_factory=None,
                 calling_format=None, path='/',
                 provider='aws', bucket_class=Bucket, security_token=None,
                 suppress_consec_slashes=True, anon=False,
                 validate_certs=None, profile_name=None):
        super(S3CompatConnection, self).__init__(aws_access_key_id,
                aws_secret_access_key,
                is_secure, port, proxy, proxy_port, proxy_user, proxy_pass,
                host=host,
                debug=debug, https_connection_factory=https_connection_factory,
                calling_format=calling_format,
                path=path, provider=provider, bucket_class=bucket_class,
                security_token=security_token, anon=anon,
                validate_certs=validate_certs, profile_name=profile_name)

    def add_auth(self, method, url, headers):
        urlo = parse.urlparse(url)
        self._auth_handler.add_auth(HTTPRequest(method, urlo.scheme,
                                                urlo.hostname, self.port,
                                                urlo.path, urlo.path, {},
                                                headers, ''))
        return url[:url.index('?')] if '?' in url else url

    def _required_auth_capability(self):
        return ['s3']


class S3CompatProvider(provider.BaseProvider):
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
    NAME = 's3compat'
    CHUNK_SIZE = settings.CHUNK_SIZE
    CONTIGUOUS_UPLOAD_SIZE_LIMIT = settings.CONTIGUOUS_UPLOAD_SIZE_LIMIT

    def __init__(self, auth, credentials, settings, **kwargs):
        """
        .. note::

            Neither `S3CompatConnection#__init__` nor `S3CompatConnection#get_bucket`
            sends a request.

        :param dict auth: Not used
        :param dict credentials: Dict containing `access_key` and `secret_key`
        :param dict settings: Dict containing `bucket`
        """
        super().__init__(auth, credentials, settings, **kwargs)

        host = credentials['host']
        port = 443
        m = re.match(r'^(.+)\:([0-9]+)$', host)
        if m is not None:
            host = m.group(1)
            port = int(m.group(2))
        self.connection = S3CompatConnection(credentials['access_key'],
                                             credentials['secret_key'],
                                             calling_format=OrdinaryCallingFormat(),
                                             host=host,
                                             port=port,
                                             is_secure=port == 443)
        self.bucket = self.connection.get_bucket(settings['bucket'], validate=False)
        self.encrypt_uploads = self.settings.get('encrypt_uploads', False)
        self.prefix = settings.get('prefix', '')

    async def validate_v1_path(self, path, **kwargs):
        wbpath = WaterButlerPath(path, prepend=self.prefix)
        if path == '/':
            return wbpath

        implicit_folder = path.endswith('/')

        prefix = wbpath.full_path.lstrip('/')  # '/' -> '', '/A/B' -> 'A/B'
        if implicit_folder:
            params = {'prefix': prefix, 'delimiter': '/'}
            resp = await self.make_request(
                'GET',
                functools.partial(self.bucket.generate_url, settings.TEMP_URL_SECS, 'GET'),
                params=params,
                expects=(200, 404, ),
                throws=exceptions.MetadataError,
            )
        else:
            resp = await self.make_request(
                'HEAD',
                functools.partial(self.bucket.new_key(prefix).generate_url, settings.TEMP_URL_SECS, 'HEAD'),
                expects=(200, 404, ),
                throws=exceptions.MetadataError,
            )

        await resp.release()

        if resp.status == 404:
            raise exceptions.NotFoundError(str(prefix))

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

        dest_key = dest_provider.bucket.new_key(dest_path.full_path)

        # ensure no left slash when joining paths
        source_path = '/' + os.path.join(self.settings['bucket'], source_path.full_path)
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
        r"""Returns a ResponseWrapper (Stream) for the specified path
        raises FileNotFoundError if the status from S3 is not 200

        :param str path: Path to the key you want to download
        :param dict \*\*kwargs: Additional arguments that are ignored
        :rtype: :class:`waterbutler.core.streams.ResponseStreamReader`
        :raises: :class:`waterbutler.core.exceptions.DownloadError`
        """
        if not path.is_file:
            raise exceptions.DownloadError('No file specified for download', code=400)

        if not revision or revision.lower() == 'latest':
            query_parameters = None
        else:
            query_parameters = {'versionId': revision}

        display_name = kwargs.get('display_name') or path.name
        response_headers = {
            'response-content-disposition': make_disposition(display_name)
        }

        url = functools.partial(
            self.bucket.new_key(path.full_path).generate_url,
            settings.TEMP_URL_SECS,
            query_parameters=query_parameters,
            response_headers=response_headers
        )

        headers = {}
        raw_url = self.connection.add_auth('GET', url('GET'), headers)

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

        if stream.size < self.CONTIGUOUS_UPLOAD_SIZE_LIMIT:
            await self._contiguous_upload(stream, path)
        else:
            await self._chunked_upload(stream, path)

        return (await self.metadata(path, **kwargs)), not exists

    async def _contiguous_upload(self, stream, path):
        """Uploads the given stream in one request.
        """

        stream.add_writer('md5', streams.HashStreamWriter(hashlib.md5))

        headers = {'Content-Length': str(stream.size)}

        # this is usually set in boto.s3.key.generate_url, but do it here
        # do be explicit about our header payloads for signing purposes
        if self.encrypt_uploads:
            headers['x-amz-server-side-encryption'] = 'AES256'

        upload_url = functools.partial(
            self.bucket.new_key(path.full_path).generate_url,
            settings.TEMP_URL_SECS,
            'PUT',
            headers=headers,
        )
        resp = await self.make_request(
            'PUT',
            upload_url,
            data=stream,
            skip_auto_headers={'CONTENT-TYPE'},
            headers=headers,
            expects=(200, 201, ),
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
            logger.error('{} upload_id={} error={!r}'.format(msg, session_upload_id, err))
            aborted = await self._abort_chunked_upload(path, session_upload_id)
            if aborted:
                msg += '  The abort action failed to clean up the temporary file parts generated ' \
                       'during the upload process.  Please manually remove them.'
            raise exceptions.UploadError(msg)

    async def _create_upload_session(self, path):
        """This operation initiates a multipart upload and returns an upload ID. This upload ID is
        used to associate all of the parts in the specific multipart upload. You specify this upload
        ID in each of your subsequent upload part requests (see Upload Part). You also include this
        upload ID in the final request to either complete or abort the multipart upload request.

        Docs: https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadInitiate.html
        """

        headers = {}
        # "Initiate Multipart Upload" supports AWS server-side encryption
        if self.encrypt_uploads:
            headers = {'x-amz-server-side-encryption': 'AES256'}
        params = {'uploads': ''}
        upload_url = functools.partial(
            self.bucket.new_key(path.path).generate_url,
            settings.TEMP_URL_SECS,
            'POST',
            query_parameters=params,
            headers=headers,
        )
        resp = await self.make_request(
            'POST',
            upload_url,
            headers=headers,
            skip_auto_headers={'CONTENT-TYPE'},
            params=params,
            expects=(200, 201,),
            throws=exceptions.UploadError,
        )
        upload_session_metadata = await resp.read()
        session_data = xmltodict.parse(upload_session_metadata, strip_whitespace=False)
        # Session upload id is the only info we need
        return session_data['InitiateMultipartUploadResult']['UploadId']

    async def _upload_parts(self, stream, path, session_upload_id):
        """Uploads all parts/chunks of the given stream to S3 one by one.
        """

        metadata = []
        parts = [self.CHUNK_SIZE for i in range(0, stream.size // self.CHUNK_SIZE)]
        if stream.size % self.CHUNK_SIZE:
            parts.append(stream.size - (len(parts) * self.CHUNK_SIZE))
        logger.debug('Multipart upload segment sizes: {}'.format(parts))
        for chunk_number, chunk_size in enumerate(parts):
            logger.debug('  uploading part {} with size {}'.format(chunk_number + 1, chunk_size))
            metadata.append(await self._upload_part(stream, path, session_upload_id,
                                                    chunk_number + 1, chunk_size))
        return metadata

    async def _upload_part(self, stream, path, session_upload_id, chunk_number, chunk_size):
        """Uploads a single part/chunk of the given stream to S3.

        :param int chunk_number: sequence number of chunk. 1-indexed.
        """

        cutoff_stream = streams.CutoffStream(stream, cutoff=chunk_size)

        headers = {'Content-Length': str(chunk_size)}
        params = {
            'partNumber': str(chunk_number),
            'uploadId': session_upload_id,
        }
        upload_url = functools.partial(
            self.bucket.new_key(path.path).generate_url,
            settings.TEMP_URL_SECS,
            'PUT',
            query_parameters=params,
            headers=headers
        )
        resp = await self.make_request(
            'PUT',
            upload_url,
            data=cutoff_stream,
            skip_auto_headers={'CONTENT-TYPE'},
            headers=headers,
            params=params,
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
        params = {'uploadId': session_upload_id}
        abort_url = functools.partial(
            self.bucket.new_key(path.path).generate_url,
            settings.TEMP_URL_SECS,
            'DELETE',
            query_parameters=params,
            headers=headers,
        )

        iteration_count = 0
        is_aborted = False
        while iteration_count <= settings.CHUNKED_UPLOAD_MAX_ABORT_RETRIES:

            # ABORT
            resp = await self.make_request(
                'DELETE',
                abort_url,
                skip_auto_headers={'CONTENT-TYPE'},
                headers=headers,
                params=params,
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
        params = {'uploadId': session_upload_id}
        list_url = functools.partial(
            self.bucket.new_key(path.path).generate_url,
            settings.TEMP_URL_SECS,
            'GET',
            query_parameters=params,
            headers=headers
        )

        resp = await self.make_request(
            'GET',
            list_url,
            skip_auto_headers={'CONTENT-TYPE'},
            headers=headers,
            params=params,
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
        headers = {
            'Content-Length': str(len(payload)),
            'Content-MD5': compute_md5(BytesIO(payload))[1],
            'Content-Type': 'text/xml',
        }
        params = {'uploadId': session_upload_id}
        complete_url = functools.partial(
            self.bucket.new_key(path.path).generate_url,
            settings.TEMP_URL_SECS,
            'POST',
            query_parameters=params,
            headers=headers
        )

        resp = await self.make_request(
            'POST',
            complete_url,
            data=payload,
            headers=headers,
            params=params,
            expects=(200, 201,),
            throws=exceptions.UploadError,
        )
        await resp.release()

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
            resp = await self.make_request(
                'DELETE',
                self.bucket.new_key(path.full_path).generate_url(settings.TEMP_URL_SECS, 'DELETE'),
                expects=(200, 204, ),
                throws=exceptions.DeleteError,
            )
            await resp.release()
        else:
            await self._delete_folder(path, **kwargs)

    async def _folder_prefix_exists(self, folder_prefix):
        # Even if the storage is MinIO, Contents with a leaf folder is
        # returned when a last slash of a prefix is removed.
        query_params = {
            'prefix': folder_prefix.rstrip('/'),  # 'A/B/' -> 'A/B'
            'delimiter': '/'}
        resp = await self.make_request(
            'GET',
            self.bucket.generate_url(settings.TEMP_URL_SECS, 'GET'),
            params=query_params,
            expects=(200, ),
            throws=exceptions.MetadataError,
        )
        contents = await resp.read()
        parsed = xmltodict.parse(contents, strip_whitespace=False)['ListBucketResult']
        common_prefixes = parsed.get('CommonPrefixes', [])
        # common_prefixes is dict when returned prefix is one.
        if not isinstance(common_prefixes, list):
            common_prefixes = [common_prefixes]
        for common_prefix in common_prefixes:
            val = common_prefix.get('Prefix')
            if val == folder_prefix:  # with last slash
                return True
        return False

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
        more_to_come = True
        content_keys = []
        prefix = path.full_path.lstrip('/')  # '/' -> '', '/A/B/' -> 'A/B/'
        query_params = {'prefix': prefix}
        marker = None

        while more_to_come:
            if marker is not None:
                query_params['marker'] = marker

            resp = await self.make_request(
                'GET',
                self.bucket.generate_url(settings.TEMP_URL_SECS, 'GET'),
                params=query_params,
                expects=(200, ),
                throws=exceptions.MetadataError,
            )

            contents = await resp.read()
            parsed = xmltodict.parse(contents, strip_whitespace=False)['ListBucketResult']
            more_to_come = parsed.get('IsTruncated') == 'true'
            contents = parsed.get('Contents', [])

            if isinstance(contents, dict):
                contents = [contents]

            content_keys.extend([content['Key'] for content in contents])
            if len(content_keys) > 0:
                marker = content_keys[-1]

        # Query against non-existant folder does not return 404
        if len(content_keys) == 0:
            # MinIO cannot return Contents with a leaf folder itself.
            if await self._folder_prefix_exists(prefix):
                content_keys = [prefix]
            else:
                raise exceptions.NotFoundError(str(path))

        for content_key in content_keys[::-1]:
            resp = await self.make_request(
                'DELETE',
                self.bucket.new_key(content_key).generate_url(settings.TEMP_URL_SECS, 'DELETE'),
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
        # "versions" in "query_parameters" is required for generate_url().
        # SignatureDoesNotMatch is returned when "versions" is not specified.
        query_params = {'prefix': prefix, 'delimiter': '/', 'versions': ''}
        url = functools.partial(self.bucket.generate_url, settings.TEMP_URL_SECS, 'GET', query_parameters=query_params)
        try:
            resp = await self.make_request(
                'GET',
                url,
                params=query_params,
                expects=(200,),
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
            S3CompatRevision(item)
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

        async with self.request(
            'PUT',
            functools.partial(self.bucket.new_key(path.full_path).generate_url, settings.TEMP_URL_SECS, 'PUT'),
            skip_auto_headers={'CONTENT-TYPE'},
            expects=(200, 201, ),
            throws=exceptions.CreateFolderError
        ):
            return S3CompatFolderMetadata(self, {'Prefix': path.full_path})

    async def _metadata_file(self, path, revision=None):
        if revision == 'Latest':
            revision = None
        resp = await self.make_request(
            'HEAD',
            functools.partial(
                self.bucket.new_key(path.full_path).generate_url,
                settings.TEMP_URL_SECS,
                'HEAD',
                query_parameters={'versionId': revision} if revision else None
            ),
            expects=(200, ),
            throws=exceptions.MetadataError,
        )
        await resp.release()
        return S3CompatFileMetadataHeaders(self, path.full_path, resp.headers)

    async def _metadata_folder(self, path):
        prefix = path.full_path.lstrip('/')  # '/' -> '', '/A/B' -> 'A/B'
        params = {'prefix': prefix, 'delimiter': '/'}
        resp = await self.make_request(
            'GET',
            functools.partial(self.bucket.generate_url, settings.TEMP_URL_SECS, 'GET'),
            params=params,
            expects=(200, ),
            throws=exceptions.MetadataError,
        )

        contents = await resp.read()

        parsed = xmltodict.parse(contents, strip_whitespace=False)['ListBucketResult']

        contents = parsed.get('Contents', [])
        prefixes = parsed.get('CommonPrefixes', [])

        if not contents and not prefixes and not path.is_root:
            # If contents and prefixes are empty then this "folder"
            # must exist as a key with a / at the end of the name
            # if the path is root there is no need to test if it exists
            resp = await self.make_request(
                'HEAD',
                functools.partial(self.bucket.new_key(prefix).generate_url, settings.TEMP_URL_SECS, 'HEAD'),
                expects=(200, ),
                throws=exceptions.MetadataError,
            )
            await resp.release()

        if isinstance(contents, dict):
            contents = [contents]

        if isinstance(prefixes, dict):
            prefixes = [prefixes]

        items = [
            S3CompatFolderMetadata(self, item)
            for item in prefixes
        ]

        for content in contents:
            if content['Key'] == path.full_path:  # self
                continue

            if content['Key'].endswith('/'):
                items.append(S3CompatFolderKeyMetadata(self, content))
            else:
                items.append(S3CompatFileMetadata(self, content))

        return items
