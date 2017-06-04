import os
import hashlib
import functools
from urllib import parse

import xmltodict

import xml.sax.saxutils

from boto import config as boto_config
from boto.compat import BytesIO  # type: ignore
from boto.utils import compute_md5
from boto.auth import get_auth_handler
from boto.s3.connection import S3Connection
from boto.s3.connection import OrdinaryCallingFormat

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.s3 import settings
from waterbutler.providers.s3.metadata import S3Revision
from waterbutler.providers.s3.metadata import S3FileMetadata
from waterbutler.providers.s3.metadata import S3FolderMetadata
from waterbutler.providers.s3.metadata import S3FolderKeyMetadata
from waterbutler.providers.s3.metadata import S3FileMetadataHeaders


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
        """
        .. note::

            Neither `S3Connection#__init__` nor `S3Connection#get_bucket`
            sends a request.

        :param dict auth: Not used
        :param dict credentials: Dict containing `access_key` and `secret_key`
        :param dict settings: Dict containing `bucket`
        """
        super().__init__(auth, credentials, settings)

        self.connection = S3Connection(credentials['access_key'],
                credentials['secret_key'], calling_format=OrdinaryCallingFormat())
        self.bucket = self.connection.get_bucket(settings['bucket'], validate=False)
        self.encrypt_uploads = self.settings.get('encrypt_uploads', False)
        self.region = None

    async def validate_v1_path(self, path, **kwargs):
        await self._check_region()

        if path == '/':
            return WaterButlerPath(path)

        implicit_folder = path.endswith('/')

        if implicit_folder:
            params = {'prefix': path, 'delimiter': '/'}
            resp = await self.make_request(
                'GET',
                functools.partial(self.bucket.generate_url, settings.TEMP_URL_SECS, 'GET', query_parameters=params),
                params=params,
                expects=(200, 404),
                throws=exceptions.MetadataError,
            )
        else:
            resp = await self.make_request(
                'HEAD',
                functools.partial(self.bucket.new_key(path).generate_url, settings.TEMP_URL_SECS, 'HEAD'),
                expects=(200, 404),
                throws=exceptions.MetadataError,
            )

        await resp.release()

        if resp.status == 404:
            raise exceptions.NotFoundError(str(path))

        return WaterButlerPath(path)

    async def validate_path(self, path, **kwargs):
        return WaterButlerPath(path)

    def can_duplicate_names(self):
        return True

    def can_intra_copy(self, dest_provider, path=None):
        return type(self) == type(dest_provider) and not getattr(path, 'is_dir', False)

    def can_intra_move(self, dest_provider, path=None):
        return type(self) == type(dest_provider) and not getattr(path, 'is_dir', False)

    async def intra_copy(self, dest_provider, source_path, dest_path):
        """Copy key from one S3 bucket to another. The credentials specified in
        `dest_provider` must have read access to `source.bucket`.
        """
        await self._check_region()
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

    async def download(self, path, accept_url=False, version=None, range=None, **kwargs):
        """Returns a ResponseWrapper (Stream) for the specified path
        raises FileNotFoundError if the status from S3 is not 200

        :param str path: Path to the key you want to download
        :param dict \*\*kwargs: Additional arguments that are ignored
        :rtype: :class:`waterbutler.core.streams.ResponseStreamReader`
        :raises: :class:`waterbutler.core.exceptions.DownloadError`
        """
        await self._check_region()

        if not path.is_file:
            raise exceptions.DownloadError('No file specified for download', code=400)

        if not version or version.lower() == 'latest':
            query_parameters = None
        else:
            query_parameters = {'versionId': version}

        if kwargs.get('displayName'):
            response_headers = {'response-content-disposition': 'attachment; filename*=UTF-8\'\'{}'.format(parse.quote(kwargs['displayName']))}
        else:
            response_headers = {'response-content-disposition': 'attachment'}

        url = functools.partial(
            self.bucket.new_key(path.path).generate_url,
            settings.TEMP_URL_SECS,
            query_parameters=query_parameters,
            response_headers=response_headers
        )

        if accept_url:
            return url()

        resp = await self.make_request(
            'GET',
            url,
            range=range,
            expects=(200, 206),
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
        stream.add_writer('md5', streams.HashStreamWriter(hashlib.md5))

        headers = {'Content-Length': str(stream.size)}

        # this is usually set in boto.s3.key.generate_url, but do it here
        # do be explicit about our header payloads for signing purposes
        if self.encrypt_uploads:
            headers['x-amz-server-side-encryption'] = 'AES256'

        upload_url = functools.partial(
            self.bucket.new_key(path.path).generate_url,
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
        # md5 is returned as ETag header as long as server side encryption is not used.
        # TODO: nice assertion error goes here
        assert resp.headers['ETag'].replace('"', '') == stream.writers['md5'].hexdigest

        await resp.release()
        return (await self.metadata(path, **kwargs)), not exists

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
            resp = await self.make_request(
                'DELETE',
                self.bucket.new_key(path.path).generate_url(settings.TEMP_URL_SECS, 'DELETE'),
                expects=(200, 204, ),
                throws=exceptions.DeleteError,
            )
            await resp.release()
        else:
            await self._delete_folder(path, **kwargs)

    async def _delete_folder(self, path, **kwargs):
        """Query for recursive contents of folder and delete in batches of 1000

        Called from: func: delete if not path.is_file

        Calls: func: self._check_region
               func: self.make_request
               func: self.bucket.generate_url

        :param *ProviderPath path: Path to be deleted

        On S3, folders are not first-class objects, but are instead inferred
        from the names of their children.  A regular DELETE request issued
        against a folder will not work unless that folder is completely empty.
        To fully delete an occupied folder, we must delete all of the comprising
        objects.  Amazon provides a bulk delete operation to simplify this.
        """
        await self._check_region()

        more_to_come = True
        content_keys = []
        query_params = {'prefix': path.path}
        marker = None

        while more_to_come:
            if marker is not None:
                query_params['marker'] = marker

            resp = await self.make_request(
                'GET',
                self.bucket.generate_url(settings.TEMP_URL_SECS, 'GET', query_parameters=query_params),
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
            raise exceptions.NotFoundError(str(path))

        while len(content_keys) > 0:
            key_batch = content_keys[:1000]
            del content_keys[:1000]

            payload = '<?xml version="1.0" encoding="UTF-8"?>'
            payload += '<Delete>'
            payload += ''.join(map(
                lambda x: '<Object><Key>{}</Key></Object>'.format(xml.sax.saxutils.escape(x)),
                key_batch
            ))
            payload += '</Delete>'
            payload = payload.encode('utf-8')
            md5 = compute_md5(BytesIO(payload))

            query_params = {'delete': ''}
            headers = {
                'Content-Length': str(len(payload)),
                'Content-MD5': md5[1],
                'Content-Type': 'text/xml',
            }

            # We depend on a customized version of boto that can make query parameters part of
            # the signature.
            url = functools.partial(
                self.bucket.generate_url,
                settings.TEMP_URL_SECS,
                'POST',
                query_parameters=query_params,
                headers=headers,
            )
            resp = await self.make_request(
                'POST',
                url,
                params=query_params,
                data=payload,
                headers=headers,
                expects=(200, 204, ),
                throws=exceptions.DeleteError,
            )
            await resp.release()

    async def revisions(self, path, **kwargs):
        """Get past versions of the requested key

        :param str path: The path to a key
        :rtype list:
        """
        await self._check_region()

        query_params = {'prefix': path.path, 'delimiter': '/', 'versions': ''}
        url = functools.partial(self.bucket.generate_url, settings.TEMP_URL_SECS, 'GET', query_parameters=query_params)
        resp = await self.make_request(
            'GET',
            url,
            params=query_params,
            expects=(200, ),
            throws=exceptions.MetadataError,
        )
        content = await resp.read()
        versions = xmltodict.parse(content)['ListVersionsResult'].get('Version') or []

        if isinstance(versions, dict):
            versions = [versions]

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
            return (await self._metadata_folder(path))

        return (await self._metadata_file(path, revision=revision))

    async def create_folder(self, path, folder_precheck=True, **kwargs):
        """
        :param str path: The path to create a folder at
        """
        await self._check_region()

        WaterButlerPath.validate_folder(path)

        if folder_precheck:
            if (await self.exists(path)):
                raise exceptions.FolderNamingConflict(str(path))

        async with self.request(
            'PUT',
            functools.partial(self.bucket.new_key(path.path).generate_url, settings.TEMP_URL_SECS, 'PUT'),
            skip_auto_headers={'CONTENT-TYPE'},
            expects=(200, 201),
            throws=exceptions.CreateFolderError
        ):
            return S3FolderMetadata({'Prefix': path.path})

    async def _metadata_file(self, path, revision=None):
        await self._check_region()

        if revision == 'Latest':
            revision = None
        resp = await self.make_request(
            'HEAD',
            functools.partial(
                self.bucket.new_key(path.path).generate_url,
                settings.TEMP_URL_SECS,
                'HEAD',
                query_parameters={'versionId': revision} if revision else None
            ),
            expects=(200, ),
            throws=exceptions.MetadataError,
        )
        await resp.release()
        return S3FileMetadataHeaders(path.path, resp.headers)

    async def _metadata_folder(self, path):
        await self._check_region()

        params = {'prefix': path.path, 'delimiter': '/'}
        resp = await self.make_request(
            'GET',
            functools.partial(self.bucket.generate_url, settings.TEMP_URL_SECS, 'GET', query_parameters=params),
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
                functools.partial(self.bucket.new_key(path.path).generate_url, settings.TEMP_URL_SECS, 'HEAD'),
                expects=(200, ),
                throws=exceptions.MetadataError,
            )
            await resp.release()

        if isinstance(contents, dict):
            contents = [contents]

        if isinstance(prefixes, dict):
            prefixes = [prefixes]

        items = [
            S3FolderMetadata(item)
            for item in prefixes
        ]

        for content in contents:
            if content['Key'] == path.path:
                continue

            if content['Key'].endswith('/'):
                items.append(S3FolderKeyMetadata(content))
            else:
                items.append(S3FileMetadata(content))

        return items

    async def _check_region(self):
        """Lookup the region via bucket name, then update the host to match.

        Manually constructing the connection hostname allows us to use OrdinaryCallingFormat
        instead of SubdomainCallingFormat, which can break on buckets with periods in their name.
        The default region, US East (N. Virginia), is represented by the empty string and does not
        require changing the host.  Ireland is represented by the string 'EU', with the host
        parameter 'eu-west-1'.  All other regions return the host parameter as the region name.

        Region Naming: http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region
        """
        if self.region is None:
            self.region = await self._get_bucket_region()
            if self.region == 'EU':
                self.region = 'eu-west-1'

            if self.region != '':
                self.connection.host = self.connection.host.replace('s3.', 's3-' + self.region + '.', 1)
                self.connection._auth_handler = get_auth_handler(
                    self.connection.host, boto_config, self.connection.provider, self.connection._required_auth_capability())

        self.metrics.add('region', self.region)

    async def _get_bucket_region(self):
        """Bucket names are unique across all regions.

       Endpoint doc:
       http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGETlocation.html
        """
        resp = await self.make_request(
            'GET',
            functools.partial(self.bucket.generate_url, settings.TEMP_URL_SECS, 'GET', query_parameters={'location': ''}),
            expects=(200, ),
            throws=exceptions.MetadataError,
        )

        contents = await resp.read()
        parsed = xmltodict.parse(contents, strip_whitespace=False)
        return parsed['LocationConstraint'].get('#text', '')
