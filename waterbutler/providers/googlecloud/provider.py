import re
import json
import time
import base64
import typing
import hashlib
import logging
import binascii
from http import HTTPStatus

from oauth2client.service_account import ServiceAccountCredentials

from waterbutler.core.path import WaterButlerPath
from waterbutler.core.provider import BaseProvider
from waterbutler.core.streams import BaseStream, HashStreamWriter, ResponseStreamReader
from waterbutler.core.metadata import BaseMetadata, BaseFileMetadata
from waterbutler.core.exceptions import (MetadataError, NotFoundError, UploadError, DownloadError,
                                         UploadChecksumMismatchError, InvalidProviderConfigError,
                                         DeleteError, CopyError, WaterButlerError, )

from waterbutler.providers.googlecloud import utils
from waterbutler.providers.googlecloud import settings as pd_settings
from waterbutler.providers.googlecloud.metadata import (BaseGoogleCloudMetadata,
                                                        GoogleCloudFileMetadata,
                                                        GoogleCloudFolderMetadata, )

logger = logging.getLogger(__name__)


class GoogleCloudProvider(BaseProvider):
    """Provider for Google's Cloud Storage Service.

    General API Docs: https://cloud.google.com/storage/docs/apis
    JSON API Docs: https://cloud.google.com/storage/docs/json_api/
    XML API Docs: https://cloud.google.com/storage/docs/xml-api/overview

    Please note the name of the service is "Cloud Storage" on "Google Cloud Platform".  However,
    it is named ``GoogleCloudProvider`` for better clarity and consistency.  "Google Cloud", "Cloud
    Storage" and "Google Cloud Storage" are used interchangeably.

    """

    NAME = 'googlecloud'

    # BASE URL for JSON API
    BASE_URL = pd_settings.BASE_URL_JSON

    # BASE URL for XML API
    BASE_URL_XML = pd_settings.BASE_URL_XML

    # EXPIRATION for Signed Request/URL for XML API
    SIGNATURE_EXPIRATION = pd_settings.SIGNATURE_EXPIRATION

    # The action for copy
    COPY_ACTION = pd_settings.COPY_ACTION

    def __init__(self, auth: dict, credentials: dict, settings: dict):
        """Initialize a provider instance with the given params.

        1. Here is an example of the settings for the "osfstorage" addon in OSF.

            WATERBUTLER_CREDENTIALS = {
                'storage': {
                    'token': 'change_me',
                    'json_creds': 'change_me'
                }
            }

            WATERBUTLER_SETTINGS = {
                'storage': {
                    'provider': 'change_me',
                    'bucket': 'change_me',
                    'region': 'change_me',
                },
            }

            WATERBUTLER_RESOURCE = 'bucket'

        2. More about authentication and authorization for Google Cloud

        Although WaterButler does not handle authentication/authorization, it is still worthwhile to
        understand how Google Cloud works.  As mentioned below in their documentations, Google Cloud
        uses OAuth 2.0.  WaterButler obtains the access token from OSF and set the "Authorization"
        header in every API requests.

        Docs: https://cloud.google.com/storage/docs/authentication

        "Cloud Storage uses OAuth 2.0 for API authentication and authorization. Authentication is
        the process of determining the identity of a client."

        "A server-centric flow allows an application to directly hold the credentials of a service
        account to complete authentication."

        "When you use a service account to authenticate your application, you do not need a user to
        authenticate to get an access token. Instead, you obtain a private key from the Google Cloud
        Platform Console, which you then use to send a signed request for an access token."
        """

        super().__init__(auth, credentials, settings)

        self.bucket = settings.get('bucket')
        if not self.bucket:
            raise InvalidProviderConfigError(self.NAME, message='Missing bucket settings')

        # TODO: replaces self.creds with self.json_creds after OSF/DevOps update
        # self.json_creds = credentials.get('json_creds')
        # if not self.json_creds:
        #     raise InvalidProviderConfigError(
        #         self.NAME,
        #         message='Missing service account credentials'
        #     )
        self.creds = ServiceAccountCredentials.from_json_keyfile_name(pd_settings.CREDS_PATH)

        # TODO: OSF/DevOps must guarantee that the region is correct for the given bucket
        self.region = settings.get('region')

        # TODO: remove self.access_token XML API refactor
        self.access_token = credentials.get('token')

    @property
    def default_headers(self) -> dict:

        return {'Authorization': 'Bearer {}'.format(self.access_token)}

    async def validate_v1_path(self, path: str, **kwargs) -> WaterButlerPath:
        return await self.validate_path(path)

    async def validate_path(self, path: str, **kwargs) -> WaterButlerPath:
        return WaterButlerPath(path)

    async def metadata(
            self,
            path: WaterButlerPath,
            **kwargs
    ) -> typing.Union[BaseGoogleCloudMetadata, typing.List[BaseGoogleCloudMetadata]]:
        """Get the metadata about the object with the given WaterButler path.

        :param path: the WaterButler path to the file or folder
        :param kwargs: additional kwargs are ignored
        :rtype BaseGoogleCloudMetadata: for file
        :rtype List<BaseGoogleCloudMetadata>: for folder
        """

        if path.is_folder:
            return await self._metadata_folder(path)
        else:
            return await self._metadata_object(path, is_folder=False)

    # TODO: refactor to use XML API
    async def upload(
            self,
            stream: BaseStream,
            path: WaterButlerPath,
            *args,
            **kwargs
    ) -> typing.Tuple[BaseGoogleCloudMetadata, bool]:
        """Upload a stream with the given WaterButler path.

        Google Cloud Storage: Objects - Insert
        JSON API Docs: https://cloud.google.com/storage/docs/json_api/v1/objects/insert

        "In the JSON API, the objects resource md5Hash and crc32c properties contain base64-encoded
        MD5 and CRC32c hashes, respectively."  Need to encode the MD5 hash calculated by stream
        writer.  For more information, refer to https://cloud.google.com/storage/docs/hashes-etags.

        :param stream: the stream to post
        :param path: the WaterButler path of the file to upload
        :param args: additional args are ignored
        :param kwargs: additional kwargs are ignored
        :rtype BaseGoogleCloudMetadata:
        :rtype bool:
        """

        created = not await self.exists(path)

        stream.add_writer('md5', HashStreamWriter(hashlib.md5))

        http_method = 'POST'
        query = {
            'uploadType': 'media',
            'name': utils.get_obj_name(path, is_folder=False)
        }
        upload_url = self.build_url(base_url=self.BASE_URL + '/upload', **query)
        headers = {'Content-Length': str(stream.size)}

        resp = await self.make_request(
            http_method,
            upload_url,
            data=stream,
            headers=headers,
            expects=(HTTPStatus.OK,),
            throws=UploadError
        )

        try:
            data = await resp.json()
        except (TypeError, json.JSONDecodeError):
            await resp.release()
            raise MetadataError('Failed to parse response, expecting JSON format.')

        # Convert the base64 encoded MD5 hash to hex digest representation.  Both `.encode()` and
        # `.decode()` are used for explicit conversion between "bytes" and "string"
        hex_digest = binascii.hexlify(base64.b64decode(data.get('md5Hash', '').encode())).decode()

        if hex_digest != stream.writers['md5'].hexdigest:
            raise UploadChecksumMismatchError()

        return GoogleCloudFileMetadata(data), created

    # TODO: refactor to use XML API
    async def download(
            self,
            path: WaterButlerPath,
            accept_url=False,
            range=None,
            **kwargs
    ) -> typing.Union[str, ResponseStreamReader]:
        """Download the object with the given path.  The behavior of download differs depending on
        the value of ``accept_url``.  Otherwise,

        1. ``accept_url == True``

            This method returns a signed URL with a short-lived signature that directly downloads
            the file from the provider.  Use the following XML API instead of JSON API for direct
            download.  See "Authentication by Signed URLs" for why and see Content Disposition for
            how to trigger download.

            XML API: https://cloud.google.com/storage/docs/xml-api/get-object-download

        2. ``accept_url == False``

            The download goes through WaterButler and the method returns a ``ResponseStreamReader``.

            Google Cloud Storage: Objects - Get
            JSON API: https://cloud.google.com/storage/docs/json_api/v1/objects/get

            "By default, this responds with an object resource in the response body. If you provide
            the URL parameter alt=media, then it will respond with the object data in the response
            body."

            Note: download request on folders return HTTP 200 OK with empty body. The action doesn't
            do anything. It doesn't make any sense just to download the folder anyway.

        3. Authentication by Signed URL

            Google Cloud Storage: Signed URLs
            Docs: https://cloud.google.com/storage/docs/access-control/signed-urls

            "Signed URLs can only be used to access resources in Google Cloud Storage through the
            XML API."

            How to Sign URL with python:
                https://cloud.google.com/storage/docs/access-control/create-signed-urls-program

        4. Content Disposition

            Docs: https://cloud.google.com/storage/docs/xml-api/reference-headers
                  #responsecontentdisposition

            "A query string parameter that allows content-disposition to be overridden for
            authenticated GET requests."

        Note: ``OSFStorageProvider`` never uses the inner provider to calls ``download`` on folders

        :param path: the WaterButler path to the object to download
        :param accept_url: the flag to solicit a direct time-limited download url from the provider
        :param range: the Range HTTP request header
        :param kwargs: additional kwargs are ignored
        :rtype str:
        :rtype ResponseStreamReader:
        """

        if path.is_folder:
            raise DownloadError('Cannot download folders', code=HTTPStatus.BAD_REQUEST)

        http_method = 'GET'
        obj_name = utils.get_obj_name(path, is_folder=False)
        display_name = kwargs.get('displayName', path.name)

        # Return a direct download URL (signed, short-lived) from the provider
        if accept_url:
            content_md5 = ''
            content_type = ''
            expires = int(time.time()) + self.SIGNATURE_EXPIRATION
            segments = (self.bucket, obj_name, )
            url_path = utils.build_url('', *segments, **{})
            string_to_sign = '\n'.join([http_method, content_md5, content_type, str(expires), url_path])
            encoded_signature = base64.b64encode(self.creds.sign_blob(string_to_sign)[1])
            query = {
                'response-content-disposition': 'attachment; filename={}'.format(display_name),
                'GoogleAccessId': self.creds.service_account_email,
                'Expires': str(expires),
                'Signature': encoded_signature
            }
            signed_url = utils.build_url(self.BASE_URL_XML, *segments, **query)
            return signed_url

        # Return a ``ResponseStreamReader``
        query = {'alt': 'media'}
        download_url = self.build_url(
            base_url=self.BASE_URL,
            obj_name=obj_name,
            **query
        )
        resp = await self.make_request(
            http_method,
            download_url,
            range=range,
            expects=(HTTPStatus.OK, HTTPStatus.PARTIAL_CONTENT),
            throws=DownloadError
        )

        return ResponseStreamReader(resp)

    async def delete(self, path: WaterButlerPath, *args, **kwargs) -> None:
        """Deletes the file object with the specified WaterButler path.

        :param path: the WaterButler path of the object to delete
        :param args: additional args are ignored
        :param kwargs: additional kwargs are ignored
        :rtype None:
        """

        if path.is_folder:
            raise DeleteError('Folder deletion is not supported')
        else:
            return await self._delete_file(path)

    async def intra_copy(
            self,
            dest_provider: BaseProvider,
            source_path: WaterButlerPath,
            dest_path: WaterButlerPath
    ) -> typing.Tuple[BaseMetadata, bool]:
        """Copy file objects within the same Google Cloud Storage Provider.

        :param dest_provider: the destination provider, must be the same as the source one
        :param source_path: the source WaterButler path for the object to copy from
        :param dest_path: the destination WaterButler path for the object to copy to
        :rtype BaseMetadata:
        :rtype bool:
        """

        if source_path.is_folder and dest_path.is_folder:
            raise CopyError('Folder intra-copy is not supported')
        if not source_path.is_folder and not dest_path.is_folder:
            return await self._intra_copy_file(dest_provider, source_path, dest_path)

        raise CopyError('Cannot copy between a file and a folder')

    def can_intra_copy(self, other: BaseProvider, path: WaterButlerPath = None) -> bool:
        """Google Cloud Storage XML API supports intra-copy for files.  Intra-copy for folders
        requires "BATCH" request support.  JSON API supports such request while there is no
        documentation on the XML API.  It may be eligible given the fact that Amazon S3 supports
        a similar action called "BULK" request.  Need more investigation in Phase 2.  Phase 1
        OSFStorage does not perform any folder level actions (e.g. delete, copy ,move, etc.)
        using the inner provider.

        TODO [new ticket]: investigate whether folders are eligible; if so, enable for folders

        "The authenticated user must have READER permissions on the source object on the source
        bucket, and WRITER permissions on the destination bucket."
        """
        return self == other and not getattr(path, 'is_folder', False)

    def can_intra_move(self, other: BaseProvider, path: WaterButlerPath = None) -> bool:
        """Google Cloud Storage XML API supports intra move for files.  It is a combination of
        intra-copy and delete. Please refer to ``can_intra_copy()`` and ``can_batch_delete`` for
        more information.

        "The authenticated user must have WRITE permissions on the source object on the source
        bucket, and WRITER permissions on the destination bucket."
        """

        return self.can_intra_copy(other, path)

    def can_duplicate_names(self):
        """Google Cloud Storage allows a file and a folder to share the same name.
        """
        return True

    # TODO: refactor to use XML API
    def build_url(
            self,
            base_url: str = None,
            obj_name: str = None,
            obj_action: str = None,
            dest_bucket: str = None,
            dest_obj_name: str = None,
            **query
    ) -> str:
        """Build the request URL for various object actions.

        :param base_url: the base url for Google Cloud Storage API
                         use ``BASE_URL`` for most cases
                         use ``UPLOAD_URL`` for uploading files and creating folders
                         use ``None`` for relative URL without
        :param obj_name: the object name of the object or src object
        :param obj_action: the action to perform from (between) src to (and) the object
        :param dest_bucket: the bucket for the dest object
        :param dest_obj_name: the object name of the dest object
        :param query: the dict for query parameters
        :rtype str:
        """

        segments = ('storage', 'v1', 'b', self.bucket, 'o',)

        if obj_name:
            segments = segments + (obj_name,)

        if obj_action and dest_bucket and dest_obj_name:
            segments = segments + (obj_action, 'b', dest_bucket, 'o', dest_obj_name,)

        if base_url:
            return utils.build_url(base_url, *segments, **query)
        else:
            return utils.build_url('', *segments, **query)

    # TODO: remove if no longer relevant; otherwise, refactor to use XML API
    async def _exists_folder(self, path: WaterButlerPath) -> bool:
        """Check if a folder with the given WaterButler path exists. Calls ``_metadata_object()``.

        For folders, ``exists()`` from the core provider calls ``metadata()``, which further calls
        ``_metadata_folder``.  This makes simple action more complicated and more expensive.

        However, ``exists()`` for files does not have this limitation.

        :param path: the WaterButler path of the folder to check
        :rtype bool:
        """

        if not path.is_folder:
            raise WaterButlerError('Wrong type, expecting folder but received file')

        try:
            await self._metadata_object(path, is_folder=True)
        except NotFoundError:
            return False
        except MetadataError as exc:
            if exc.code != HTTPStatus.NOT_FOUND:
                raise
            return False
        return True

    # TODO: refactor to use XML API
    async def _metadata_object(
            self,
            path: WaterButlerPath,
            is_folder: bool = False
    ) -> BaseGoogleCloudMetadata:
        """Get the metadata about the object itself with the given WaterButler path.

        Google Cloud Storage: Objects - Get
        JSON API Docs: https://cloud.google.com/storage/docs/json_api/v1/objects/get

        "By default, this responds with an object resource in the response body. If you provide the
        URL parameter alt=media, then it will respond with the object data in the response body."

        Please note that ``is_folder`` flag is explicitly used. Providing the wrong type will fail
        all the time. This is also true for many internal/private/helper/utility methods of/for this
        class. They are not exposed to usage outside, including the parent classes.

        :param path: the WaterButler path of the object
        :param is_folder: whether the object is a file or folder
        :rtype BaseGoogleCloudMetadata
        """

        http_method = 'GET'
        obj_name = utils.get_obj_name(path, is_folder=is_folder)
        metadata_url = self.build_url(base_url=self.BASE_URL, obj_name=obj_name)

        resp = await self.make_request(
            http_method,
            metadata_url,
            expects=(HTTPStatus.OK,),
            throws=MetadataError
        )

        try:
            data = await resp.json()
        except (TypeError, json.JSONDecodeError):
            await resp.release()
            raise MetadataError('Failed to parse response, expecting JSON format.')

        if is_folder:
            return GoogleCloudFolderMetadata(data)
        else:
            return GoogleCloudFileMetadata(data)

    # TODO: refactor to use XML API
    async def _metadata_folder(
            self,
            path: WaterButlerPath
    ) -> typing.List[BaseGoogleCloudMetadata]:
        """Get the metadata about the folder's immediate children with the given WaterButler path.

        This method calls ``_metadata_all_children()``, which makes only one request to retrieve all
        all the children including itself.  Iterate through the children list and use "regex" to
        select only immediate ones.  For more information refer to ``_metadata_all_children()``.

        :param path: the WaterButler path of the folder
        :rtype List<BaseGoogleCloudMetadata>
        """

        # Retrieve a list of metadata for all immediate and non-immediate children
        prefix, items = await self._metadata_all_children(path)

        # Raise ``NotFoundError`` if folder does not exist
        if not items:
            raise NotFoundError(path.path)

        # Iterate through the metadata_list and only select immediate children
        metadata_list_immediate = []
        for item in items:
            pattern = r'^(' + re.escape(prefix) + r')[^/]+[/]?'
            name = item.get('name', '')
            if re.fullmatch(pattern, name):
                if name.endswith('/'):
                    metadata_list_immediate.append(GoogleCloudFolderMetadata(item))
                else:
                    metadata_list_immediate.append(GoogleCloudFileMetadata(item))

        return metadata_list_immediate

    # TODO: refactor to use XML API
    async def _metadata_all_children(
            self,
            path: WaterButlerPath
    ) -> typing.Tuple[str, typing.List[dict]]:
        """Get the metadata about all of the folder's children with the given WaterButler path.

        :param path: the WaterButler path of the folder
        :rtype str
        :rtype List<BaseGoogleCloudMetadata>
        """

        http_method = 'GET'
        prefix = utils.get_obj_name(path, is_folder=True)
        query = {'prefix': prefix}
        metadata_url = self.build_url(base_url=self.BASE_URL, **query)

        resp = await self.make_request(
            http_method,
            metadata_url,
            expects=(HTTPStatus.OK,),
            throws=MetadataError
        )

        try:
            data = await resp.json()
        except (TypeError, json.JSONDecodeError):
            await resp.release()
            raise MetadataError('Failed to parse response, expecting JSON format.')

        return prefix, data.get('items', [])

    # TODO: refactor to use XML API
    async def _delete_file(self, path: WaterButlerPath) -> None:
        """Deletes the file with the specified WaterButler path.

        :param path: the WaterButler path of the file to delete
        :rtype None:
        """

        http_method = 'DELETE'
        delete_url = self.build_url(
            base_url=self.BASE_URL,
            obj_name=utils.get_obj_name(path)
        )

        resp = await self.make_request(
            http_method,
            delete_url,
            expects=(HTTPStatus.NO_CONTENT,),
            throws=DeleteError,
        )

        await resp.release()

    # TODO: refactor to use XML API
    async def _intra_copy_file(
            self,
            dest_provider: BaseProvider,
            source_path: WaterButlerPath,
            dest_path: WaterButlerPath
    ) -> typing.Tuple[BaseFileMetadata, bool]:
        """Copy files within the same Google Cloud Storage provider, overwrite existing ones if
        there are any.  Return the metadata of the destination file, created or overwritten.

        :param dest_provider: the destination provider, must be the same as the source one
        :param source_path: the source WaterButler path for the object to copy from
        :param dest_path: the destination WaterButler path for the object to copy to
        :rtype BaseMetadata:
        :rtype bool:
        """

        created = not await dest_provider.exists(dest_path)

        http_method = 'POST'
        copy_url = self.build_url(
            base_url=self.BASE_URL,
            obj_name=utils.get_obj_name(source_path),
            obj_action=self.COPY_ACTION,
            dest_bucket=dest_provider.bucket,
            dest_obj_name=utils.get_obj_name(dest_path)
        )
        headers = {
            'Content-Length': '0',
            'Content-Type': ''
        }

        resp = await self.make_request(
            http_method,
            copy_url,
            headers=headers,
            expects=(HTTPStatus.OK,),
            throws=CopyError
        )
        try:
            data = await resp.json()
        except (TypeError, json.JSONDecodeError):
            await resp.release()
            raise MetadataError('Failed to parse response, expecting JSON format.')

        return GoogleCloudFileMetadata(data), created
