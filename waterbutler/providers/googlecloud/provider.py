import re
import json
import time
import uuid
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
from waterbutler.core.exceptions import (MetadataError, NotFoundError, CreateFolderError,
                                         UploadError, UploadChecksumMismatchError,
                                         DownloadError, DeleteError, CopyError,
                                         WaterButlerError, InvalidProviderConfigError)

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

    TODO: add a list of quirks for Google Cloud Storage here (short version)

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

    # Batch request configurations
    BATCH_THRESHOLD = pd_settings.BATCH_THRESHOLD
    BATCH_MAX_RETRIES = pd_settings.BATCH_MAX_RETRIES
    BATCH_BOUNDARY = pd_settings.BATCH_BOUNDARY

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

        Note: ``OSFStorageProvider`` never uses the inner provider to call ``metadata`` on folders

        :param path: the WaterButler path to the file or folder
        :param kwargs: additional kwargs are ignored
        :rtype BaseGoogleCloudMetadata: for file
        :rtype List<BaseGoogleCloudMetadata>: for folder
        """

        if path.is_folder:
            return await self._metadata_folder(path)
        else:
            return await self._metadata_object(path, is_folder=False)

    async def create_folder(
            self,
            path: WaterButlerPath,
            **kwargs
    ) -> BaseGoogleCloudMetadata:
        """Create a folder with the given WaterButler path.

        Google Cloud Storage: Objects - Insert
        JSON API Docs: https://cloud.google.com/storage/docs/json_api/v1/objects/insert

        Create a folder in the Google Cloud is basically to upload an object (which is a folder or
        of which the name ends with a `/`). First, the size of the object (folder) is set to 0.
        Second, the Content-Type header is set to: application/x-www-form-urlencoded;charset=UTF-8

        1. It is possible to create a folder "/a/b/c/" when neither "/a/" or "/a/b/" exists.

        2. When creating a folder, if it already exists, throw a HTTP 409 Conflict

        3. ``create_folder``, though fully implemented, is not required for the limited version.

        Note: ``OSFStorageProvider`` never uses the inner provider to call ``create_folder``

        TODO [New Ticket #]: what if the folder name contains '/'? how about parent folders' name?

        :param path: the WaterButler path of the folder to create
        :param kwargs: additional kwargs are ignored
        :rtype: BaseGoogleCloudMetadata
        """

        if await self._exists_folder(path):
            raise CreateFolderError('Folder already exists.', code=HTTPStatus.CONFLICT)

        http_method = 'POST'
        query = {
            'uploadType': 'media',
            'name': utils.get_obj_name(path, is_folder=True)
        }
        upload_url = self.build_url(base_url=self.BASE_URL + '/upload', **query)
        headers = {
            'Content-Length': '0',
            'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8'
        }

        resp = await self.make_request(
            http_method,
            upload_url,
            headers=headers,
            expects=(HTTPStatus.OK,),
            throws=CreateFolderError
        )

        try:
            data = await resp.json()
        except (TypeError, json.JSONDecodeError):
            await resp.release()
            raise MetadataError('Failed to parse response, expecting JSON format.')

        return GoogleCloudFolderMetadata(data)

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
        """Deletes the object with the specified WaterButler path.

        Google Cloud Storage: Objects - Delete
        JSON API Docs: https://cloud.google.com/storage/docs/json_api/v1/objects/delete

        "Deletes an object and its metadata. Deletions are permanent if versioning is not enabled
        for the bucket, or if the generation parameter is used."

        Note: ``OSFStorageProvider`` never uses the inner provider to call ``delete`` on folders

        :param path: the WaterButler path of the object to delete
        :param args: additional args are ignored
        :param kwargs: additional kwargs are ignored
        :rtype None:
        """

        if path.is_folder:
            return await self._delete_folder(path)
        else:
            return await self._delete_file(path)

    async def intra_copy(
            self,
            dest_provider: BaseProvider,
            source_path: WaterButlerPath,
            dest_path: WaterButlerPath
    ) -> typing.Tuple[BaseMetadata, bool]:
        """Copy objects within the same Google Cloud Storage Provider.

        For files, issue a single "copyTo" request and return the target's metadata. For folders,
        first issue a metadata request to retrieve all the children (immediate and non-immediate),
        and then issue a batch "copyTo" request to copy all of them (including the folder itself).

        Please note that Google Cloud Storage provides two methods to copy objects:

        JSON API Docs:
            https://cloud.google.com/storage/docs/json_api/v1/objects/rewrite
            https://cloud.google.com/storage/docs/json_api/v1/objects/copy

        Currently, use "copyTo" since the response is the metadata of the destination file.

        Note: ``OSFStorageProvider`` never uses the inner provider to call ``intra_copy`` on folders

        # TODO: update related methods to use "rewriteTo" instead

        :param dest_provider: the destination provider, must be the same as the source one
        :param source_path: the source WaterButler path for the object to copy from
        :param dest_path: the destination WaterButler path for the object to copy to
        :rtype BaseMetadata:
        :rtype bool:
        """

        if source_path.is_folder and dest_path.is_folder:
            return await self._intra_copy_folder(dest_provider, source_path, dest_path)
        if not source_path.is_folder and not dest_path.is_folder:
            return await self._intra_copy_file(dest_provider, source_path, dest_path)

        raise CopyError('Cannot copy between a file and a folder')

    def can_intra_copy(self, other: BaseProvider, path: WaterButlerPath = None) -> bool:
        """Google Cloud Storage supports intra copy for both files and folders.

        "The authenticated user must have READER permissions on the source object on the source
        bucket, and WRITER permissions on the destination bucket."
        """

        return self == other

    def can_intra_move(self, other: BaseProvider, path: WaterButlerPath = None) -> bool:
        """Google Cloud Storage supports intra move for both file and folders.  It is a combination
        of intra copy and delete. For more information, please refer to ``can_intra_copy()``.

        "The authenticated user must have WRITE permissions on the source object on the source
        bucket, and WRITER permissions on the destination bucket."
        """

        return self == other

    def can_duplicate_names(self):
        """Google Cloud Storage allows a file and a folder to share the same name.
        """
        return True

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

    async def _metadata_all_children(
            self,
            path: WaterButlerPath
    ) -> typing.Tuple[str, typing.List[dict]]:
        """Get the metadata about all of the folder's children with the given WaterButler path.

        Google Cloud Storage: Objects - List
        JSON API Docs: https://cloud.google.com/storage/docs/json_api/v1/objects/list

        "In conjunction with the prefix filter, the use of the delimiter parameter allows the list
        method to operate like a directory listing, despite the object namespace being flat."

        In Google Cloud Storage, find all children of a given folder is equal to finding all objects
        of which the names are prefixed with the folder's object name.  Note that the folder itself
        is included in the result.  Set the object name of the folder as the PREFIX, and do not use
        a DELIMITER.  For example:

            "/storage/v1/b/gcloud-test.longzechen.com/o?prefix=test-folder-1%2F"

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

    async def _delete_folder(self, path: WaterButlerPath) -> None:
        """Deletes the folder and all its content with the specified WaterButler path.

        For Google Cloud Storage, `DELETE` a folder does delete the folder object.  However, given
        the storage is flat, everything else in (or more accurately, prefixed with) the folder are
        not deleted.  To fully delete a folder and all its contents, we need to delete each of its
        children (whether immediate or not).

        1. No need for recursive behavior.  Every children must have an object name that is prefixed
           with the object name of the folder.  And every object that is prefixed with the object
           name of the folder must be its children.

        2. Google Cloud Storage provides the option for "Sending Batch Requests" and one of the
           suggested use case  is "Deleting many objects".

        3. HARD: However, there are a few limitations with batch request but we HANDLES them.

           3.1 "You're LIMITED to 100 calls in a single batch request.  If you need to make more
               calls than that, use multiple batch requests."

           3.2 "The server may perform your calls in any order.  Don't count on their being executed
               in the order in which you specified them." In addition, the request is not atomic.
               Some may fail while others succeed.  The response status is HTTP 200 OK even when
               individual requests fail.  Need to parse the response to detect and handle failures.

        4. Quirk: If the folder does not exist, the server returns "HTTP 200 OK", instead of
           "HTTP 404 Not found".  If the response doesn't have an item list, we say the folder does
            not exists.  This saves us one extra request to check existence.

        JSON API Docs: https://cloud.google.com/storage/docs/json_api/v1/how-tos/batch

        Note: "OSFStorage Provider" never calls ``delete()`` on folders

        :param path: the WaterButler path of the folder to delete
        :rtype None:
        """

        # Retrieve a list of metadata for all immediate and non-immediate children
        prefix, items = await self._metadata_all_children(path)

        # Raise ``NotFoundError`` if folder does not exist
        if not items:
            raise NotFoundError(path.path)

        # Generate a UUID as the shared prefix of the Content-ID header fo each individual request.
        id_prefix = uuid.uuid4()

        # Iterate through the item list and delete one batch at a time
        while len(items) > 0:
            one_batch_of_items = items[:self.BATCH_THRESHOLD]
            del items[:self.BATCH_THRESHOLD]
            await self._batch_delete_items(id_prefix, one_batch_of_items)

        return

    async def _intra_copy_file(
            self,
            dest_provider: BaseProvider,
            source_path: WaterButlerPath,
            dest_path: WaterButlerPath
    ) -> typing.Tuple[BaseFileMetadata, bool]:
        """Copy files within the same Google Cloud Storage provider, overwrite existing ones if
        there are any.  Return the metadata of the destination file, created or overwritten.

        For 'POST' request with an empty body, Google Cloud API does not expect a Content-Type.
        However, need to explicitly provider an empty header to prevent it from being set to the
        default "application/octet-stream" which is not recognized by Google Cloud API.

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

    async def _intra_copy_folder(
            self,
            dest_provider: BaseProvider,
            source_path: WaterButlerPath,
            dest_path: WaterButlerPath,
    ) -> typing.Tuple[BaseGoogleCloudMetadata, bool]:
        """Copy folders and all its content within the same Google Cloud Storage provider. If it
        already exists, the folder and all its contents are deleted before the copy starts.  Return
        the metadata of the destination folder itself with updated ``._children``, and a bool flag
        for whether the folder is created or overwritten.

        Similar to ``_delete_folder``, batch request is used. Please refer to ``_delete_folder`` for
        more information on batch request, its limitations and our work-around.

        Note: "OSFStorage Provider" never calls ``intra_copy`` on folders

        :param dest_provider: the destination provider, must be the same as the source one
        :param source_path: the source WaterButler path for the object to copy from
        :param dest_path: the destination WaterButler path for the object to copy to
        :rtype BaseMetadata:
        :rtype bool:
        """

        created = await dest_provider._exists_folder(dest_path)
        if not created:
            await dest_provider._delete_folder(dest_path)

        # Retrieve a list of metadata for all immediate and non-immediate children
        src_prefix, items = await self._metadata_all_children(source_path)
        dest_prefix = utils.get_obj_name(dest_path, is_folder=True)

        # Raise ``NotFoundError`` if folder does not exist
        if not items:
            raise NotFoundError(src_prefix.path)

        # Generate a UUID as the shared prefix of the Content-ID header fo each individual request.
        id_prefix = uuid.uuid4()

        # Iterate through the item list and copy one batch at a time
        while len(items) > 0:
            items_one_batch = items[:self.BATCH_THRESHOLD]
            del items[:self.BATCH_THRESHOLD]
            await self._batch_copy_items(
                id_prefix,
                items_one_batch,
                src_prefix,
                dest_prefix,
                dest_provider.bucket
            )

        # TODO [New Ticket #]: build metadata based on responses from batch copy requests
        metadata_itself = await self._metadata_object(dest_path, is_folder=True)
        metadata_immediate = await self._metadata_folder(dest_path)
        metadata_itself.children = metadata_immediate
        return metadata_itself, created

    async def _batch_delete_items(self, id_prefix: str, items: list) -> None:
        """Make a batch request that deletes multiple objects on Google Cloud Storage.

        1. The number of objects must be less or equal to the ``BATCH_THRESHOLD``.
        2. Response is parsed and failures are detected.  Iteratively re-try failed requests.
        3. Raise ``DeleteError`` if the number of attempts goes above the ``BATCH_MAX_RETRIES``.

        :param id_prefix: the shared prefix of the Content-ID header for each requests
        :param items: the list of items to be deleted
        :rtype None:
        """

        http_method = 'POST'
        headers = {
            'Content-Type': 'multipart/mixed; boundary="{}"'.format(self.BATCH_BOUNDARY)
        }
        # Build the payload and requests map for the initial batch delete request
        init_payload, req_map = self._build_payload_for_batch_delete(items, id_prefix)

        init_req = True
        num_of_retries = 0
        req_list_failed = []

        while num_of_retries < self.BATCH_MAX_RETRIES:

            if init_req:
                init_req = False
                payload = init_payload
            elif len(req_list_failed) > 0:
                num_of_retries += 1
                payload = utils.build_payload_from_req_map(req_list_failed, req_map)
            else:
                raise DeleteError('The batch delete action has failed unexpectedly.')

            resp = await self.make_request(
                http_method,
                self.BASE_URL + '/batch',
                headers=headers,
                data=payload,
                expects=(HTTPStatus.OK,),
                throws=DeleteError
            )
            data = await resp.read()

            # Parse the response and find out if there are failures and what are they.
            req_list_failed = utils.parse_batch_delete_resp(data)

            # Break the loop if every request has succeeded
            if len(req_list_failed) == 0:
                break

        # Raise ``DeleteError`` if too many failed retries
        if num_of_retries >= self.BATCH_MAX_RETRIES:
            raise DeleteError('Too many failed delete requests.')

        return

    async def _batch_copy_items(
            self,
            id_prefix: str,
            items: list,
            src_prefix: str,
            dest_prefix: str,
            dest_bucket: str
    ) -> None:
        """Make a batch request that copy multiple items (objects) on Google Cloud Storage.

        1. The number of objects must be less or equal to the ``BATCH_THRESHOLD``.
        2. Response is parsed and failures are detected.  Iteratively re-try failed requests.
        3. Raise ``CopyError`` if the number of attempts goes above the ``BATCH_MAX_RETRIES``.

        :param id_prefix: the shared prefix for the Content-ID header
        :param items: the list of items/objects to be deleted
        :param src_prefix: the object name of the source folder
        :param dest_prefix: the object name of the destination folder
        :param dest_bucket: the name of the destination bucket
        :rtype None:
        """

        http_method = 'POST'
        headers = {
            'Content-Type': 'multipart/mixed; boundary="{}"'.format(self.BATCH_BOUNDARY)
        }
        # Build the payload and requests map for the initial batch delete request
        init_payload, req_map = self._build_payload_for_batch_copy(
            items,
            id_prefix,
            src_prefix,
            dest_prefix,
            dest_bucket
        )

        init_req = True
        num_of_retries = 0
        req_list_failed = []

        while num_of_retries < self.BATCH_MAX_RETRIES:

            if init_req:
                init_req = False
                payload = init_payload
            elif len(req_list_failed) > 0:
                num_of_retries += 1
                payload = utils.build_payload_from_req_map(req_list_failed, req_map)
            else:
                raise CopyError('The batch copy action has failed unexpectedly.')

            resp = await self.make_request(
                http_method,
                self.BASE_URL + '/batch',
                headers=headers,
                data=payload,
                expects=(HTTPStatus.OK,),
                throws=DeleteError
            )
            data = await resp.read()

            # Parse the response and find out if there are failures and what are they.
            _, req_list_failed = utils.parse_batch_copy_resp(data)

            # Break the loop if every request has succeeded
            if len(req_list_failed) == 0:
                break

        # Raise ``CopyError`` if too many failed retries
        if num_of_retries >= self.BATCH_MAX_RETRIES:
            raise CopyError('Too many failed copy requests.')

        return

    def _build_payload_for_batch_delete(
            self,
            items: list,
            id_prefix: str
    ) -> typing.Tuple[str, dict]:
        """Build the payload for batch delete request.  Return the full payload in string and a
        map of request id and payload for the request.

        :param items: the dict that contains all the objects
        :param id_prefix: the prefix for Content-ID header
        :rtype str:
        :rtype dict:
        """

        assert len(items) <= self.BATCH_THRESHOLD, 'Too many items for one batch requests!'

        # Build payload and a map between individual request id and partial payload
        req_map = {}
        method = 'DELETE'
        payload = ''
        req_id = 1

        for item in items:
            payload_part = ''
            object_name = item.get('name', None)
            if object_name:
                delete_url = self.build_url(obj_name=object_name)
                content_id = '{}+{}'.format(id_prefix, req_id)
                payload_part += '--{}\n' \
                                'Content-Type: application/http\n' \
                                'Content-Transfer-Encoding: binary\n' \
                                'Content-ID: <{}>'.format(self.BATCH_BOUNDARY, content_id)
                payload_part += '\n\n{} {}\n\n'.format(method, delete_url)
                req_map.update({req_id: payload_part})
                payload += payload_part
                req_id += 1

        payload += '--{}--'.format(self.BATCH_BOUNDARY)

        return payload, req_map

    def _build_payload_for_batch_copy(
            self,
            items: list,
            id_prefix: str,
            src_prefix: str,
            dest_prefix: str,
            dest_bucket: str,
    ) -> typing.Tuple[str, dict]:
        """Build the payload for batch copy request.  Return the full payload in string and a
        map of child request id and child payload.

        :param items: the dictionary that contains all the objects to copy
        :param id_prefix: the shared prefix for the Content-ID header
        :param src_prefix: the prefix (object name) for the src folder
        :param dest_prefix: the prefix (object name) for the dest folder
        :param dest_bucket: the name of the bucket where the destination folder resides in
        :rtype str:
        :rtype dict:
        """

        assert len(items) <= self.BATCH_THRESHOLD, 'Too many items for one batch requests!'

        # Build payload and a map between individual request id and partial payload
        req_map = {}
        method = 'POST'
        payload = ''
        req_id = 1
        src_prefix_length = len(src_prefix)

        for item in items:

            payload_part = ''
            src_obj_name = item.get('name', None)

            if src_obj_name:
                assert src_obj_name.startswith(src_prefix), 'Invalid prefix for the source object!'

                dest_obj_name = dest_prefix + src_obj_name[src_prefix_length:]
                copy_url = self.build_url(
                    obj_name=src_obj_name,
                    obj_action=self.COPY_ACTION,
                    dest_bucket=dest_bucket,
                    dest_obj_name=dest_obj_name,
                )

                content_id = '{}+{}'.format(id_prefix, req_id)
                payload_part += '--{}\n' \
                                'Content-Type: application/http\n' \
                                'Content-Transfer-Encoding: binary\n' \
                                'Content-ID: <{}>'.format(self.BATCH_BOUNDARY, content_id)
                payload_part += '\n\n{} {}\n\n'.format(method, copy_url)
                req_map.update({req_id: payload_part})

                payload += payload_part
                req_id += 1

        payload += '--{}--'.format(self.BATCH_BOUNDARY)

        return payload, req_map
