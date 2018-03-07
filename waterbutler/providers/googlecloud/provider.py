import time
import base64
import typing
import hashlib
import logging
from http import HTTPStatus

from oauth2client.service_account import ServiceAccountCredentials

from waterbutler.core.path import WaterButlerPath
from waterbutler.core.provider import BaseProvider
from waterbutler.core.streams import BaseStream, HashStreamWriter, ResponseStreamReader
from waterbutler.core.metadata import BaseFileMetadata, BaseFolderMetadata
from waterbutler.core.exceptions import (WaterButlerError, MetadataError, NotFoundError,
                                         CopyError, UploadError, DownloadError, DeleteError,
                                         UploadChecksumMismatchError, InvalidProviderConfigError, )

from waterbutler.providers.googlecloud import utils
from waterbutler.providers.googlecloud import settings as pd_settings
from waterbutler.providers.googlecloud.metadata import (BaseGoogleCloudMetadata,
                                                        GoogleCloudFileMetadata,
                                                        GoogleCloudFolderMetadata, )

logger = logging.getLogger(__name__)


class GoogleCloudProvider(BaseProvider):
    """Provider for Google's Cloud Storage Service.

    GoogleCloudProvider uses signed XML API request with a service account.

        General API Docs:
            https://cloud.google.com/storage/docs/apis
        XML API Docs:
            https://cloud.google.com/storage/docs/xml-api/overview

    The official name of the service is "Cloud Storage" by "Google Cloud Platform". However, it is
    named ``GoogleCloudProvider`` for better clarity and consistency in WB.  "Google Cloud", "Cloud
    Storage" and "Google Cloud Storage" are used interchangeably when referring to this service.
    """

    NAME = 'googlecloud'

    # BASE URL for JSON API
    BASE_URL = pd_settings.BASE_URL

    # EXPIRATION for Signed Request/URL for XML API
    SIGNATURE_EXPIRATION = pd_settings.SIGNATURE_EXPIRATION

    def __init__(self, auth: dict, credentials: dict, settings: dict):
        """Initialize a provider instance with the given params.

        Here is an example of the settings for the "osfstorage" addon in OSF.

            WATERBUTLER_CREDENTIALS = {
                'storage': {
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
        """

        super().__init__(auth, credentials, settings)

        self.bucket = settings.get('bucket')
        if not self.bucket:
            raise InvalidProviderConfigError(self.NAME, message='Missing bucket settings')

        # TODO [Phase 1]: replaces self.creds with self.json_creds after OSF/DevOps update
        # self.json_creds = credentials.get('json_creds')
        self.creds = ServiceAccountCredentials.from_json_keyfile_name(pd_settings.CREDS_PATH)
        if not self.creds:
            raise InvalidProviderConfigError(
                self.NAME,
                message='Missing service account credentials'
            )

        self.region = settings.get('region')

    @property
    def default_headers(self) -> dict:

        return {}

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

        TODO [Phase 1.5]: implement ``_metadata_folder``

        :param path: the WaterButler path to the file or folder
        :param kwargs: additional kwargs are ignored
        :rtype BaseGoogleCloudMetadata: for file
        :rtype List<BaseGoogleCloudMetadata>: for folder
        """

        if path.is_folder:
            raise MetadataError('This limited provider does not support folder metadata.')
        else:
            return await self._metadata_object(path, is_folder=False)

    async def upload(
            self,
            stream: BaseStream,
            path: WaterButlerPath,
            *args,
            **kwargs
    ) -> typing.Tuple[BaseGoogleCloudMetadata, bool]:
        """Upload a stream with the given WaterButler path.

        PUT Object:
            https://cloud.google.com/storage/docs/xml-api/put-object
        Upload an Object
            https://cloud.google.com/storage/docs/xml-api/put-object-upload

        Quirks: The response body is empty and the response header does not have "last-modified" and
                have a "content-type" for the response itself ("text/html; charset=UTF-8"), not for
                the file WB just uploaded.  Need to make a metadata request after successful upload.

        Quirks: The "etag" header XML API returns is the hex digest of the MD5.  Use this header
                to verify the upload checksum instead of parsing and converting the "x-goog-hash".

        :param stream: the stream to post
        :param path: the WaterButler path of the file to upload
        :param args: additional args are ignored
        :param kwargs: additional kwargs are ignored
        :rtype BaseGoogleCloudMetadata:
        :rtype bool:
        """

        created = not await self.exists(path)

        stream.add_writer('md5', HashStreamWriter(hashlib.md5))

        req_method = 'PUT'
        obj_name = utils.get_obj_name(path, is_folder=False)
        signed_url = self.build_and_sign_url(req_method, obj_name, **{})
        logger.info('signed_upload_url = "{}"'.format(signed_url))
        headers = {'Content-Length': str(stream.size)}

        resp = await self.make_request(
            req_method,
            signed_url,
            data=stream,
            # TODO [Phase 1]: s3 uses this, how about google cloud?
            # skip_auto_headers={'Content-Type'},
            headers=headers,
            expects=(HTTPStatus.OK,),
            throws=UploadError
        )

        await resp.release()

        if resp.headers.get('etag', None) != stream.writers['md5'].hexdigest:
            raise UploadChecksumMismatchError()

        metadata = await self._metadata_object(path, is_folder=False)
        return metadata, created

    async def download(
            self,
            path: WaterButlerPath,
            accept_url=False,
            range=None,
            **kwargs
    ) -> typing.Union[str, ResponseStreamReader]:
        """Download the object with the given path.  The behavior of download differs depending on
        the value of ``accept_url``.

        GET Object:
            https://cloud.google.com/storage/docs/xml-api/get-object
        Download an Object
            https://cloud.google.com/storage/docs/xml-api/get-object-download

        1. ``accept_url == False``

            Make a standard signed request and return a ``ResponseStreamReader``


        2. ``accept_url == True``


            Build and sign the GET request with an extra "content-disposition" query to trigger the
            download. Return the signed URL.

            Content Disposition: "A query string parameter that allows content-disposition to be
            overridden for authenticated GET requests."
                https://cloud.google.com/storage/docs/xml-api/reference-headers
                #responsecontentdisposition.

        :param path: the WaterButler path to the object to download
        :param accept_url: the flag to solicit a direct time-limited download url from the provider
        :param range: the Range HTTP request header
        :param kwargs:
        :rtype str:
        :rtype ResponseStreamReader:
        """

        if path.is_folder:
            raise DownloadError('Cannot download folders', code=HTTPStatus.BAD_REQUEST)

        req_method = 'GET'
        obj_name = utils.get_obj_name(path, is_folder=False)

        if accept_url:
            display_name = kwargs.get('displayName', path.name)
            query = {'response-content-disposition': 'attachment; filename={}'.format(display_name)}
            signed_url = self.build_and_sign_url(req_method, obj_name, **query)
            logger.info('signed_download_url = "{}"'.format(signed_url))
            return signed_url

        signed_url = self.build_and_sign_url(req_method, obj_name, **{})
        logger.info('signed_download_url = "{}"'.format(signed_url))
        resp = await self.make_request(
            req_method,
            signed_url,
            range=range,
            expects=(HTTPStatus.OK, HTTPStatus.PARTIAL_CONTENT),
            throws=DownloadError
        )
        return ResponseStreamReader(resp)

    async def delete(self, path: WaterButlerPath, *args, **kwargs) -> None:
        """Deletes the file object with the specified WaterButler path.

        Similarly to intra-copy folders, delete folders requires "BATCH" request support which is
        available for JSON API. There is no documentation on the XML API.

        TODO [Phase 2]: investigate whether folders are eligible
        TODO [Phase 2]: if so, enable for folders
        TODO [Phase 2]: if not, iterate through all children and make a delete request for each

        :param path: the WaterButler path of the object to delete
        :param args: additional args are ignored
        :param kwargs: additional kwargs are ignored
        :rtype None:
        """

        if path.is_folder:
            raise DeleteError('This limited provider does not support folder deletion.')
        else:
            return await self._delete_file(path)

    async def intra_copy(
            self,
            dest_provider: BaseProvider,
            source_path: WaterButlerPath,
            dest_path: WaterButlerPath
    ) -> typing.Tuple[typing.Union[BaseFileMetadata, BaseFolderMetadata], bool]:
        """Copy file objects within the same Google Cloud Storage Provider.

        Similarly to delete folders, intra-copy folders requires "BATCH" request support which is
        available for JSON API.  There is no documentation on the XML API.

        TODO [Phase 2]: investigate whether folders are eligible
        TODO [Phase 2]: if so, enable for folders
        TODO [Phase 2]: if not, the action has been taken care of by the parent provider

        :param dest_provider: the destination provider, must be the same as the source one
        :param source_path: the source WaterButler path for the object to copy from
        :param dest_path: the destination WaterButler path for the object to copy to
        :rtype BaseFileMetadata:
        :rtype BaseFolderMetadata:
        :rtype bool:
        """

        if source_path.is_folder and dest_path.is_folder:
            raise CopyError('This limited provider does not support folder intra-copy.')
        if not source_path.is_folder and not dest_path.is_folder:
            return await self._intra_copy_file(dest_provider, source_path, dest_path)

        raise CopyError('Cannot copy between a file and a folder')

    def can_intra_copy(self, other: BaseProvider, path: WaterButlerPath = None) -> bool:
        """Google Cloud Storage XML API supports intra-copy for files.  Intra-copy for folders
        requires "BATCH" request support which is available for JSON API. However, there is no
        documentation on the XML API.  It may be eligible given the fact that Amazon S3 supports
        a similar action called "BULK" request.  Need more investigation in Phase 2.  Phase 1
        OSFStorage does not perform any folder level actions (e.g. delete, copy ,move, etc.)
        using the inner provider.

        TODO [Phase 2]: investigate whether folders are eligible; if so, enable for folders
        """
        return self == other and not getattr(path, 'is_folder', False)

    def can_intra_move(self, other: BaseProvider, path: WaterButlerPath = None) -> bool:
        """Google Cloud Storage XML API supports intra move for files.  It is a combination of
        intra-copy and delete. Please refer to ``can_intra_copy()`` for more information.
        """

        return self.can_intra_copy(other, path)

    def can_duplicate_names(self):
        """Google Cloud Storage allows a file and a folder to share the same name.
        """
        return True

    def build_and_sign_url(
            self,
            http_method: str,
            obj_name: str,
            content_md5: str='',
            content_type: str='',
            canonical_ext_headers: dict=None,
            **queries
    ) -> str:
        """Build and sign the request URL for various actions.

        Building the URL:

            Most Cloud Storage XML API requests use the following URI for accessing buckets and
            objects.  Both forms are supported by Google and we select the second one since we use
            signed request.

                [ ] https://[BUCKET_NAME].[BASE_URL]/[OBJECT_NAME]
                [x] https://[BASE_URL]/[BUCKET_NAME]/[OBJECT_NAME]


        Signing the URL:

            WB uses authentication by signed URL for Google Cloud Storage.  XML API is the only
            choice that supports such authentication.

            Google Cloud Storage: Signed URLs
                https://cloud.google.com/storage/docs/access-control/signed-urls
                https://cloud.google.com/storage/docs/access-control/create-signed-urls-program

            "Signed URLs can only be used to access resources in Google Cloud Storage through the
            XML API."

        :param obj_name: the object name of the object or src object
        :param http_method: the http method
        :param content_md5: the value of the Content-MD5 header
        :param content_type: the value of the Content-Type header
        :param canonical_ext_headers: the canonical extension headers string
        :param queries: the dict for query parameters
        :rtype str:
        """

        segments = (self.bucket, )

        if obj_name:
            segments = segments + (obj_name,)

        expires = int(time.time()) + self.SIGNATURE_EXPIRATION
        canonical_resource = utils.build_url('', *segments, **{})

        canonical_ext_headers_str = utils.build_canonical_ext_headers_str(canonical_ext_headers)
        canonical_part = canonical_ext_headers_str + canonical_resource
        string_to_sign = '\n'.join([
            http_method,
            content_md5,
            content_type,
            str(expires),
            canonical_part
        ])
        encoded_signature = base64.b64encode(self.creds.sign_blob(string_to_sign)[1])
        queries.update({
            'GoogleAccessId': self.creds.service_account_email,
            'Expires': str(expires),
            'Signature': encoded_signature
        })
        signed_url = utils.build_url(self.BASE_URL, *segments, **queries)
        return signed_url

    async def _exists_folder(self, path: WaterButlerPath) -> bool:
        """Check if a folder with the given WaterButler path exists. Calls ``_metadata_object()``.

        For folders, ``exists()`` from the core provider calls ``metadata()``, which further calls
        ``_metadata_folder``.  This makes simple action more complicated and more expensive if the
        folder exists.

        For files, ``exists()`` does not have such limitation.

        :param path: the WaterButler path of the folder to check
        :rtype bool:
        """

        if not path.is_folder:
            raise WaterButlerError('Wrong type, expecting folder type but received a file')

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
    ) -> typing.Union[GoogleCloudFileMetadata, GoogleCloudFolderMetadata]:
        """Get the metadata about the object itself with the given WaterButler path.

        GET Object
            API Docs: https://cloud.google.com/storage/docs/xml-api/get-object
        HEAD Object
            API Docs: https://cloud.google.com/storage/docs/xml-api/head-object

        Use HEAD instead of GET to retrieve the metadata of an object.  Google points out that:

            "You should not use a GET object request to retrieve only non-ACL metadata, because
            doing so incurs egress charges associated with downloading the entire object. Instead
            use a HEAD object request to retrieve non-ACL metadata for the object."

        The flag ``is_folder`` is explicitly used. Providing the wrong type will always fail. This
        is the case for many internal/private/helper/utility methods of/for this class. They are not
        exposed to any outside usage, including the parent classes.

        :param path: the WaterButler path of the object
        :param is_folder: whether the object is a file or folder
        :rtype GoogleCloudFileMetadata
        :rtype GoogleCloudFolderMetadata
        """

        req_method = 'HEAD'
        obj_name = utils.get_obj_name(path, is_folder=is_folder)
        signed_url = self.build_and_sign_url(req_method, obj_name, **{})
        logger.info('signed_metadata_url = "{}"'.format(signed_url))

        resp = await self.make_request(
            req_method,
            signed_url,
            expects=(HTTPStatus.OK,),
            throws=MetadataError
        )
        await resp.release()
        parsed_resp_headers = BaseGoogleCloudMetadata.get_metadata_from_resp_headers(
            obj_name,
            resp.headers
        )

        if is_folder:
            return GoogleCloudFolderMetadata(parsed_resp_headers)
        else:
            return GoogleCloudFileMetadata(parsed_resp_headers)

    async def _delete_file(self, path: WaterButlerPath) -> None:
        """Deletes the file with the specified WaterButler path.

        Delete Object:
            https://cloud.google.com/storage/docs/xml-api/delete-object

        "If you make a DELETE request for an object that doesn't exist, you will get a 404 Not Found
        status code and the body of the error response will contain NoSuchKey in the Code element."

        :param path: the WaterButler path of the file to delete
        :rtype None:
        """

        req_method = 'HEAD'
        obj_name = utils.get_obj_name(path, is_folder=False)
        signed_url = self.build_and_sign_url(req_method, obj_name, **{})
        logger.info('signed_delete_url = "{}"'.format(signed_url))

        resp = await self.make_request(
            req_method,
            signed_url,
            expects=(HTTPStatus.NO_CONTENT,),
            throws=DeleteError,
        )

        await resp.release()

    async def _intra_copy_file(
            self,
            dest_provider: BaseProvider,
            source_path: WaterButlerPath,
            dest_path: WaterButlerPath
    ) -> typing.Tuple[GoogleCloudFileMetadata, bool]:
        """Copy files within the same Google Cloud Storage provider, overwrite existing ones if
        there are any.  Return the metadata of the destination file, created or overwritten.

        Copy an Object
            https://cloud.google.com/storage/docs/xml-api/put-object-copy

        The response body contains "CopyObjectResult", "ETag" and "LastModified" of the new file.
        The response header contains most of the metadata WB needed for the new file. However, two
        pieces are missing: "content-type" and "last-modified".  The metadata can be constructed
        from the response but current implementation chooses to make a metadata request.

        :param dest_provider: the destination provider, must be the same as the source one
        :param source_path: the source WaterButler path for the object to copy from
        :param dest_path: the destination WaterButler path for the object to copy to
        :rtype GoogleCloudFileMetadata:
        :rtype bool:
        """

        created = not await dest_provider.exists(dest_path)

        req_method = 'HEAD'
        src_obj_name = utils.get_obj_name(source_path, is_folder=False)
        dest_obj_name = utils.get_obj_name(dest_path, is_folder=False)
        canonical_ext_headers = {'x-goog-copy-source': src_obj_name}
        signed_url = self.build_and_sign_url(
            req_method,
            dest_obj_name,
            canonical_ext_headers=canonical_ext_headers,
            **{}
        )
        logger.info('signed_intra_copy_url = "{}"'.format(signed_url))

        headers = {
            'Content-Length': '0',
            'Content-Type': ''
        }.update(canonical_ext_headers)

        resp = await self.make_request(
            req_method,
            signed_url,
            headers=headers,
            expects=(HTTPStatus.OK,),
            throws=CopyError
        )

        await resp.release()

        # TODO [Phase 1.5]: if possible, obtain the metadata from response headers and body
        metadata = await self._metadata_object(dest_path, is_folder=False)

        return metadata, created
