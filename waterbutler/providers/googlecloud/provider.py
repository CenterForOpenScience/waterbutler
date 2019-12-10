import time
import base64
import typing
import hashlib
import logging
import functools
from http import HTTPStatus

from google.oauth2 import service_account

from waterbutler.core.path import WaterButlerPath
from waterbutler.core.provider import BaseProvider
from waterbutler.core.utils import make_disposition
from waterbutler.core.streams import BaseStream, HashStreamWriter, ResponseStreamReader
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

    ``GoogleCloudProvider`` uses the XML API and Signed Request associated with a service account.

    General API docs: https://cloud.google.com/storage/docs/apis

    XML API cocs: https://cloud.google.com/storage/docs/xml-api/overview

    Quirks:

    * The official name of the service is "Cloud Storage" provided by "Google Cloud Platform".
      However, it is named ``GoogleCloudProvider`` for better clarity and consistency in WB.
      "Google Cloud", "Cloud Storage", and "Google Cloud Storage" are used interchangeably. "GC",
      "GCS" are used as abbreviations in the commit messages.
    """

    # Provider Name
    NAME = 'googlecloud'

    # BASE URL for XML API
    BASE_URL = pd_settings.BASE_URL

    # EXPIRATION for Signed Request/URL for XML API
    SIGNATURE_EXPIRATION = pd_settings.SIGNATURE_EXPIRATION

    def __init__(self, auth: dict, credentials: dict, settings: dict, **kwargs) -> None:
        """Initialize a provider instance with the given parameters.

        :param dict auth: the auth dictionary
        :param dict credentials: the credentials dictionary
        :param dict settings: the settings dictionary
        """

        # Here is an example of the settings for the ``OSFStorageProvider`` in OSF.
        #
        #     WATERBUTLER_CREDENTIALS = {
        #         'storage': {
        #             'json_creds': 'change_me'
        #         }
        #     }
        #
        #     WATERBUTLER_SETTINGS = {
        #         'storage': {
        #             'provider': 'change_me',
        #             'bucket': 'change_me',
        #         },
        #     }
        #
        #     WATERBUTLER_RESOURCE = 'bucket'

        super().__init__(auth, credentials, settings, **kwargs)

        self.bucket = settings.get('bucket')
        if not self.bucket:
            raise InvalidProviderConfigError(
                self.NAME,
                message='Missing cloud storage bucket settings from OSF'
            )

        json_creds = credentials.get('json_creds')
        if not json_creds:
            raise InvalidProviderConfigError(
                self.NAME,
                message='Missing service account credentials from OSF'
            )
        try:
            self.creds = service_account.Credentials.from_service_account_info(json_creds)
        except ValueError as exc:
            raise InvalidProviderConfigError(
                self.NAME,
                message='Invalid or mal-formed service account credentials: {}'.format(str(exc))
            )

    async def validate_v1_path(self, path: str, **kwargs) -> WaterButlerPath:
        return await self.validate_path(path)

    async def validate_path(self, path: str, **kwargs) -> WaterButlerPath:
        return WaterButlerPath(path)

    async def metadata(self,  # type: ignore
                       path: WaterButlerPath,
                       **kwargs) \
                       -> typing.Union[GoogleCloudFileMetadata, typing.List[BaseGoogleCloudMetadata]]:
        r"""Get the metadata about the object with the given WaterButlerPath.

        .. note::

            This limited version only supports metadata for file objects.  There are no technical
            blockers. The only reason is that OSFStorage never performs any action on folders for
            this inner storage provider.  We prefer not to have dead or unreachable code.

            *TODO [Phase 2]: if needed, implement _metadata_folder()*

        :param path: the WaterButlerPath to the file or folder
        :type path: :class:`.WaterButlerPath`
        :param dict \*\*kwargs: additional kwargs are ignored
        :rtype: :class:`.GoogleCloudFileMetadata` (for file)
        :rtype: List<:class:`.BaseGoogleCloudMetadata`> (for folder)
        """

        if path.is_folder:
            raise MetadataError('This limited provider does not support folder metadata.')
        else:
            return await self._metadata_object(path, is_folder=False)  # type: ignore

    async def upload(self, stream: BaseStream, path: WaterButlerPath, *args,
                     **kwargs) -> typing.Tuple[GoogleCloudFileMetadata, bool]:
        """Upload a file stream to the given WaterButlerPath.

        API docs:

            PUT Object: https://cloud.google.com/storage/docs/xml-api/put-object

            Upload an Object: https://cloud.google.com/storage/docs/xml-api/put-object-upload

        The response has an empty body. It does not have the required header ``Last-Modified``.
        In addition, the ``Content-Type`` header is for the response itself, not for the file WB
        just uploaded. WB must make an extra metadata request after a successful upload.

        The "etag" header returned by the XML API is exactly the same as the hex-digest of the
        MD5 hash. WB uses this header to verify the upload checksum instead of parsing the hash
        headers.

        Similarly to Amazon S3, WB must set ``skip_auto_headers={'Content-Type'}`` when calling
        :meth:`.BaseProvider.make_request()` because ``Content-Type`` is part of the "String To
        Sign".  The signed request would fail and return ``HTTP 403 Forbidden`` with the error
        message ``SignatureDoesNotMatch`` if auto headers were not skipped.

        :param stream: the stream to post
        :type stream: :class:`.streams.BaseStream`
        :param path: the WaterButlerPath of the file to upload
        :type path: :class:`.WaterButlerPath`
        :param list args: additional args are ignored
        :param dict kwargs: additional kwargs are ignored
        :rtype: :class:`.GoogleCloudFileMetadata`
        :rtype: bool
        """

        created = not await self.exists(path)

        stream.add_writer('md5', HashStreamWriter(hashlib.md5))

        req_method = 'PUT'
        obj_name = utils.get_obj_name(path, is_folder=False)
        signed_url = functools.partial(self._build_and_sign_url, req_method, obj_name, **{})
        headers = {'Content-Length': str(stream.size)}

        resp = await self.make_request(
            req_method,
            signed_url,
            data=stream,
            skip_auto_headers={'Content-Type'},
            headers=headers,
            expects=(HTTPStatus.OK,),
            throws=UploadError
        )

        await resp.release()

        header_etag = resp.headers.get('etag', None)
        if not header_etag:
            raise UploadError('Missing response header "ETag" for upload.')

        if header_etag.strip('"') != stream.writers['md5'].hexdigest:
            raise UploadChecksumMismatchError()

        metadata = await self._metadata_object(path, is_folder=False)
        return metadata, created  # type: ignore

    async def download(self, path: WaterButlerPath, accept_url=False, range=None,  # type: ignore
                       **kwargs) -> typing.Union[str, ResponseStreamReader]:
        """Download the object with the given path.


        API Docs:

            GET Object: https://cloud.google.com/storage/docs/xml-api/get-object

            Download an Object: https://cloud.google.com/storage/docs/xml-api/get-object-download

        The behavior of download differs depending on the value of ``accept_url``.  If
        ``accept_url == False``, WB makes a standard signed request and returns a
        ``ResponseStreamReader``.  If ``accept_url == True``, WB builds and signs the ``GET``
        request with an extra query parameter ``response-content-disposition`` to trigger the
        download with the display name.  The signed URL is returned.

        :param path: the WaterButlerPath for the object to download
        :type path: :class:`.WaterButlerPath`
        :param bool accept_url: should return a direct time-limited download url from the provider
        :param tuple range: the Range HTTP request header
        :param dict kwargs: ``display_name`` - the display name of the file on OSF and for download
        :rtype: str or :class:`.streams.ResponseStreamReader`
        """

        if path.is_folder:
            raise DownloadError('Cannot download folders', code=HTTPStatus.BAD_REQUEST)

        req_method = 'GET'
        obj_name = utils.get_obj_name(path, is_folder=False)

        if accept_url:
            display_name = kwargs.get('display_name') or path.name
            query = {'response-content-disposition': make_disposition(display_name)}
            # There is no need to delay URL building and signing
            signed_url = self._build_and_sign_url(req_method, obj_name, **query)  # type: ignore
            return signed_url

        signed_url = functools.partial(self._build_and_sign_url, req_method, obj_name, **{})
        resp = await self.make_request(
            req_method,
            signed_url,
            range=range,
            expects=(HTTPStatus.OK, HTTPStatus.PARTIAL_CONTENT),
            throws=DownloadError
        )
        return ResponseStreamReader(resp)

    async def delete(self, path: WaterButlerPath, *args, **kwargs) -> None:  # type: ignore
        r"""Deletes the file object with the specified WaterButler path.

        .. note::

            This limited version only supports deletion for file objects.  The main reason is that
            ``OSFStorageProvider`` does not need it.  The secondary reason is that there is no
            documentation for the XML API on ``BATCH`` request support which is available for JSON
            API.

            *TODO [Phase 2]: If needed, investigate whether folders are eligible. If so, enable for
            folders.  Otherwise, iterate through all children and delete each of them.*

        :param path: the WaterButlerPath of the object to delete
        :type path: :class:`.WaterButlerPath`
        :param list \*args: additional args are ignored
        :param dict \*\*kwargs: additional kwargs are ignored
        :rtype: None
        """

        if path.is_folder:
            raise DeleteError('This limited provider does not support folder deletion.')
        else:
            return await self._delete_file(path)

    async def intra_copy(self, dest_provider: BaseProvider,  # type: ignore
                         source_path: WaterButlerPath,
                         dest_path: WaterButlerPath) -> \
                         typing.Tuple[typing.Union[GoogleCloudFileMetadata,
                                                   GoogleCloudFolderMetadata],
                                      bool]:
        """Copy file objects within the same Google Cloud Storage Provider.

        .. note::

            This limited version only supports intra-copy for file objects.  The main reason is that
            ``OSFStorageProvider`` does not need it.  The secondary reason is that there is no
            documentation for the XML API on ``BATCH`` request support which is available for JSON
            API.

            *TODO [Phase 2]: If needed, investigate whether folders are eligible.  If so, enable
            for folders.  Otherwise, it is taken care of by the parent provider.*

        :param dest_provider: the destination provider, must be the same as the source one
        :type dest_provider: :class:`.BaseProvider`
        :param source_path: the source WaterButlerPath for the object to copy from
        :type source_path: :class:`.WaterButlerPath`
        :param dest_path: the destination WaterButlerPath for the object to copy to
        :type dest_path: :class:`.WaterButlerPath`
        :rtype: :class:`.GoogleCloudFileMetadata` or :class:`.GoogleCloudFolderMetadata`
        :rtype: bool
        """

        if source_path.is_folder and dest_path.is_folder:
            raise CopyError('This limited provider does not support folder intra-copy.')
        if not source_path.is_folder and not dest_path.is_folder:
            return await self._intra_copy_file(dest_provider, source_path, dest_path)

        raise CopyError('Cannot copy between a file and a folder')

    def can_intra_copy(self, other: BaseProvider, path: WaterButlerPath=None) -> bool:
        """Google Cloud Storage XML API supports intra-copy for files.

        .. note::

            Intra-copy for folders requires ``BATCH`` request support which is available for the
            JSON API. However, there is no documentation on the XML API.  It may be eligible given
            the fact that Amazon S3 supports a similar action called ``BULK`` request. Phase 1
            ``OSFStorageProvider`` does not perform any folder actions using this inner provider.
        """

        return self == other and not getattr(path, 'is_folder', False)

    def can_intra_move(self, other: BaseProvider, path: WaterButlerPath=None) -> bool:
        """Google Cloud Storage XML API supports intra move for files.  It is simply a combination
        of intra-copy and delete. Please refer to :meth:`.GoogleCloudProvider.can_intra_copy()`
        for more information.
        """

        return self.can_intra_copy(other, path)

    def can_duplicate_names(self):
        """Google Cloud Storage allows a file and a folder to share the same name.
        """
        return True

    async def _exists_folder(self, path: WaterButlerPath) -> bool:
        """Check if a folder with the given WaterButlerPath exists. Calls
        :meth:`._metadata_object()`.

        .. note::

            For folders, :meth:`.BaseProvider.exists()` calls :meth:`.BaseProvider.metadata()`,
            which further calls :meth:`._metadata_folder()`.  This makes the simple action more
            complicated and more expensive if the folder exists.  For files,
            :meth:`.BaseProvider.exists()` does not have such limitation.

        :param path: the WaterButlerPath of the folder to check
        :type path: :class:`.WaterButlerPath`
        :rtype: bool
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

    async def _metadata_object(self, path: WaterButlerPath,
                               is_folder: bool=False) \
                               -> typing.Union[GoogleCloudFileMetadata, GoogleCloudFolderMetadata]:
        """Get the metadata about the object with the given WaterButlerPath.

        API docs:

            GET Object: https://cloud.google.com/storage/docs/xml-api/get-object

            HEAD Object: https://cloud.google.com/storage/docs/xml-api/head-object

        .. note::

            Use ``HEAD`` instead of ``GET`` to retrieve the metadata of an object.  Google points
            out that:  "You should not use a ``GET`` object request to retrieve only non-ACL
            metadata, because doing so incurs egress charges associated with downloading the entire
            object.  Instead use a ``HEAD`` object request to retrieve non-ACL metadata for the
            object."

        .. note::

            The flag ``is_folder`` is explicitly used.  Providing the wrong type will always fail.
            This is the case for many internal/private methods of and helper/utility functions for
            this class. They are not exposed to any outside usage, including the parent classes.

        :param path: the WaterButlerPath of the object
        :type path: :class:`.WaterButlerPath`
        :param bool is_folder: whether the object is a file or folder
        :rtype: :class:`.GoogleCloudFileMetadata`
        :rtype: :class:`.GoogleCloudFolderMetadata`
        """

        req_method = 'HEAD'
        obj_name = utils.get_obj_name(path, is_folder=is_folder)
        signed_url = functools.partial(self._build_and_sign_url, req_method, obj_name, **{})

        resp = await self.make_request(
            req_method,
            signed_url,
            expects=(HTTPStatus.OK,),
            throws=MetadataError
        )
        await resp.release()

        if is_folder:
            return GoogleCloudFolderMetadata.new_from_resp_headers(obj_name, resp.headers)
        else:
            return GoogleCloudFileMetadata.new_from_resp_headers(obj_name, resp.headers)

    async def _delete_file(self, path: WaterButlerPath) -> None:
        """Deletes the file with the specified WaterButlerPath.

        API docs: https://cloud.google.com/storage/docs/xml-api/delete-object

        If WB makes a ``DELETE`` request for an object that doesn't exist, it will receive the
        ``HTTP 404 Not Found`` status and the error message containing ``NoSuchKey``.

        :param path: the WaterButlerPath of the file to delete
        :type path: :class:`.WaterButlerPath`
        :rtype: None
        """

        req_method = 'DELETE'
        obj_name = utils.get_obj_name(path, is_folder=False)
        signed_url = functools.partial(self._build_and_sign_url, req_method, obj_name, **{})

        resp = await self.make_request(
            req_method,
            signed_url,
            expects=(HTTPStatus.NO_CONTENT,),
            throws=DeleteError,
        )

        await resp.release()

    async def _intra_copy_file(self, dest_provider: BaseProvider, source_path: WaterButlerPath,
                               dest_path: WaterButlerPath) -> typing.Tuple[GoogleCloudFileMetadata, bool]:  # noqa
        """Copy files within the same Google Cloud Storage provider, overwrite existing ones if
        there are any.  Return the metadata of the destination file and a flag indicating if the
        file was created (new) or overwritten (existing).

        API docs: https://cloud.google.com/storage/docs/xml-api/put-object-copy

        .. note::

            The XML response body contains ``CopyObjectResult``, ``ETag`` and ``LastModified`` of
            the new file.  The response header contains most of the metadata WB needs for the file.
            However, two pieces are missing/incorrect: ``Content-Type`` and ``Last-Modified``.  The
            metadata can be constructed from the response but current implementation chooses to make
            a metadata request.

            *TODO [Phase 2]: if needed, build the metadata from response headers and XML body*

        :param dest_provider: the destination provider, must be the same as the source one
        :type dest_provider: :class:`.BaseProvider`
        :param source_path: the source WaterButlerPath for the object to copy from
        :type source_path: :class:`.WaterButlerPath`
        :param dest_path: the destination WaterButlerPath for the object to copy to
        :type dest_path: :class:`.WaterButlerPath`
        :rtype: :class:`.GoogleCloudFileMetadata`
        :rtype: bool
        """

        created = not await dest_provider.exists(dest_path)

        req_method = 'PUT'
        headers = {'Content-Length': '0', 'Content-Type': ''}

        src_obj_name = utils.get_obj_name(source_path, is_folder=False)
        canonical_ext_headers = {'x-goog-copy-source': '{}/{}'.format(self.bucket, src_obj_name)}
        headers.update(canonical_ext_headers)

        dest_obj_name = utils.get_obj_name(dest_path, is_folder=False)
        signed_url = functools.partial(
            self._build_and_sign_url,
            req_method,
            dest_obj_name,
            canonical_ext_headers=canonical_ext_headers,
            **{}
        )

        resp = await self.make_request(
            req_method,
            signed_url,
            headers=headers,
            expects=(HTTPStatus.OK,),
            throws=CopyError
        )

        await resp.release()

        metadata = await self._metadata_object(dest_path, is_folder=False)

        return metadata, created  # type: ignore

    def _build_and_sign_url(self, http_method: str, obj_name: str, content_md5: str='',
                            content_type: str='', canonical_ext_headers: dict=None,
                            **queries) -> str:
        r"""Build and sign the request URL for various actions.

        **Building the URL**

        Most Cloud Storage XML API requests use the following URI for accessing buckets and
        objects.  Both forms are supported by Google and we select the second one::

            [ ] https://[BUCKET_NAME].[BASE_URL]/[OBJECT_NAME]
            [x] https://[BASE_URL]/[BUCKET_NAME]/[OBJECT_NAME]

        **Signing the URL**

        WB uses authentication via signed URL for Google Cloud Storage.  Google points out that
        "Signed URLs can only be used to access resources in Google Cloud Storage through the
        XML API."  This is the main reason that XML API is our choice for this limited provider.

        References:

            https://cloud.google.com/storage/docs/access-control/signed-urls
            https://cloud.google.com/storage/docs/access-control/create-signed-urls-program

        :param str http_method: the http method
        :param str obj_name: the object name of the object or src object
        :param str content_md5: the value of the Content-MD5 header
        :param str content_type: the value of the Content-Type header
        :param dict canonical_ext_headers: the canonical extension headers string
        :param dict \*\*queries: the dict for query parameters
        :rtype: str
        """

        segments = (self.bucket, )

        if obj_name:
            segments += (obj_name, )  # type: ignore

        expires = int(time.time()) + self.SIGNATURE_EXPIRATION
        canonical_resource = utils.build_url('', *segments, **{})  # type: ignore
        canonical_ext_headers_str = utils.build_canonical_ext_headers_str(canonical_ext_headers)
        canonical_part = canonical_ext_headers_str + canonical_resource

        string_to_sign = '\n'.join([
            http_method,
            content_md5,
            content_type,
            str(expires),
            canonical_part
        ])
        encoded_signature = base64.b64encode(self.creds.sign_bytes(string_to_sign))
        queries.update({
            'GoogleAccessId': self.creds.service_account_email,
            'Expires': str(expires),
            'Signature': encoded_signature
        })
        signed_url = utils.build_url(self.BASE_URL, *segments, **queries)

        return signed_url
