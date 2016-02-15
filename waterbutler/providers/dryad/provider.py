import cgi

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.dryad.metadata import DryadPackageMetadata, DryadFileMetadata
from waterbutler.providers.dryad.settings import DRYAD_META_URL, DYRAD_FILE_URL


class DryadProvider(provider.BaseProvider):
    """

        Dryad stores packages as:
        <API ACCESS POINT>/doi:10.5061/dryad.XXXX
        Files are stored as:
        <API ACCESS POINT>/doi:10.5061/dryad.XXXX/Y

        The WaterbutlerPath version of this is:
        Accesses Dryad repository info:
        /
        Accesses Dryad Package info:
        /XXXX
        Acesses File info:
        /XXXX/YY

        Therefore, the waterbutler path is /package/file_number
    """

    NAME = 'dryad'

    def __init__(self, auth, credentials, dryad_settings):
        super().__init__(auth, credentials, dryad_settings)
        self.doi = dryad_settings['doi']

    async def _package_metadata(self):
        """ Interface to file and package metadata from Dryad

        :param path: Path mapping to waterbutler interpretation of Dryad package
        :type path: WaterbutlerPath.
        :returns:  list -- A list of metadata
        :raises: urllib.error.HTTPError

        """
        resp = await self.make_request(
            'GET',
            DRYAD_META_URL + self.doi.split('.')[-1],
            expects=(200, 206),
            throws=exceptions.MetadataError)
        body_text = await resp.text()
        return DryadPackageMetadata(body_text, self.doi)

    async def _file_metadata(self, path):
        """ Interface to file and package metadata from Dryad

        :param path: Path mapping to waterbutler interpretation of Dryad file
        :type path: WaterbutlerPath.
        :returns:  list -- A list of metadata
        :raises: urllib.error.HTTPError

        """

        metadata_resp = await self.make_request(
            'GET',
            DRYAD_META_URL + path.strip('/'),
            expects=(200, 206),
            throws=exceptions.MetadataError)

        file_metadata_resp = await self.make_request(
            'GET',
            DYRAD_FILE_URL + path.strip('/') + "/bitstream",
            expects=(200, 206),
            throws=exceptions.MetadataError)

        file_stream = await self.make_request(
            'GET',
            DRYAD_META_URL + path.strip('/') + '/bitstream',
            expects=(200, 206),
            throws=exceptions.DownloadError,
        )

        _, params = cgi.parse_header(file_stream.headers.get('Content-Disposition', ''))
        file_name = params['filename']
        metadata_text = await metadata_resp.text()
        file_metadata = await file_metadata_resp.text()

        return DryadFileMetadata(metadata_text, path.strip('/'), file_metadata, file_name)

    async def metadata(self, path, **kwargs):
        """ Interface to file and package metadata from Dryad

        :param path: Path mapping to waterbutler interpretation of Dryad file
        :type path: WaterbutlerPath.
        :returns:  list -- A list of metadata
        :raises: urllib.error.HTTPError

        """
        package = await self._package_metadata()

        if str(path) == u'/':
            return [package]

        if not path.is_dir:
            return (await self._file_metadata(path.path))

        children = []
        for child in package.file_parts:
            # convert from the identifier format listed in the metadata to a path
            child_doi = child
            child_doi = child_doi.split(".")[-1]
            children.append((await self._file_metadata(child_doi)))
        return children

    async def download(self, path, **kwargs):
        """ Interface to downloading files from Dryad

        :param path: Path mapping to waterbutler interpretation of Dryad file
        :type path: WaterbutlerPath.

        :returns:  streams.ResponseStreamReader -- the return code.
        :raises:   exceptions.DownloadError

        """

        resp = await self.make_request(
            'GET',
            DRYAD_META_URL + path.path + '/bitstream',
            expects=(200, 206),
            throws=exceptions.DownloadError,
        )

        file_metadata = await self._file_metadata(path.path)

        return streams.ResponseStreamReader(resp,
            size=file_metadata.size,
            name=file_metadata.name)

    async def validate_path(self, path, **kwargs):
        """
        :param path path to either a package or file. Format should be:
        / or /XXXX/ or /XXXX/YY for package XXXX as specified in doi or
        file YY or / for the reference to Dryad itself.
        """
        if path is None:
            return WaterButlerPath('/')
        depth = path.count('/')
        if depth == 1:
            if path == '/':
                return WaterButlerPath(path)
            else:
                raise exceptions.NotFoundError(path)
        if depth == 2:
            internal_doi = path.split('/')[1]
            file_doi = path.split('/')[2]
            if path.endswith('/') and len(internal_doi) == 4:
                return WaterButlerPath(path)
            elif len(internal_doi) == 4 and len(file_doi) > 0:
                return WaterButlerPath(path)
            else:
                raise exceptions.NotFoundError(path)
        else:
            raise exceptions.NotFoundError(path)

    async def validate_v1_path(self, path, **kwargs):
        """
        :param path path to either a package or file. Format should be:
        / or /XXXX/ or /XXXX/YY for package XXXX as specified in doi or
        file YY or / for the reference to Dryad itself.
        """
        if path is None:
            return WaterButlerPath('/')
        depth = path.count('/')
        if depth == 1:
            if path == '/':
                return WaterButlerPath(path)
            else:
                raise exceptions.NotFoundError(path)
        if depth == 2:
            internal_doi = path.split('/')[1]
            file_doi = path.split('/')[2]
            full_url = DRYAD_META_URL + internal_doi
            if len(file_doi) > 0:
                full_url += '/' + file_doi
            resp = await self.make_request(
                'GET',
                full_url,
                expects=(200, 404),
                throws=exceptions.MetadataError,
            )

            if resp.status == 404:
                raise exceptions.NotFoundError(str(path))

            if path.endswith('/') and len(internal_doi) == 4:
                return WaterButlerPath(path)
            elif len(internal_doi) == 4 and len(file_doi) > 0:
                return WaterButlerPath(path)
            else:
                raise exceptions.NotFoundError(path)
        else:
            raise exceptions.NotFoundError(path)

    def can_intra_move(self, other, path=None):
        raise exceptions.ReadOnlyProviderError(self)

    def can_intra_copy(self, other, path=None):
        return True

    async def intra_copy(self, dest_provider, src_path, dest_path):
        return await self._do_intra_move_or_copy(dest_provider, src_path, dest_path)

    async def _do_intra_move_or_copy(self, dest_provider, src_path, dest_path):
        resp = await self.make_request(
            'GET',
            DRYAD_META_URL + src_path.path.strip('/') + '/bitstream',
            expects=(200, 206),
            throws=exceptions.DownloadError,
        )
        file_metadata = await self._file_metadata(src_path.path)

        download_stream = streams.ResponseStreamReader(resp, size=file_metadata.size, name=file_metadata.name)
        dest_path.rename(file_metadata.name)
        return await dest_provider.upload(download_stream, dest_path)

    async def upload(self, stream, **kwargs):
        raise exceptions.ReadOnlyProviderError(self)

    async def delete(self, **kwargs):
        raise exceptions.ReadOnlyProviderError(self)

    def can_duplicate_names(self):
        return False
