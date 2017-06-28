import cgi

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions

from .path import DryadPath
from .settings import DRYAD_META_URL, DRYAD_FILE_URL
from .metadata import DryadPackageMetadata, DryadFileMetadata


class DryadProvider(provider.BaseProvider):
    """
        Read_only provider for Dryad packages.
        Within the Dryad API, packages are accessed via the following format:
            `<API ACCESS POINT>/doi:10.5061/dryad.XXXX`
        Where `XXXX` denotes the packages-specific DOI suffix.
        Similarly, files are accessed with the following format:
            `<API ACCESS POINT>/doi:10.5061/dryad.XXXX/Y`
        Where `Y` is an index number assigned to each file.

        In Waterbutler, this is translated to the following table:

        ========== ================================
        WB Path    Object
        ========== ================================
        `/`        Dryad repository (unimplemented)
        `/XXXX/`   Dryad Package
        `/XXXX/YY` Package File
        ========== ================================

        Paths need to follow this scheme or will :class:`waterbutler.core.exceptions.NotFoundError`
        when validated through :func:`waterbutler.providers.dryad.provider.DryadProvider.validate_path`
    """

    NAME = 'dryad'

    def __init__(self, auth, credentials, dryad_settings):
        super().__init__(auth, credentials, dryad_settings)
        self.doi = dryad_settings['doi']

    async def validate_v1_path(self, path, **kwargs):
        """See :func:`waterbutler.providers.dryad.provider.DryadProvider.validate_path`.
            Additionally queries the Dryad API to check if the package exists.
        """
        wbpath = await self.validate_path(path, **kwargs)
        if wbpath.is_root:
            return wbpath
        full_url = DRYAD_META_URL + wbpath.package_doi
        if wbpath.is_file:
            full_url += '/' + wbpath.file_id

        resp = await self.make_request(
            'GET',
            full_url,
            expects=(200, 404),
            throws=exceptions.MetadataError,
        )
        await resp.release()
        if resp.status == 404:
            raise exceptions.NotFoundError(str(path))

        return wbpath

    async def validate_path(self, path, **kwargs):
        """Returns WaterButlerPath if the string `path` is valid, else raises
        not found error. See :class:`waterbutler.providers.dryad.provider.DryadProvider`
        for details on path formatting.

        :param path: Path to either a package or file.
        :type path: `str`
        """
        wbpath = DryadPath(path)
        if wbpath.is_root:
            return wbpath
        if len(wbpath.parts) == 2 and not wbpath.is_dir:
                raise exceptions.NotFoundError(path)
        elif len(wbpath.parts) == 3 and not wbpath.is_file:
            raise exceptions.NotFoundError(path)
        return wbpath

    async def revalidate_path(self, base, path, folder=False):
        """Take a path and a base path and build a WaterButlerPath representing `/base/path`.  For
        id-based providers, this will need to lookup the id of the new child object.

        :param WaterButlerPath base: The base folder to look under
        :param str path: the path of a child of `base`, relative to `base`
        :param bool folder: whether the returned WaterButlerPath should represent a folder
        :rtype: WaterButlerPath
        """
        return base.child(path, folder=folder)

    async def metadata(self, path, **kwargs):
        """ Interface to file and package metadata from Dryad

        :param path: Path mapping to waterbutler interpretation of Dryad file
        :type path: `waterbutler.core.path.WaterButlerPath`
        :returns:  `list` A list of metadata
        :raises: `urllib.error.HTTPError`
        """
        package = await self._package_metadata()

        if path.is_root:
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

    def revisions(self, **kwargs):
        """Currently, there are no revisions in dryad
            :raises: `waterbutler.core.exceptions.UnsupportedHTTPMethodError` always
        """
        raise exceptions.UnsupportedHTTPMethodError()

    async def download(self, path, **kwargs):
        """ Interface to downloading files from Dryad

        :param path: Path mapping to waterbutler interpretation of Dryad file
        :type path: `waterbutler.core.path.WaterButlerPath`
        :returns:  `waterbutler.core.streams.ResponseStreamReader` Download stream generator
        :raises:   `waterbutler.core.exceptions.DownloadError`
        """

        resp = await self.make_request(
            'GET',
            DRYAD_META_URL + path.path + '/bitstream',
            expects=(200, 206),
            throws=exceptions.DownloadError,
        )

        file_metadata = await self._file_metadata(path.path)
        # This is a kludge in place to fix a bug in the Dryad API.
        ret = streams.ResponseStreamReader(resp,
            size=file_metadata.size,
            name=file_metadata.name)
        ret._size = file_metadata.size
        return ret

    def can_duplicate_names(self):
        """Dryad write access is not allowed.

        :raises: `waterbutler.core.exceptions.ReadOnlyProviderError` Always
        """
        return False

    def can_intra_move(self, other, path=None):
        """Moves are not allowed. Only Copies from Dryad to another provider.

            :raises: `waterbutler.core.exceptions.ReadOnlyProviderError` Always
        """
        raise exceptions.ReadOnlyProviderError(self)

    def can_intra_copy(self, other, path=None):
        """All files in Dryad are able to be copied out (if accessible).

            :returns: `False` Always
        """
        return False

    async def upload(self, stream, **kwargs):
        """Uploads are not allowed.

        :raises:`waterbutler.core.exceptions.ReadOnlyProviderError` Always
        """
        raise exceptions.ReadOnlyProviderError(self)

    async def delete(self, **kwargs):
        """Deletions are not allowed.

        :raises: `exceptions.ReadOnlyProviderError`: Always
        """
        raise exceptions.ReadOnlyProviderError(self)

    async def _package_metadata(self):
        """ Interface to file and package metadata from Dryad

        :param path: Path mapping to waterbutler interpretation of Dryad package
        :type path: `waterbutler.core.path.WaterButlerPath`
        :returns:  `list` -- A list of metadata
        :raises: `urllib.error.HTTPError`

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
        :type path: `waterbutler.core.path.WaterButlerPath`
        :returns:  `list` A list of metadata
        :raises: `urllib.error.HTTPError`

        """

        metadata_resp = await self.make_request(
            'GET',
            DRYAD_META_URL + path.strip('/'),
            expects=(200, 206),
            throws=exceptions.MetadataError)

        file_metadata_resp = await self.make_request(
            'GET',
            DRYAD_FILE_URL + path.strip('/') + "/bitstream",
            expects=(200, 206),
            throws=exceptions.MetadataError)

        file_stream = await self.make_request(
            'GET',
            DRYAD_META_URL + path.strip('/') + '/bitstream',
            expects=(200, 206),
            throws=exceptions.DownloadError,
        )
        content = file_stream.headers.get('CONTENT-DISPOSITION', '')
        _, params = cgi.parse_header(content)
        file_stream.close()
        file_name = params['filename']
        metadata_text = await metadata_resp.text()
        file_metadata = await file_metadata_resp.text()

        return DryadFileMetadata(metadata_text, path.strip('/'), file_metadata, file_name)
