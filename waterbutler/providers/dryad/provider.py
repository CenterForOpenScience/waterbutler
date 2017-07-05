import cgi
import xml.dom.minidom

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions

from .path import DryadPath
from .settings import DRYAD_META_URL, DRYAD_FILE_URL, DRYAD_DOI_BASE
from .metadata import (DryadPackageMetadata,
                       DryadFileMetadata,
                       DryadFileRevisionMetadata)


class DryadProvider(provider.BaseProvider):
    """Read-only provider for Dryad packages.

    **Paths**

    Within the Dryad API packages are accessed via the following format::

        <API ACCESS POINT>/doi:10.5061/dryad.XXXX

    Where ``XXXX`` denotes the package-specific DOI suffix, henceforth called the "package ID".

    Similarly, files are accessed with the following format::

        <API ACCESS POINT>/doi:10.5061/dryad.XXXX/Y

    Where ``Y`` is an index number assigned to each file, henceforth called the "file ID".

    In WaterButler, this is translated to the following table::

        ========  =================================
        WB Path   Object
        ========  =================================
        /         List of configured Dryad packages
        /XXXX/    Dryad package
        /XXXX/Y   Package file
        ========  =================================

    The auth provider returns a settings object that contains a ``doi`` key.  This DOI will
    determine what package metadata is returned when querying the root. The Dryad package ID given
    in the path is redundant with the package specified by the auth provider. Asking for the root
    path metadata will **always** return a one-element list containing the metadata of the
    configured package.  If the package ID does not match the settings DOI, the provider will
    return a 404.

    Paths need to follow this scheme or will raise a
    :class:`waterbutler.core.exceptions.NotFoundError` when validated through
    :func:`waterbutler.providers.dryad.provider.DryadProvider.validate_path`

    **Settings**

    * ``doi``: The DOI of the package currently attached to the auth provider.  Unlike the url
      path, this will be the full DOI, e.g. ``10.5061/dryad.XXXX``.

    **Quirks**

    * Dryad does not (as of July 2017) support authentication, so this provider does not do any
      auth checking beyond asserting that the package DOI in the path matches the DOI given in
      the settings from the auth provider.

    * The redundancy of the configured base DOI and the package ID in the path is deliberate. It
      was chosen to maintain structural parity with a possible future where the provider may be
      updated to support a list of connected packages or a list of user-owned packages.

    **API Documentation**

    * Dryad's RESTful API: http://wiki.datadryad.org/DataONE_RESTful_API

    * DOI usage: http://wiki.datadryad.org/DOI_Usage

    """

    NAME = 'dryad'

    def __init__(self, auth, credentials, dryad_settings):
        super().__init__(auth, credentials, dryad_settings)
        self.doi = dryad_settings['doi']

    async def validate_v1_path(self, path, **kwargs):
        """Verify that the requested file or folder exists and that it conforms to the v1 path
        semantics. Unusually, the v1 path semantics checking is a side effect of the code in
        :func:`waterbutler.providers.dryad.provider.DryadProvider.validate_path`.

        Additionally queries the Dryad API to check if the package exists.

        :param str path: string path to either a package or file
        :rtype: :class:`waterbutler.providers.dryad.path.DryadPath`
        :return: a DryadPath object representing the requested entity
        """

        if path == '/':
            return DryadPath('/', _ids=[self.doi], folder=True)

        names = ['']
        ids = path.rstrip('/').split('/')
        looks_like_dir = path.endswith('/')
        if len(ids) == 2 and not looks_like_dir:
            raise exceptions.NotFoundError(path)
        elif len(ids) == 3 and looks_like_dir:
            raise exceptions.NotFoundError(path)
        elif len(ids) > 3:
            raise exceptions.NotFoundError(path)

        package_id = ids[1]
        if 'doi:{}'.format(self.doi) != '{}{}'.format(DRYAD_DOI_BASE, package_id):
            raise exceptions.NotFoundError(path)

        # TODO: this should probably cached or the function memoized
        package_science_meta = await self._get_scientific_metadata_for_package(package_id)
        package_name = package_science_meta.getElementsByTagName('dcterms:title')[0]\
                                           .firstChild.wholeText
        names.append(package_name)

        if not looks_like_dir:
            file_id = ids[2]
            file_name = await self._get_filename_for_file(package_id, file_id)
            names.append(file_name)

        wb_path = DryadPath('/'.join(names), _ids=ids, folder=looks_like_dir)
        return wb_path

    async def validate_path(self, path, **kwargs):
        """Returns `DryadPath` if the string ``path`` is valid, else raises a `NotFoundError`. See
        :class:`waterbutler.providers.dryad.provider.DryadProvider` for details on path formatting.

        :param str path: string path to either a package or file
        :rtype: :class:`waterbutler.providers.dryad.path.DryadPath`
        :return: A `DryadPath` object representing the requested entity
        """

        if path == '/':
            return DryadPath('/', _ids=[self.doi], folder=True)

        names = ['']
        ids = path.rstrip('/').split('/')
        if len(ids) > 3:
            raise exceptions.NotFoundError(path)

        is_file = len(ids) == 3

        package_id = ids[1]
        if 'doi:{}'.format(self.doi) != '{}{}'.format(DRYAD_DOI_BASE, package_id):
            raise exceptions.NotFoundError(path)

        # TODO: this should probably cached or the function memoized
        package_science_meta = await self._get_scientific_metadata_for_package(package_id)
        package_name = package_science_meta.getElementsByTagName('dcterms:title')[0]\
                                           .firstChild.wholeText.strip(" \n")
        names.append(package_name)

        if is_file:
            file_id = ids[2]
            file_name = await self._get_filename_for_file(package_id, file_id)
            names.append(file_name)

        wb_path = DryadPath('/'.join(names), _ids=ids, folder=not is_file)

        return wb_path

    async def revalidate_path(self, base, path, folder=False):
        # if path is root, return package metadata in one-element list
        if base.is_root:
            package_id = self.doi.replace(DRYAD_DOI_BASE.replace('doi:', ''), '')
            package_science_meta = await self._get_scientific_metadata_for_package(package_id)
            package_name = package_science_meta.getElementsByTagName('dcterms:title')[0]\
                                               .firstChild.wholeText
            package_path = base.child(package_name, _id=package_id, folder=True)
            return package_path

        package_science_meta = await self._get_scientific_metadata_for_package(base.package_id)
        package = DryadPackageMetadata(base, package_science_meta)
        for child in package.file_parts:
            # convert from the identifier format listed in the metadata to a path
            child_file_id = child.split('/')[-1]
            child_path = await self._get_child_for_parent(base, child_file_id)
            if child_path.name == path:
                return child_path

        # couldn't find it
        raise exceptions.NotFoundError(path)

    def path_from_metadata(self, parent_path, metadata):
        return parent_path.child(metadata.name, _id=metadata.id, folder=metadata.is_folder)

    async def metadata(self, path, **kwargs):
        """ Interface to file and package metadata from Dryad

        :param path: Path mapping to waterbutler interpretation of Dryad file
        :type path: `waterbutler.core.path.WaterButlerPath`
        :returns:  `list` A list of metadata
        :raises: `urllib.error.HTTPError`
        """
        if not path.is_dir:
            return await self._file_metadata(path)

        # if path is root, return package metadata in one-element list
        if path.is_root:
            package_id = self.doi.replace(DRYAD_DOI_BASE.replace('doi:', ''), '')
            package_science_meta = await self._get_scientific_metadata_for_package(package_id)
            package_name = package_science_meta.getElementsByTagName('dcterms:title')[0]\
                                               .firstChild.wholeText
            package_path = path.child(package_name, _id=package_id, folder=True)
            return [DryadPackageMetadata(package_path, package_science_meta)]

        # if path is package, return list of children's metadata
        package_science_meta = await self._get_scientific_metadata_for_package(path.package_id)
        package = DryadPackageMetadata(path, package_science_meta)
        children = []
        for child in package.file_parts:
            # convert from the identifier format listed in the metadata to a path
            child_file_id = child.split('/')[-1]
            child_path = await self._get_child_for_parent(path, child_file_id)
            children.append(await self._file_metadata(child_path))

        return children

    async def revisions(self, path, **kwargs):
        """Currently, there are no revisions in dryad
        """
        science_meta = await self._get_scientific_metadata_for_file(path.package_id, path.file_id)
        return [DryadFileRevisionMetadata({}, science_meta)]

    async def download(self, path, **kwargs):
        """ Interface to downloading files from Dryad

        :param path: Path mapping to waterbutler interpretation of Dryad file
        :type path: `waterbutler.core.path.WaterButlerPath`
        :returns:  `waterbutler.core.streams.ResponseStreamReader` Download stream generator
        :raises:   `waterbutler.core.exceptions.DownloadError`
        """

        resp = await self.make_request(
            'GET',
            '{}{}/bitstream'.format(DRYAD_META_URL, path.full_identifier),
            expects=(200, 206),
            throws=exceptions.DownloadError,
        )

        system_meta = await self._get_system_metadata_for_file(path.package_id, path.file_id)
        size = system_meta.getElementsByTagName('size')[0].firstChild.wholeText

        # This is a kludge in place to fix a bug in the Dryad API.
        ret = streams.ResponseStreamReader(resp, size=size, name=path.name)
        ret._size = size
        return ret

    def can_duplicate_names(self):
        """Dryad packages and files always have different IDs"""
        return False

    def can_intra_move(self, other, path=None):
        return False

    def can_intra_copy(self, other, path=None):
        return False

    async def upload(self, stream, **kwargs):
        """Read-only provider, uploads are not allowed.

        :raises: `waterbutler.core.exceptions.ReadOnlyProviderError` always
        """
        raise exceptions.ReadOnlyProviderError(self.NAME)

    async def delete(self, **kwargs):
        """Read-only provider, deletions are not allowed.

        :raises: `waterbutler.core.exceptions.ReadOnlyProviderError` always
        """
        raise exceptions.ReadOnlyProviderError(self.NAME)

    async def move(self, *args, **kwargs):
        """Read-only provider, moves are not allowed.

        :raises: `waterbutler.core.exceptions.ReadOnlyProviderError` always
        """
        raise exceptions.ReadOnlyProviderError(self.NAME)

    # copy is okay if source is dryad and destination is not
    async def copy(self, dest_provider, *args, **kwargs):
        if dest_provider.NAME == self.NAME:
            raise exceptions.ReadOnlyProviderError(self.NAME)
        return await super().copy(dest_provider, *args, **kwargs)

    async def _file_metadata(self, path):
        """Retrieve file metadata from Dryad.

        :param DryadPath path: DryadPath object mapping Dryad file to WB interpretation
        :rtype: `DryadFileMetadata`
        :return:  Metadata for the Dryad file.
        :raises: `exceptions.MetadataError`
        :raises: `exceptions.DownloadError`
        """
        science_meta = await self._get_scientific_metadata_for_file(path.package_id, path.file_id)
        system_meta = await self._get_system_metadata_for_file(path.package_id, path.file_id)

        return DryadFileMetadata(path, science_meta, system_meta)

    async def _get_scientific_metadata_for_package(self, package_id):
        url = '{}{}'.format(DRYAD_META_URL, package_id)
        resp = await self.make_request(
            'GET',
            url,
            expects=(200, 404),
            throws=exceptions.MetadataError,
        )
        if resp.status == 404:
            await resp.release()
            raise exceptions.NotFoundError('/{}/'.format(package_id))

        return xml.dom.minidom.parseString(await resp.read())

    async def _get_scientific_metadata_for_file(self, package_id, file_id):
        url = '{}{}/{}'.format(DRYAD_META_URL, package_id, file_id)
        resp = await self.make_request(
            'GET',
            url,
            expects=(200, 404),
            throws=exceptions.MetadataError,
        )
        if resp.status == 404:
            await resp.release()
            raise exceptions.NotFoundError('/{}/{}'.format(package_id, file_id))

        return xml.dom.minidom.parseString(await resp.text())

    async def _get_system_metadata_for_file(self, package_id, file_id):
        url = '{}{}/{}/bitstream'.format(DRYAD_FILE_URL, package_id, file_id)
        resp = await self.make_request(
            'GET',
            url,
            expects=(200, 206),
            throws=exceptions.MetadataError
        )
        return xml.dom.minidom.parseString(await resp.text())

    async def _get_filename_for_file(self, package_id, file_id):
        """Dryad doesn't make the file name (as given when downloading the package) available in
        either the scientific or system metadata.  To get it, we must start a download of the file
        and read the Content-Disposition header.
        """

        url = '{}{}/{}/bitstream'.format(DRYAD_META_URL, package_id, file_id)
        file_stream = await self.make_request(
            'GET',
            url,
            expects=(200, 206),
            throws=exceptions.MetadataError,  # MetadataError b/c we only want the name
        )

        content = file_stream.headers.get('CONTENT-DISPOSITION', '')
        _, params = cgi.parse_header(content)
        file_stream.close()
        file_name = params['filename'].strip('"')
        return file_name

    async def _get_child_for_parent(self, parent_path, file_id):
        file_name = await self._get_filename_for_file(parent_path.package_id, file_id)
        return parent_path.child(file_name, _id=file_id, folder=False)
