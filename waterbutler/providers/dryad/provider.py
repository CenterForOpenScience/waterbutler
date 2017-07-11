import cgi
import xml.dom.minidom

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions

from waterbutler.providers.dryad.path import DryadPath
from waterbutler.providers.dryad.utils import get_xml_element
from waterbutler.providers.dryad.settings import (DRYAD_META_URL,
                                                  DRYAD_FILE_URL,
                                                  DRYAD_DOI_BASE)
from waterbutler.providers.dryad.metadata import (DryadPackageMetadata,
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
        semantics. ``path`` is an ID-based path string (e.g. ``"/hs727/1"``). API queries must
        be issued in order to get the names of the package and file.

        :param str path: ID-based path string for either root, a package, or file
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
        package_name = get_xml_element(package_science_meta, 'dcterms:title').strip(" \n")
        names.append(package_name)

        if not looks_like_dir:
            file_id = ids[2]
            file_name = await self._get_filename_for_file(package_id, file_id)
            names.append(file_name)

        wb_path = DryadPath('/'.join(names), _ids=ids, folder=looks_like_dir)
        return wb_path

    async def validate_path(self, path, **kwargs):
        """Returns a `DryadPath` if the string ``path`` is valid, else raises a `NotFoundError`.
        ``path`` is an ID-based path string (e.g. ``"/hs727/1"``). API queries must be issued in
        order to get the names of the package and file. See
        :class:`waterbutler.providers.dryad.provider.DryadProvider` for details on path formatting.

        :param str path: ID-based path string for either root, a package, or file
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
        package_name = get_xml_element(package_science_meta, 'dcterms:title').strip(" \n")
        names.append(package_name)

        if is_file:
            file_id = ids[2]
            file_name = await self._get_filename_for_file(package_id, file_id)
            names.append(file_name)

        wb_path = DryadPath('/'.join(names), _ids=ids, folder=not is_file)

        return wb_path

    async def revalidate_path(self, base, path, folder=False):
        """Takes a `DryadPath`, ``base``, and a stringy name-based path, and build a `DryadPath`
        object representing the child of ``base`` named ``path``.  Throws a `NotFoundError` if
        no child named ``path`` exists under ``base``.

        :param DryadPath base: DryadPath object represent a base folder
        :param str path: the name (NOT ID) of a child file or folder to lookup
        :param bool folder: whether the thing being looked up is a file or folder
        :rtype: :class:`waterbutler.providers.dryad.path.DryadPath`
        :return: a `DryadPath` representing child named ``path`` of ``base``
        """

        # if path is root, return package metadata in one-element list
        if base.is_root:
            package_id = self.doi.replace(DRYAD_DOI_BASE.replace('doi:', ''), '')
            package_science_meta = await self._get_scientific_metadata_for_package(package_id)
            package_name = get_xml_element(package_science_meta, 'dcterms:title').strip(" \n")
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
        """Takes a `DryadPath` object representing a parent path and a metadata object representing
        a child of ``parent_path``, and returns a `DryadPath` object representing the child.

        :param DryadPath parent_path: DryadPath object representing a package
        :param DryadFileMetadata metadata: A metadata object representing a child of the parent.
        :rtype: :class:`waterbutler.providers.dryad.path.DryadPath`
        :return: a `DryadPath` representing a child of ``parent_path``
        """
        return parent_path.child(metadata.name, _id=metadata.id, folder=metadata.is_folder)

    async def metadata(self, path, **kwargs):
        """Interface to file and package metadata from Dryad

        :param DryadPath path: Path mapping to waterbutler interpretation of Dryad file
        :rtype: `list`
        :return: A list of metadata objects DryadPackageMetadata or DryadFileMetadata
        """
        if not path.is_dir:
            return await self._file_metadata(path)

        # if path is root, return package metadata in one-element list
        if path.is_root:
            package_id = self.doi.replace(DRYAD_DOI_BASE.replace('doi:', ''), '')
            package_science_meta = await self._get_scientific_metadata_for_package(package_id)
            package_name = get_xml_element(package_science_meta, 'dcterms:title').strip(" \n")
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
        """Dryad doesn't support revisions, so always return current version as latest.
        """
        science_meta = await self._get_scientific_metadata_for_file(path.package_id, path.file_id)
        return [DryadFileRevisionMetadata({}, science_meta)]

    async def download(self, path, **kwargs):
        """Interface to downloading files from Dryad.

        :param DryadPath path: DryadPath representing a root, package, or file
        :rtype: :class:`waterbutler.core.streams.ResponseStreamReader`
        :return: Download stream generator
        :raises: :class:`waterbutler.core.exceptions.DownloadError`
        """

        resp = await self.make_request(
            'GET',
            '{}{}/bitstream'.format(DRYAD_META_URL, path.full_identifier),
            expects=(200, 206),
            throws=exceptions.DownloadError,
        )

        system_meta = await self._get_system_metadata_for_file(path.package_id, path.file_id)
        size = get_xml_element(system_meta, 'size')

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

        :raises: `waterbutler.core.exceptions.ReadOnlyProviderError`
        """
        raise exceptions.ReadOnlyProviderError(self.NAME)

    async def delete(self, **kwargs):
        """Read-only provider, deletions are not allowed.

        :raises: `waterbutler.core.exceptions.ReadOnlyProviderError`
        """
        raise exceptions.ReadOnlyProviderError(self.NAME)

    async def move(self, *args, **kwargs):
        """Read-only provider, moves are not allowed.

        :raises: `waterbutler.core.exceptions.ReadOnlyProviderError`
        """
        raise exceptions.ReadOnlyProviderError(self.NAME)

    # copy is okay if source is dryad and destination is not
    async def copy(self, dest_provider, *args, **kwargs):
        if dest_provider.NAME == self.NAME:
            raise exceptions.ReadOnlyProviderError(self.NAME)
        return await super().copy(dest_provider, *args, **kwargs)

    async def _file_metadata(self, path):
        """Retrieve file metadata from Dryad.

        :param DryadPath path: DryadPath object representing a file
        :rtype: `DryadFileMetadata`
        :return: a metadata object for the file
        :raises: `exceptions.MetadataError`
        :raises: `exceptions.DownloadError`
        """
        science_meta = await self._get_scientific_metadata_for_file(path.package_id, path.file_id)
        system_meta = await self._get_system_metadata_for_file(path.package_id, path.file_id)

        return DryadFileMetadata(path, science_meta, system_meta)

    async def _get_scientific_metadata_for_package(self, package_id):
        """Retrieve the scientific metadata for a package with ID ``package_id``. Queries the
        ``object`` endpoint of the Dryad API.

        :param str package_id: ID of a package (the suffix of the DOI)
        :rtype: `xml.dom.minidom.Document`
        :return: XML document containing the scientific metadata for the package
        :raises: `exceptions.MetadataError`
        :raises: `exceptions.NotFoundError`
        """
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
        """Retrieve the scientific metadata for a file with ID ``file_id`` in the package with
        ID ``package_id``. Queries the ``object`` endpoint of the Dryad API.

        :param str package_id: ID of a package (the suffix of the DOI)
        :param str file_id: ID of a file (its index within the package)
        :rtype: `xml.dom.minidom.Document`
        :return: XML document containing the scientific metadata for the file
        :raises: `exceptions.MetadataError`
        :raises: `exceptions.NotFoundError`
        """
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
        """Retrieve the system metadata for a file with ID ``file_id`` in the package with ID
        ``package_id``. Queries the ``meta`` endpoint of the Dryad API.  This endpoint contains
        the file size and content type.

        :param str package_id: ID of a package (the suffix of the DOI)
        :param str file_id: ID of a file (its index within the package)
        :rtype: `xml.dom.minidom.Document`
        :return: XML document containing the system metadata for the file
        :raises: `exceptions.MetadataError`
        :raises: `exceptions.NotFoundError`
        """
        url = '{}{}/{}/bitstream'.format(DRYAD_FILE_URL, package_id, file_id)
        resp = await self.make_request(
            'GET',
            url,
            expects=(200, 206),
            throws=exceptions.MetadataError
        )
        return xml.dom.minidom.parseString(await resp.text())

    async def _get_filename_for_file(self, package_id, file_id):
        """Retrieve the name of the file with ID ``file_id`` in the package with ID ``package_id``.
        Dryad doesn't make the file name (as given when downloading the package) available in
        either the scientific or system metadata.  To get it, we must start a download of the file
        and read the Content-Disposition header.

        :param str package_id: ID of a package (the suffix of the DOI)
        :param str file_id: ID of a file (its index within the package)
        :rtype: `str`
        :return: the name of the file as if it was downloaded directly from Dryad.
        :raises: `exceptions.MetadataError`
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
        """Given a `DryadPath` object representing a package and the ID of a file within it
        (``file_id``), looks up the name of the file and constructs a new `DryadPath` object
        representing it.

        :param DryadPath parent_path: `DryadPath` object representing a package
        :param str file_id: ID of a file (its index within the package)
        :rtype: :class:`waterbutler.providers.dryad.path.DryadPath`
        :return: a `DryadPath` object representing the child
        """
        file_name = await self._get_filename_for_file(parent_path.package_id, file_id)
        return parent_path.child(file_name, _id=file_id, folder=False)
