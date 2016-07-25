import cgi

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.tasks.core import backgroundify

from .metadata import EvernotePackageMetadata, EvernoteFileMetadata
from .settings import EVERNOTE_META_URL, EVERNOTE_FILE_URL
from .utils import get_evernote_client, get_notebooks, notes_metadata, timestamp_iso

@backgroundify
def _evernote_notes(notebook_guid, token):

    client = get_evernote_client(token)

    # will want to pick up notes for the notebook
    # start with calculating the number of notes in nb

    notes = notes_metadata(client,
                    notebookGuid=notebook_guid,
                    includeTitle=True,
                    includeUpdated=True,
                    includeCreated=True)

    results = [{'title': note.title,
              'guid': note.guid,
              'created': timestamp_iso(note.created),
              'updated': timestamp_iso(note.updated)}
              for note in notes]

    return results


class EvernoteProvider(provider.BaseProvider):
    """
        Read_only provider for Evernote packages.
        Within the Evernote API, packages are accessed via the following format:
            `<API ACCESS POINT>/doi:10.5061/evernote.XXXX`
        Where `XXXX` denotes the packages-specific DOI suffix.
        Similarly, files are accessed with the following format:
            `<API ACCESS POINT>/doi:10.5061/evernote.XXXX/Y`
        Where `Y` is an index number assigned to each file.

        In Waterbutler, this is translated to the following table:

        ========== ================================
        WB Path    Object
        ========== ================================
        `/`        Evernote repository (unimplemented)
        `/XXXX/`   Evernote Package
        `/XXXX/YY` Package File
        ========== ================================

        Paths need to follow this scheme or will :class:`waterbutler.core.exceptions.NotFoundError`
        when validated through :func:`waterbutler.providers.evernote.provider.EvernoteProvider.validate_path`
    """

    NAME = 'evernote'

    def __init__(self, auth, credentials, evernote_settings):

        super().__init__(auth, credentials, evernote_settings)

    async def _package_metadata(self):
        """ Interface to file and package metadata from Evernote

        :param path: Path mapping to waterbutler interpretation of Evernote package
        :type path: `waterbutler.core.path.WaterButlerPath`
        :returns:  `list` -- A list of metadata
        :raises: `urllib.error.HTTPError`

        """

        # TO DO: 


        token = self.credentials['token']
        notebook_guid = self.settings['folder']

        notes = await _evernote_notes(notebook_guid, token)
        print (notes)

        #body_text = await _body_text('')
        return EvernotePackageMetadata(notes)
        # return []

    async def _file_metadata(self, path):
        """ Interface to file and package metadata from Evernote

        :param path: Path mapping to waterbutler interpretation of Evernote file
        :type path: `waterbutler.core.path.WaterButlerPath`
        :returns:  `list` A list of metadata
        :raises: `urllib.error.HTTPError`

        """

        metadata_resp = await self.make_request(
            'GET',
            EVERNOTE_META_URL + path.strip('/'),
            expects=(200, 206),
            throws=exceptions.MetadataError)

        file_metadata_resp = await self.make_request(
            'GET',
            EVERNOTE_FILE_URL + path.strip('/') + "/bitstream",
            expects=(200, 206),
            throws=exceptions.MetadataError)

        file_stream = await self.make_request(
            'GET',
            EVERNOTE_META_URL + path.strip('/') + '/bitstream',
            expects=(200, 206),
            throws=exceptions.DownloadError,
        )
        content = file_stream.headers.get('Content-Disposition', '')
        _, params = cgi.parse_header(content)
        file_stream.close()
        file_name = params['filename']
        metadata_text = await metadata_resp.text()
        file_metadata = await file_metadata_resp.text()

        return EvernoteFileMetadata(metadata_text, path.strip('/'), file_metadata, file_name)

    async def metadata(self, path, **kwargs):
        """ Interface to file and package metadata from Evernote

        :param path: Path mapping to waterbutler interpretation of Evernote file
        :type path: `waterbutler.core.path.WaterButlerPath`
        :returns:  `list` A list of metadata
        :raises: `urllib.error.HTTPError`
        """

        # TO DO: IMPORTANT
        """
        responds to a GET request for the url you posted.  
        If the `path` query arg refers to a particular file/note, it should return the metadata for that file/note.
        If `path` is just `/`, it should return a list of metadata objects for all file/notes in the root directory.  
         IIRC, Evernote doesn’t have a hierarchy, so the root directory is just a collection of all available notes.
        """

        print ("metadata: path: {}".format(path), type(path), path.is_dir)

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
        """ Interface to downloading files from Evernote

        :param path: Path mapping to waterbutler interpretation of Evernote file
        :type path: `waterbutler.core.path.WaterButlerPath`
        :returns:  `waterbutler.core.streams.ResponseStreamReader` Download stream generator
        :raises:   `waterbutler.core.exceptions.DownloadError`
        """

       # TO DO: IMPORTANT

        resp = await self.make_request(
            'GET',
            EVERNOTE_META_URL + path.path + '/bitstream',
            expects=(200, 206),
            throws=exceptions.DownloadError,
        )

        file_metadata = await self._file_metadata(path.path)
        # This is a kludge in place to fix a bug in the Evernote API.
        ret = streams.ResponseStreamReader(resp,
            size=file_metadata.size,
            name=file_metadata.name)
        ret._size = file_metadata.size
        return ret

    async def validate_path(self, path, **kwargs):
        """
        Returns WaterButlerPath if the string `path` is valid, else raises
        not found error. See :class:`waterbutler.providers.evernote.provider.EvernoteProvider`
        for details on path formatting.

        :param path: Path to either a package or file.
        :type path: `str`
        """

       # TO DO: IMPORTANT

        wbpath = WaterButlerPath(path)
        if wbpath.is_root:
            return wbpath
        if len(wbpath.parts) == 2 and not wbpath.is_dir:
                raise exceptions.NotFoundError(path)
        elif len(wbpath.parts) == 3 and not wbpath.is_file:
            raise exceptions.NotFoundError(path)
        return wbpath

    async def validate_v1_path(self, path, **kwargs):
        """
            See :func:`waterbutler.providers.evernote.provider.EvernoteProvider.validate_path`.
            Additionally queries the Evernote API to check if the package exists.
        """

        # TO DO: IMPORTANT

        wbpath = await self.validate_path(path, **kwargs)
        if wbpath.is_root:
            return wbpath
        full_url = EVERNOTE_META_URL + wbpath.parts[1].value
        if wbpath.is_file:
            full_url += '/' + wbpath.parts[2].value

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

    def can_intra_move(self, other, path=None):
        """
            Moves are not allowed. Only Copies from Evernote to another provider.

            Raises:
                `waterbutler.core.exceptions.ReadOnlyProviderError` Always
        """
        raise exceptions.ReadOnlyProviderError(self)

    def can_intra_copy(self, other, path=None):
        """
            All files in Evernote are able to be copied out (if accessible).

            :returns: `True` Always
        """
        return True

    async def intra_copy(self, dest_provider, src_path, dest_path):
        """
            Accesses Evernote file bitstream for file access.
            Package access raises 404 error.
            Must access file metadata first in order to get file name and size.

            :returns: `coroutine` File stream generated by :func:`waterbutler.providers.evernote.provider.EvernoteProvider.download`
        """
        return await self._do_intra_move_or_copy(dest_provider,
                                                src_path,
                                                dest_path)

    async def _do_intra_move_or_copy(self, dest_provider, src_path, dest_path):
        """
            Accesses Evernote file bitstream for file access.
            Package access raises 404 error.
            Must access file metadata first in order to get file name and size.

            :returns: `coroutine`
        """
        resp = await self.make_request(
            'GET',
            EVERNOTE_META_URL + src_path.path.strip('/') + '/bitstream',
            expects=(200, 206),
            throws=exceptions.DownloadError,
        )
        file_metadata = await self._file_metadata(src_path.path)
        name = file_metadata.name
        size = file_metadata.size

        download_stream = streams.ResponseStreamReader(resp,
                            size=size,
                            name=name)
        dest_path.rename(file_metadata.name)
        return await dest_provider.upload(download_stream, dest_path)

    async def upload(self, stream, **kwargs):
        """
        Uploads are not allowed.

        Raises:
            `waterbutler.core.exceptions.ReadOnlyProviderError` Always
        """
        raise exceptions.ReadOnlyProviderError(self)

    async def delete(self, **kwargs):
        """
        Deletions are not allowed.

        Raises:
            exceptions.ReadOnlyProviderError: Always
        """
        raise exceptions.ReadOnlyProviderError(self)

    def can_duplicate_names(self):
        """
            Evernote write access is not allowed.

        Raises:
            `waterbutler.core.exceptions.ReadOnlyProviderError` Always
        """
        return False