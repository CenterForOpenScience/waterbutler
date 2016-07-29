import cgi

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.tasks.core import backgroundify

from .metadata import EvernoteFileMetadata
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
                    includeCreated=True,
                    includeContentLength=True)

    results = [{'title': note.title,
              'guid': note.guid,
              'created': timestamp_iso(note.created),
              'updated': timestamp_iso(note.updated),
              'length':note.contentLength}
              for note in notes]

    return results


class EvernoteProvider(provider.BaseProvider):

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

        token = self.credentials['token']
        notebook_guid = self.settings['folder']

        notes = await _evernote_notes(notebook_guid, token)

        return [EvernoteFileMetadata(note) for note in notes]


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

        if str(path) == u'/':
            package = await self._package_metadata()
            return package

        if not path.is_dir:
            return (await self._file_metadata(path.path))


    async def download(self, path, **kwargs):
        """ Interface to downloading files from Evernote

        :param path: Path mapping to waterbutler interpretation of Evernote file
        :type path: `waterbutler.core.path.WaterButlerPath`
        :returns:  `waterbutler.core.streams.ResponseStreamReader` Download stream generator
        :raises:   `waterbutler.core.exceptions.DownloadError`
        """

        # TO DO: IMPORTANT
        # what needs to be returned?
        # looking at Google docs code for example
        # https://github.com/CenterForOpenScience/waterbutler/blob/63b7d469e5545de9f2183b964fb6264fd9a423a5/waterbutler/providers/box/provider.py#L211-L227

        # if path.identifier is None:
        #     raise exceptions.DownloadError('"{}" not found'.format(str(path)), code=404)

        # query = {}
        # if revision and revision != path.identifier:
        #     query['version'] = revision

        # resp = await self.make_request(
        #     'GET',
        #     self.build_url('files', path.identifier, 'content', **query),
        #     range=range,
        #     expects=(200, 206),
        #     throws=exceptions.DownloadError,
        # )

        # ResponseStreamReader:
        # https://github.com/CenterForOpenScience/waterbutler/blob/63b7d469e5545de9f2183b964fb6264fd9a423a5/waterbutler/core/streams/http.py#L141-L183


        return streams.StringStream("hello")
        # return streams.ResponseStreamReader(resp)


    async def validate_path(self, path, **kwargs):
        """
        :param path: Path to either a package or file.
        :type path: `str`
        """

        wbpath = WaterButlerPath(path)
        if wbpath.is_root:
            return wbpath
        if len(wbpath.parts) == 1 and wbpath.is_dir:
                raise exceptions.NotFoundError(path)
        if len(wbpath.parts) > 1:
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