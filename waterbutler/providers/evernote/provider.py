from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.tasks.core import backgroundify

from .metadata import EvernoteFileMetadata
from .utils import get_evernote_client, get_note, notes_metadata, timestamp_iso, OSFMediaStore
import ENML2HTML


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
              'length': note.contentLength}
              for note in notes]

    return results


@backgroundify
def _evernote_note(note_guid, token, withContent=False):

    client = get_evernote_client(token)

    try:
        note = get_note(client, note_guid,
                withContent=withContent,
                withResourcesData=withContent)
    except Exception as e:
        return e
    else:
        result = {'title': note.title,
            'guid': note.guid,
            'created': timestamp_iso(note.created),
            'updated': timestamp_iso(note.updated),
            'length': note.contentLength,
            'notebook_guid': note.notebookGuid,
            'content': note.content}
        return result


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

        # print("_file_metadata -> path: ", path)

        token = self.credentials['token']
        note_md = await _evernote_note(path, token, withContent=False)

        return EvernoteFileMetadata(note_md)

    async def metadata(self, path, **kwargs):

        # TO DO: IMPORTANT
        """
        responds to a GET request for the url you posted.
        If the `path` query arg refers to a particular file/note, it should return the metadata for that file/note.
        If `path` is just `/`, it should return a list of metadata objects for all file/notes in the root directory.
         IIRC, Evernote doesn’t have a hierarchy, so the root directory is just a collection of all available notes.
        """

        # print("metadata: path: {}".format(path), type(path), path.is_dir)

        if str(path) == u'/':
            package = await self._package_metadata()
            return package

        if not path.is_dir:
            print("metdata path.path:", path.path)
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

        print("evernote provider download: starting")
        token = self.credentials['token']
        client = get_evernote_client(token)

        note_guid = path.parts[1].raw
        note_metadata = await _evernote_note(note_guid, token, withContent=True)

        # convert to HTML
        mediaStore = OSFMediaStore(client.get_note_store(), note_guid)
        html = ENML2HTML.ENMLToHTML(note_metadata["content"], pretty=True, header=False,
              media_store=mediaStore)

        # HACK -- let me write markdown
        # html = "**Hello World**"
        # html = """<b>Hello world</b>. Go read the <a href="http://nytimes.com">NYT</a>"""

        stream = streams.StringStream(html)
        stream.content_type = "text/html"
        stream.name = "{}.html".format(note_guid)

        # modeling after gdoc provider
        # https://github.com/CenterForOpenScience/waterbutler/blob/develop/waterbutler/providers/googledrive/provider.py#L181-L185

        print("evernote provider download: finishing")
        return stream

    async def validate_path(self, path, **kwargs):
        """
        :param path: Path to either a package or file.
        :type path: `str`
        """

        print("in Evernote.validate_path. path: {}".format(path))

        wbpath = WaterButlerPath(path)

        print("wbpath.is_root: {}".format(wbpath.is_root))
        print("wbpath.is_dir: {}".format(wbpath.is_dir))
        print("len(wbpath.parts): {}".format(len(wbpath.parts)))

        if wbpath.is_root:
            return wbpath
        if len(wbpath.parts) == 2 and wbpath.is_dir:
                raise exceptions.NotFoundError(path)
        if len(wbpath.parts) > 2:
            raise exceptions.NotFoundError(path)

        return wbpath

    async def validate_v1_path(self, path, **kwargs):
        """
            See :func:`waterbutler.providers.evernote.provider.EvernoteProvider.validate_path`.
            Additionally queries the Evernote API to check if the package exists.
        """

        print("in Evernote.validate_v1_path. path: {}".format(path),
               "kwargs: ", kwargs)

        wbpath = await self.validate_path(path, **kwargs)

        if wbpath.is_root:
            return wbpath

        token = self.credentials['token']
        print("wbpath.parts[1].raw", wbpath.parts[1].raw)
        note = await _evernote_note(wbpath.parts[1].raw, token, withContent=False)
        if isinstance(note, Exception):
            print("validate_v1_path. could not get Note", note)
            raise exceptions.NotFoundError(str(path))
        else:
            print("validate_v1_path. note is not None")
            if note['notebook_guid'] == self.settings['folder']:
                return wbpath
            else:
                print('notebook_guid {} does not match folder {}'.format(note['notebook_guid'], self.settings['folder']))
                raise exceptions.NotFoundError(str(path))

    def can_intra_move(self, other, path=None):
        """
            Moves are not allowed. Only Copies from Evernote to another provider.

            Raises:
                `waterbutler.core.exceptions.ReadOnlyProviderError` Always
        """
        return False

    def can_intra_copy(self, other, path=None):
        """
            All files in Evernote are able to be copied out (if accessible).

            :returns: `True` Always
        """
        return False

    async def intra_copy(self, dest_provider, src_path, dest_path):
        """
            Accesses Evernote file bitstream for file access.
            Package access raises 404 error.
            Must access file metadata first in order to get file name and size.

            :returns: `coroutine` File stream generated by :func:`waterbutler.providers.evernote.provider.EvernoteProvider.download`
        """
        raise exceptions.ReadOnlyProviderError(self)

    async def _do_intra_move_or_copy(self, dest_provider, src_path, dest_path):
        """
            Accesses Evernote file bitstream for file access.
            Package access raises 404 error.
            Must access file metadata first in order to get file name and size.

            :returns: `coroutine`
        """

        raise exceptions.ReadOnlyProviderError(self)

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
