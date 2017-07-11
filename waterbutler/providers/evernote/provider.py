from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.tasks.core import backgroundify

from .metadata import EvernoteFileMetadata, EvernoteFileRevisionMetadata
from .utils import get_evernote_client, get_note, notes_metadata, timestamp_iso, OSFMediaStore

import ENML2HTML


@provider.throttle(concurrency=1, interval=1)
@backgroundify
def _evernote_notes(notebook_guid, token):

    client = get_evernote_client(token)

    # will want to pick up notes for the notebook
    # start with calculating the number of notes in nb

    try:
        notes = notes_metadata(client,
                        notebookGuid=notebook_guid,
                        includeTitle=True,
                        includeUpdated=True,
                        includeCreated=True,
                        includeContentLength=True)
    except Exception as e:
        print('_evernote_notes.e:', e)
        raise e

    results = [{'title': note.title,
              'guid': note.guid,
              'created': timestamp_iso(note.created),
              'updated': timestamp_iso(note.updated),
              'length': note.contentLength}
              for note in notes]

    return results


@provider.throttle(concurrency=1, interval=1)
@backgroundify
def _evernote_note(note_guid, token, withContent=False):

    client = get_evernote_client(token)

    try:
        note = get_note(client, note_guid,
                withContent=withContent,
                withResourcesData=withContent)
    except Exception as e:
        # TO DO reraise with the proper exceptions
        print('_evernote_note.e:', e)
        raise e
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

        print("EvernoteProvider._file_metadata -> path: ", path)

        token = self.credentials['token']
        note_md = await _evernote_note(path, token, withContent=False)

        return EvernoteFileMetadata(note_md)

    async def metadata(self, path, **kwargs):

        if str(path) == u'/':
            package = await self._package_metadata()
            return package

        if not path.is_dir:
            return (await self._file_metadata(path.identifier))

    async def download(self, path, **kwargs):
        """ Interface to downloading files from Evernote

        :param path: Path mapping to waterbutler interpretation of Evernote file
        :type path: `waterbutler.core.path.WaterButlerPath`
        :returns:  `waterbutler.core.streams.ResponseStreamReader` Download stream generator
        :raises:   `waterbutler.core.exceptions.DownloadError`
        """

        print("EvernoteProvider.download: path, kwargs", type(path), str(path), path.identifier, kwargs)
        token = self.credentials['token']
        client = get_evernote_client(token)

        note_guid = path.identifier
        note = await _evernote_note(note_guid, token, withContent=True)

        # convert to HTML
        mediaStore = OSFMediaStore(client.get_note_store(), note_guid)
        html = ENML2HTML.ENMLToHTML(note["content"], pretty=True, header=False,
              media_store=mediaStore)

        # HACK -- let me write markdown
        # html = "**Hello World**"
        # html = """<b>Hello world</b>. Go read the <a href="http://nytimes.com">NYT</a>"""

        stream = streams.StringStream(html)
        stream.content_type = "text/html"
        stream.name = "{}.html".format(note['title'])

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

        if path == '/':
            wbpath = WaterButlerPath(path='/', _ids=['/'])
        else:
            try:
                note_guid = path[1:]
                print('evernote.provider.validate_path.note_guid', note_guid)
                note_metadata = await self._file_metadata(note_guid)
                print('evernote.provider.validate_path.note_metadata', note_metadata)

                print("validate_path.note_metadata.name: {}".format(note_metadata.name))
                wbpath = WaterButlerPath("/" + note_metadata.name, _ids=('/', note_guid))
            except Exception as e:
                raise exceptions.NotFoundError(path)

        print("evernote.provider.validate_path.wbpath: ", str(wbpath), type(wbpath), wbpath.identifier)
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

        note_metadata = await self._file_metadata(wbpath.identifier)

        if isinstance(note_metadata, Exception):
            raise exceptions.NotFoundError(str(path))
        else:
            # TO: actually validate the plan
            # for now just return wbpath
            if True:
                return wbpath
            else:
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

    async def move(self, *args, **kwargs):

        print("evernote.provider.move: ", args, kwargs)

        raise exceptions.ReadOnlyProviderError(self.NAME)

    # copy is okay if source is evernote and destination is not
    async def copy(self, dest_provider, *args, **kwargs):

        print("evernote.provider.copy: ", args, kwargs)

        if dest_provider.NAME == 'evernote':
            raise exceptions.ReadOnlyProviderError(self.NAME)
        return await super().copy(dest_provider, *args, **kwargs)

    def can_duplicate_names(self):
        """
            Evernote write access is not allowed.

        Raises:
            `waterbutler.core.exceptions.ReadOnlyProviderError` Always
        """
        return False

    async def revisions(self, path, **kwargs):
        metadata = await self.metadata(path)
        return [EvernoteFileRevisionMetadata.from_metadata(metadata)]
