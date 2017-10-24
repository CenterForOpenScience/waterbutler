import asyncio
import base64
import functools
from concurrent import futures
from urllib import parse

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath, WaterButlerPathPart

from waterbutler.tasks.core import __coroutine_unwrapper

from waterbutler.providers.evernote.metadata import EvernoteFileMetadata, EvernoteFileRevisionMetadata
from waterbutler.providers.evernote.utils import get_evernote_client, get_note, notes_metadata, timestamp_iso, OSFMediaStore

import ENML_PY as enml

# this module generalizes `waterbutler.tasks.core.backgroundify`
# to take an executor (which I use to enforce one job at time for Evernote)

# a ThreadPoolExecutor for enforcing a single job at a time for Evernote
_executor = futures.ThreadPoolExecutor(max_workers=1)

# code to write parameterized decorates
# borrowed from https://stackoverflow.com/a/26151604


def parametrized(dec):
    def layer(*args, **kwargs):
        def repl(f):
            return dec(f, *args, **kwargs)
        return repl
    return layer

async def backgrounded(func, executor, *args, **kwargs):
    """Runs the given function with the given arguments in
    a background thread."""
    loop = asyncio.get_event_loop()
    if asyncio.iscoroutinefunction(func):
        func = __coroutine_unwrapper(func)

    return (await loop.run_in_executor(
        executor,
        functools.partial(func, *args, **kwargs)
    ))


@parametrized
def backgroundify(func, executor):
    @functools.wraps(func)
    async def wrapped(*args, **kwargs):
        return (await backgrounded(func, executor, *args, **kwargs))
    return wrapped


@backgroundify(_executor)
def _evernote_notes(notebook_guid, token):

    print('_evernote_notes:notebook_guid, token ->', notebook_guid, token)

    client = get_evernote_client(token)

    try:
        notes = notes_metadata(client,
                        notebookGuid=notebook_guid,
                        includeTitle=True,
                        includeUpdated=True,
                        includeCreated=True,
                        includeContentLength=True,
                        includeUpdateSequenceNum=True)
    except Exception as e:
        raise exceptions.WaterButlerError('Error in _evernote_notes: type:{} str:{}'.format(type(e), str(e)), code=400)

    results = [{'title': note.title,
              'guid': note.guid,
              'created': timestamp_iso(note.created),
              'updated': timestamp_iso(note.updated),
              'length': note.contentLength,
              'updateSequenceNum': note.updateSequenceNum}
              for note in notes]

    print('_evernote_notes:results ->', results)

    return results


@backgroundify(_executor)
def _evernote_note(note_guid, token, withContent=False, withResourcesData=False):

    print('_evernote_note:note_guid, token ->', note_guid, token)
    client = get_evernote_client(token)

    try:
        note = get_note(client, note_guid,
                withContent=withContent,
                withResourcesData=withResourcesData)
    except Exception as e:
        raise exceptions.WaterButlerError('Error in _evernote_note: type:{} str:{}'.format(type(e), str(e)), code=400)
    else:

        if note.resources is not None:
            resources = dict([(resource.data.bodyHash, resource.data.body) for resource in note.resources])
        else:
            resources = dict()

        result = {'title': note.title,
            'guid': note.guid,
            'created': timestamp_iso(note.created),
            'updated': timestamp_iso(note.updated),
            'length': note.contentLength,
            'notebook_guid': note.notebookGuid,
            'content': note.content,
            'content_hash': base64.b64encode(note.contentHash),
            'resources': resources}

        print('_evernote_note:result ->', result)

        return result


@backgroundify(_executor)
def _evernote_note_store(token):

    client = get_evernote_client(token)
    return client.get_note_store()


@backgroundify(_executor)
def _enml_to_html(content, pretty, header, media_store, note_resources=None):
    return enml.ENMLToHTML(content, pretty=pretty, header=header,
        media_filter=enml.images_media_filter,
        media_store=media_store, note_resources=note_resources)


class EvernotePathPart(WaterButlerPathPart):
    DECODE = parse.unquote
    ENCODE = functools.partial(parse.quote, safe='')  # type: ignore


class EvernotePath(WaterButlerPath):
    PART_CLASS = EvernotePathPart


class EvernoteProvider(provider.BaseProvider):

    NAME = 'evernote'

    def __init__(self, auth, credentials, evernote_settings):

        print('EvernoteProvider.__init__: auth, credentials, evernote_settings', auth, credentials, evernote_settings)
        super().__init__(auth, credentials, evernote_settings)

    async def _package_metadata(self):

        token = self.credentials['token']
        notebook_guid = self.settings['folder']

        notes = await _evernote_notes(notebook_guid, token)

        return [EvernoteFileMetadata(note) for note in notes]

    async def _file_metadata(self, path):

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

        try:
            token = self.credentials['token']

            note_guid = path.identifier
            note = await _evernote_note(note_guid, token, withContent=True, withResourcesData=True)

            note_store = await _evernote_note_store(token)
            mediaStore = OSFMediaStore(note_store, note_guid, note_resources=note['resources'])
            html = await _enml_to_html(note["content"], pretty=True, header=False,
                  media_store=mediaStore)

            stream = streams.StringStream(html)
            stream.content_type = "text/html"
            stream.name = "{}.html".format(note['title'].replace("/", "_"))

        except Exception as e:
            raise exceptions.DownloadError(str(e), code=500, log_message=str(e), is_user_error=False)
        else:
            return stream

    async def validate_path(self, path, **kwargs):
        """
        :param path: Path to either a package or file.
        :type path: `str`
        """

        if path == '/':
            wbpath = EvernotePath(path='/', _ids=['/'])
        else:
            try:
                note_guid = path[1:]
                note_metadata = await self._file_metadata(note_guid)
                wbpath = EvernotePath("/" + parse.quote(note_metadata.name, safe=''), _ids=('/', note_guid))
            except Exception as e:
                raise exceptions.NotFoundError(path)

        return wbpath

    async def validate_v1_path(self, path, **kwargs):

        print('EvernoteProvider.validate_v1_path:path -> ', path)
        wbpath = await self.validate_path(path, **kwargs)

        if wbpath.is_root:
            return wbpath

        note_metadata = await self._file_metadata(wbpath.identifier)

        if isinstance(note_metadata, Exception):
            raise exceptions.NotFoundError(str(path))
        else:
            return wbpath

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

        # print("evernote.provider.move: ", args, kwargs)

        raise exceptions.ReadOnlyProviderError(self.NAME)

    # copy is okay if source is evernote and destination is not
    async def copy(self, dest_provider, *args, **kwargs):

        # print("evernote.provider.copy: ", args, kwargs)

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

    def path_from_metadata(self, parent_path, metadata):
        """ Unfortunately-named method, currently only used to get path name for zip archives. """
        return parent_path.child(metadata.export_name, _id=metadata.id, folder=metadata.is_folder)
