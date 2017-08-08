import datetime
import time
import functools
from evernote.api.client import EvernoteClient
from evernote.edam.error.ttypes import (EDAMSystemException, EDAMErrorCode)
from evernote.edam.notestore.ttypes import (NoteFilter, NotesMetadataResultSpec)

import ENML_PY as enml
import base64


def hex_decode(s):
    # http://stackoverflow.com/a/10619257/7782

    try:
        import binascii
        return binascii.unhexlify(s)
    except:
        return s.decode('hex')


def evernote_wait_try_again(f):
    """
    Wait until mandated wait and try again
    http://dev.evernote.com/doc/articles/rate_limits.php
    """

    @functools.wraps(f)
    def f2(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except EDAMSystemException as e:
            if e.errorCode == EDAMErrorCode.RATE_LIMIT_REACHED:
                print("Evernote rate limit: {0} s. wait".format(e.rateLimitDuration))
                time.sleep(e.rateLimitDuration)
                print("Evernote: wait over")
                return f(*args, **kwargs)
            else:
                # don't swallow other exceptions
                raise e

    return f2


class RateLimitingEvernoteProxy(object):
    __slots__ = ["_obj"]

    def __init__(self, obj):
        object.__setattr__(self, "_obj", obj)

    def __getattribute__(self, name):
        return evernote_wait_try_again(
            getattr(object.__getattribute__(self, "_obj"), name))


def get_evernote_client(token, sandbox=False):
    _client = EvernoteClient(token=token, sandbox=sandbox)
    return RateLimitingEvernoteProxy(_client)


@evernote_wait_try_again
def get_notebooks(client):

    noteStore = client.get_note_store()
    return [{'name': notebook.name,
             'guid': notebook.guid,
             'stack': notebook.stack,
             'defaultNotebook': notebook.defaultNotebook} for notebook in noteStore.listNotebooks()]

# https://dev.evernote.com/doc/reference/NoteStore.html#Fn_NoteStore_getNotebook


@evernote_wait_try_again
def get_notebook(client, nb_guid):
    noteStore = client.get_note_store()
    notebook = noteStore.getNotebook(nb_guid)
    return {'name': notebook.name,
             'guid': notebook.guid,
             'stack': notebook.stack,
             'defaultNotebook': notebook.defaultNotebook}


@evernote_wait_try_again
def notes_metadata(client, **input_kw):
    """ """
    # http://dev.evernote.com/documentation/reference/NoteStore.html#Fn_NoteStore_findNotesMetadata

    noteStore = client.get_note_store()

    # pull out offset and page_size value if supplied
    offset = input_kw.pop('offset', 0)
    page_size = input_kw.pop('page_size', 100)

    # let's update any keywords that are updated
    # http://dev.evernote.com/documentation/reference/NoteStore.html#Struct_NotesMetadataResultSpec

    include_kw = {
        'includeTitle': False,
        'includeContentLength': False,
        'includeCreated': False,
        'includeUpdated': False,
        'includeDeleted': False,
        'includeUpdateSequenceNum': False,
        'includeNotebookGuid': False,
        'includeTagGuids': False,
        'includeAttributes': False,
        'includeLargestResourceMime': False,
        'includeLargestResourceSize': False
    }

    include_kw.update([(k, input_kw[k]) for k in set(input_kw.keys()) & set(include_kw.keys())])

    # keywords aimed at NoteFilter
    # http://dev.evernote.com/documentation/reference/NoteStore.html#Struct_NoteFilter
    filter_kw_list = ('order', 'ascending', 'words', 'notebookGuid', 'tagGuids', 'timeZone', 'inactive', 'emphasized')
    filter_kw = dict([(k, input_kw[k]) for k in set(filter_kw_list) & set(input_kw.keys())])

    # what possible parameters are aimed at NoteFilter
    # order	i32		optional
    # ascending	bool		optional
    # words	string		optional
    # notebookGuid	Types.Guid		optional
    # tagGuids	list<Types.Guid>		optional
    # timeZone	string		optional
    # inactive   bool
    # emphasized string

    more_nm = True

    while more_nm:

        # grab a page of data

        f2 = evernote_wait_try_again(noteStore.findNotesMetadata)
        note_meta = f2(NoteFilter(**filter_kw), offset, page_size,
                                    NotesMetadataResultSpec(**include_kw))

        # yield each individually
        for nm in note_meta.notes:
            yield nm

        # grab next page if there is more to grab
        if len(note_meta.notes):
            offset += len(note_meta.notes)
        else:
            more_nm = False


@evernote_wait_try_again
def get_note(client, guid,
            withContent=False,
            withResourcesData=False,
            withResourcesRecognition=False,
            withResourcesAlternateData=False):

    # https://dev.evernote.com/doc/reference/NoteStore.html#Fn_NoteStore_getNote
    print('utils.get_note guid, withContent, withResourcesData: ', guid, withContent, withResourcesData)
    noteStore = client.get_note_store()
    return noteStore.getNote(guid, withContent, withResourcesData,
                                 withResourcesRecognition, withResourcesAlternateData)


def timestamp_iso(ts):
    """
    ts in ms since 1970
    """
    return datetime.datetime.utcfromtimestamp(ts / 1000.).isoformat()


class OSFMediaStore(enml.MediaStore):
    def __init__(self, note_store, note_guid, note_resources=None):
        super().__init__(note_store, note_guid)
        self.note_resources = note_resources if note_resources is not None else {}

    def _get_resource_by_hash(self, hash_str):
        """
        get resource by its hash
        """

        hash_bin = hex_decode(hash_str)

        if hash_bin in self.note_resources:
            body = self.note_resources[hash_bin]
        else:
            resource = self.note_store.getResourceByHash(self.note_guid, hash_bin, True, False, False)
            body = resource.data.body

        return body

    def save(self, hash_str, mime_type):
        # hash_str is the hash digest string of the resource file
        # mime_type is the mime_type of the resource that is about to be saved
        # you can get the mime type to file extension mapping by accessing the dict MIME_TO_EXTENSION_MAPPING

        # retrieve the binary data
        data = self._get_resource_by_hash(hash_str)
        # some saving operation [ not needed for embedding into data URI]

        # return the URL of the resource that has just been saved
        # convert content to data:uri
        # https://gist.github.com/jsocol/1089733

        data64 = u''.join([row.decode('utf-8') for row in base64.encodestring(data).splitlines()])
        return u'data:{};base64,{}'.format(mime_type, data64)
