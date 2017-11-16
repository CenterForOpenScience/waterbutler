import aiohttpretty
import pytest
from http import client
from urllib import parse

from unittest import mock

from waterbutler.core import exceptions
from waterbutler.tasks.core import backgroundify
from waterbutler.core.path import WaterButlerPath
import waterbutler.providers.evernote.provider as evernote_provider
from waterbutler.providers.evernote.provider import (EvernoteProvider, EvernotePath)
from waterbutler.providers.evernote.metadata import EvernoteFileMetadata


# def test_import():
#   import sys
#   print (sys.path)
#   try:
#     # from waterbutler.providers.evernote.provider import EvernoteProvider
#     #from evernote.api.client import EvernoteClient
#     import evernote
#     print(dir(evernote))
#     print(evernote.__path__)
#     print('successful import')
#   except Exception as e:
#     print (e)

#   assert True


@pytest.fixture
def auth():
    return {'id': 'cyjqv',
        'callback_url': 'http://localhost:5000/api/v1/project/bwm3z/waterbutler/logs/',
        'email': 'cyjqv@osf.io',
        'name': 'Raymond Yee'
    }


@pytest.fixture
def credentials():
    return {'token': 'S=XX:U=xxdfdlfjdsklfj:P=185:A=rsdfdfdsfsdfdsfdsf'}

@pytest.fixture
def other_credentials():
    return {'token': 'wrote lord of the rings'}

@pytest.fixture
def settings():
    return {'folder': '14d9a008-f467-443a-9fda-a9ea91960c8b'}

@pytest.fixture
def provider(auth, credentials, settings):
    return EvernoteProvider(auth, credentials, settings)

@pytest.fixture
def other_provider(auth, other_credentials, settings):
    return EvernoteProvider(auth, credentials, settings)


# # content of test_module.py
# import os.path
# def getssh(): # pseudo application code
#     return os.path.join(os.path.expanduser("~admin"), '.ssh')

# def test_mytest(monkeypatch):
#     def mockreturn(path):
#         return '/abc'
#     monkeypatch.setattr(os.path, 'expanduser', mockreturn)
#     x = getssh()
#     assert x == '/abc/.ssh'


# import math

# def myfac(n):
#   if n == 3:
#     return 6
#   else:
#     raise Exception("I just don't know")

# def test_factorial(monkeypatch):
#   monkeypatch.setattr(math, 'factorial', myfac)

#   k = math.factorial(3)
#   assert k == 6

#   with pytest.raises(Exception):
#     math.factorial(2)


@backgroundify
def mock_evernote_note(note_guid, token, withContent=False, withResourcesData=False):
  if note_guid == '71ce96e5-463b-4a72-9fc7-8cdcde7862d4':
    return {'resources': {}, 'guid': '71ce96e5-463b-4a72-9fc7-8cdcde7862d4', 'length': 136, 
    'content': '<?xml version="1.0" encoding="UTF-8"?><!DOCTYPE en-note SYSTEM "http://xml.evernote.com/pub/enml2.dtd"><en-note>hi there<div/></en-note>', 
    'updated': '2015-12-03T00:34:32', 'title': 'another note for OSF Notebook', 'created': '2015-12-03T00:34:28', 
    'content_hash': b'32SDRqNTBI1FyOgPkBMbVQ==', 'notebook_guid': '14d9a008-f467-443a-9fda-a9ea91960c8b'}
  else:
    raise exceptions.WaterButlerError('Note note found', code=400)

@backgroundify
def mock_evernote_notes(notebook_guid, token):
  if notebook_guid == '14d9a008-f467-443a-9fda-a9ea91960c8b':
    return [{'updated': '2017-09-27T14:39:32', 'updateSequenceNum': 125462, 'guid': '14d9a008-f467-443a-9fda-a9ea91960c8b', 
    'title': 'hello/hi, OSF Notebook', 'length': 2271, 'created': '2015-11-26T19:22:58'},
    {'updated': '2017-09-15T23:54:40', 'updateSequenceNum': 124812, 'guid': '3e0e3c1c-c72c-4467-98cc-b2ff93e2239b',
    'title': 'Test 2016.02.11', 'length': 793, 'created': '2016-02-18T05:41:17'}, {'updated': '2015-12-03T00:34:32',
    'updateSequenceNum': 97381, 'guid': '71ce96e5-463b-4a72-9fc7-8cdcde7862d4', 'title': 'another note for OSF Notebook',
    'length': 136, 'created': '2015-12-03T00:34:28'}]
  else: 
    raise exceptions.WaterButlerError('Notebook not found', code=400)

# I need to fill this in
class MockNoteStore(object):
  pass

@backgroundify
def mock_evernote_note_store(token):
  return MockNoteStore()

@pytest.fixture
def mock_evernote(monkeypatch):
  monkeypatch.setattr(evernote_provider, '_evernote_notes', mock_evernote_notes)
  monkeypatch.setattr(evernote_provider, '_evernote_note', mock_evernote_note)
  monkeypatch.setattr(evernote_provider, '_evernote_note_store', mock_evernote_note_store)

class TestValidatePath:

    @pytest.mark.asyncio
    async def test_validate_v1_path_file(self, provider, mock_evernote):

      note_id = '71ce96e5-463b-4a72-9fc7-8cdcde7862d4'

      try:
          wb_path_v1 = await provider.validate_v1_path('/' + note_id)
      except Exception as exc:
          pytest.fail(str(exc))
 
      with pytest.raises(exceptions.NotFoundError) as exc:
          await provider.validate_v1_path('/' + note_id + '/')

      assert exc.value.code == client.NOT_FOUND

      wb_path_v0 = await provider.validate_path('/' + note_id)

      assert wb_path_v1 == wb_path_v0


    @pytest.mark.asyncio
    async def test_validate_path_root(self, provider, mock_evernote):

        path = await provider.validate_path('/')
        assert path.is_dir
        assert len(path.parts) == 1
        assert path.name == ''

    @pytest.mark.asyncio
    async def test_validate_v1_path_root(self, provider, mock_evernote):

        path = await provider.validate_v1_path('/')
        assert path.is_dir
        assert len(path.parts) == 1
        assert path.name == ''

    @pytest.mark.asyncio
    async def test_validate_v1_path_bad_path(self, provider, mock_evernote):

        with pytest.raises(exceptions.NotFoundError) as e:
            await provider.validate_v1_path('/bulbasaur')

        print(e.value.message, e.value.code)
        assert e.value.message == 'Could not retrieve file or directory /bulbasaur'
        assert e.value.code == 404

    @pytest.mark.asyncio
    async def test_validate_path_bad_path(self, provider, mock_evernote):

        with pytest.raises(exceptions.NotFoundError) as e:
            await provider.validate_path('/bulbasaur/charmander')

        print(e.value.message, e.value.code)
        assert e.value.message == 'Could not retrieve file or directory /bulbasaur/charmander'
        assert e.value.code == 404


class TestDownload:

    @pytest.mark.asyncio
    async def test_download(self, provider, mock_evernote):

        note_guid = '71ce96e5-463b-4a72-9fc7-8cdcde7862d4'
        note_name = 'another note for OSF Notebook'

        wbpath = EvernotePath("/" + parse.quote(note_name, safe=''), _ids=('/', note_guid))

        try:
          result = await provider.download(wbpath)
          content = await result.read()
        except Exception as e:
          print (e)
          assert False
        else:
          print(content)
          assert content == b'<div>\n hi there\n <div>\n </div>\n</div>'
        assert True

    # I don't have download(id, revision) signature
    # @pytest.mark.asyncio
    # async def test_download_revision(self, provider, mock_evernote):

    #     note_guid = '71ce96e5-463b-4a72-9fc7-8cdcde7862d4'
    #     note_name = 'another note for OSF Notebook'
    #     revision = 'latest'

    #     wbpath = EvernotePath("/" + parse.quote(note_name, safe=''), _ids=('/', note_guid))

    #     result = await provider.download(wbpath, revision)
    #     content = await result.read()

    #     print(content)

    @pytest.mark.asyncio
    async def test_download_not_found(self, provider, mock_evernote):
        note_name = 'nonexistent note'
        note_guid = 'xxxxxxxxxx'

        wbpath = EvernotePath("/" + parse.quote(note_name, safe=''), _ids=('/', note_guid))

        with pytest.raises(exceptions.DownloadError) as e:
            await provider.download(wbpath)

        assert e.value.code == 500

class TestMetadata:

    @pytest.mark.asyncio
    async def test_must_not_be_none(self, provider, mock_evernote):
        note_name = 'nonexistent note'
        note_guid = 'xxxxxxxxxx'

        wbpath = EvernotePath("/" + parse.quote(note_name, safe=''), _ids=('/', note_guid))

        with pytest.raises(Exception) as e:
            await provider.metadata(wbpath)

        assert e.value.code == 400
        assert e.value.message == 'Note note found'


    @pytest.mark.asyncio
    async def test_download_not_found(self, provider, mock_evernote):

        note_name = 'nonexistent note'
        note_guid = 'xxxxxxxxxx'

        wbpath = EvernotePath("/" + parse.quote(note_name, safe=''), _ids=('/', note_guid))

        # note_md = await _evernote_note(wbpath, token, withContent=False)

        with pytest.raises(exceptions.DownloadError) as e:
            await provider.download(wbpath)

    @pytest.mark.asyncio
    async def test_folder_metadata(self, provider, credentials, mock_evernote):

        notebook_guid = '14d9a008-f467-443a-9fda-a9ea91960c8b'
        notebook_name = 'OSF Notebook'

        token = credentials['token']
        notes = await evernote_provider._evernote_notes(notebook_guid, token)

        notes_md = [EvernoteFileMetadata(note) for note in notes]

        assert len(notes_md) == 3

    @pytest.mark.asyncio
    async def test_root_metadata(self, provider, credentials, mock_evernote):

        wbpath = EvernotePath("/" , _ids=('/',))
        root_md = await provider.metadata(wbpath)

        notebook_guid = '14d9a008-f467-443a-9fda-a9ea91960c8b'
        notebook_name = 'OSF Notebook'

        token = credentials['token']
        notes = await evernote_provider._evernote_notes(notebook_guid, token)

        notes_md = [EvernoteFileMetadata(note) for note in notes]

        assert root_md == notes_md


    @pytest.mark.asyncio
    async def test_metadata(self, provider, credentials, mock_evernote):

        note_guid = '71ce96e5-463b-4a72-9fc7-8cdcde7862d4'
        note_name = 'another note for OSF Notebook'

        wbpath = EvernotePath("/" + parse.quote(note_name, safe=''), _ids=('/', note_guid))
        result = await provider.metadata(wbpath)

        token = credentials['token']

        note = await evernote_provider._evernote_note(note_guid, token, withContent=False)
        note_md = EvernoteFileMetadata(note)

        assert result == note_md

    @pytest.mark.asyncio
    async def test_metadata_bad_response(self, provider, mock_evernote):

        note_name = 'nonexistent note'
        note_guid = 'xxxxxxxxxx'

        wbpath = EvernotePath("/" + parse.quote(note_name, safe=''), _ids=('/', note_guid))

        with pytest.raises(Exception) as e:
            await provider.metadata(wbpath)

        assert e.value.code == 400
        assert e.value.message == 'Note note found'


class TestRevisions:

    @pytest.mark.asyncio
    async def test_get_revisions(self, provider, mock_evernote):

        note_guid = '71ce96e5-463b-4a72-9fc7-8cdcde7862d4'
        note_name = 'another note for OSF Notebook'

        wbpath = EvernotePath("/" + parse.quote(note_name, safe=''), _ids=('/', note_guid))

        revisions = await provider.revisions(wbpath)
        assert len(revisions) == 1


class TestOperations:

    @pytest.mark.asyncio
    async def test_can_duplicate_names(self, provider, mock_evernote):
        assert provider.can_duplicate_names() is False

    @pytest.mark.asyncio
    async def test_can_intra_move(self, provider, mock_evernote):
        assert provider.can_intra_move(provider) is False

    @pytest.mark.asyncio
    async def test_can_intra_copy(self, provider, mock_evernote):
        assert provider.can_intra_copy(provider) is False

    @pytest.mark.asyncio
    async def test_intra_copy(self, provider, other_provider, mock_evernote):
        src_path =  EvernotePath("/a", _ids=('/', '1'))
        dest_path = EvernotePath("/b", _ids=('/', '2'))

        with pytest.raises(exceptions.ReadOnlyProviderError) as e:
            result = await provider.intra_copy(other_provider, src_path, dest_path)
