import pytest

from waterbutler.core import exceptions
from waterbutler.tasks.core import backgroundify
# from waterbutler.core.path import WaterButlerPath
import waterbutler.providers.evernote.provider as evernote_provider
from waterbutler.providers.evernote.provider import EvernoteProvider

# I need to come back to this when I merge the latest code for WB
# from tests.providers.osfstorage.fixtures import provider as osfstorage_provider

from tests.providers.osfstorage.test_provider import provider as osfstorage_provider

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

