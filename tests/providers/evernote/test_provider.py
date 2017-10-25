import aiohttpretty
import pytest
from http import client
from unittest import mock

from waterbutler.core import exceptions
from waterbutler.tasks.core import backgroundify
from waterbutler.core.path import WaterButlerPath
import waterbutler.providers.evernote.provider
from waterbutler.providers.evernote.provider import (EvernoteProvider, _evernote_notes, _evernote_note)
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
    return {'folder': '014e8f86-110e-4745-bfee-4b21d8c6b51d'}

@pytest.fixture
def provider(auth, credentials, settings):
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
    return {'updated': '2015-12-03T00:34:32', 'notebook_guid': '014e8f86-110e-4745-bfee-4b21d8c6b51d', 
    'guid': '71ce96e5-463b-4a72-9fc7-8cdcde7862d4', 'title': 'another note for OSF Notebook', 'content_hash': b'32SDRqNTBI1FyOgPkBMbVQ==',
     'content': None, 'length': 136, 'created': '2015-12-03T00:34:28', 'resources': {}}
  else:
    return {}

@backgroundify
def mock_evernote_notes(notebook_guid, token):
  return [{'updated': '2017-09-27T14:39:32', 'updateSequenceNum': 125462, 'guid': '14d9a008-f467-443a-9fda-a9ea91960c8b', 
  'title': 'hello/hi, OSF Notebook', 'length': 2271, 'created': '2015-11-26T19:22:58'}, 
  {'updated': '2017-09-15T23:54:40', 'updateSequenceNum': 124812, 'guid': '3e0e3c1c-c72c-4467-98cc-b2ff93e2239b', 
  'title': 'Test 2016.02.11', 'length': 793, 'created': '2016-02-18T05:41:17'}, {'updated': '2015-12-03T00:34:32', 
  'updateSequenceNum': 97381, 'guid': '71ce96e5-463b-4a72-9fc7-8cdcde7862d4', 'title': 'another note for OSF Notebook', 
  'length': 136, 'created': '2015-12-03T00:34:28'}]

# @pytest.mark.asyncio
# async def test_mock_evernote_functions():
#   k = await mock_evernote_note('71ce96e5-463b-4a72-9fc7-8cdcde7862d4', 'xxxxx')
#   assert k['title'] == 'another note for OSF Notebook'

@pytest.fixture
def mock_evernote(monkeypatch):
  monkeypatch.setattr(waterbutler.providers.evernote.provider, '_evernote_notes', mock_evernote_notes)
  monkeypatch.setattr(waterbutler.providers.evernote.provider, '_evernote_note', mock_evernote_note)


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

