import aiohttpretty
import pytest
from http import client

from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath
#from waterbutler.providers.evernote.provider import EvernoteProvider
#from waterbutler.providers.evernote.metadata import EvernotePackageMetadata
#from waterbutler.providers.evernote.metadata import EvernoteFileMetadata

def test_hello():
  import sys
  print (sys.path)
  try:
    # from waterbutler.providers.evernote.provider import EvernoteProvider
    from evernote.api.client import EvernoteClient
    print('successful import')
  except Exception as e:
    print (e)

  assert False

@pytest.fixture
def auth():
    return {}

