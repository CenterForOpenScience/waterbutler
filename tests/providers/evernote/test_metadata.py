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


class TestBoxMetadata:

    def test_file_metadata(self):
    	assert True
