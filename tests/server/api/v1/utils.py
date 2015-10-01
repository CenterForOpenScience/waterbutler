import asyncio
import os
import copy
import shutil
import tempfile
from unittest import mock

from decorator import decorator

import pytest
from tornado import testing
from tornado.platform.asyncio import AsyncIOMainLoop

from waterbutler.core import metadata
from waterbutler.core import provider
from waterbutler.server.app import make_app
from waterbutler.core.path import WaterButlerPath

from tests.utils import MockCoroutine
from tests.utils import MockProvider1


class ServerTestCase(testing.AsyncHTTPTestCase):

    def get_url(self, path):
        return super().get_url(os.path.join('/v1', path.lstrip('/')))

    def get_app(self):
        return make_app(debug=False)

    def get_new_ioloop(self):
        return AsyncIOMainLoop()
