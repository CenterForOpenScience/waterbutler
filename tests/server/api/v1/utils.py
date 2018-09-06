import os
import asyncio

from tornado import testing
from tornado.platform.asyncio import AsyncIOMainLoop

from waterbutler.server.app import make_app


class ServerTestCase(testing.AsyncHTTPTestCase):

    def setUp(self):
        policy = asyncio.get_event_loop_policy()
        policy.get_event_loop().close()
        self.event_loop = policy.new_event_loop()
        policy.set_event_loop(self.event_loop)
        super().setUp()

    def tearDown(self):
        super().tearDown()
        self.event_loop.close()

    def get_url(self, path):
        return super().get_url(os.path.join('/v1', path.lstrip('/')))

    def get_app(self):
        return make_app(debug=False)

    def get_new_ioloop(self):
        return AsyncIOMainLoop()
