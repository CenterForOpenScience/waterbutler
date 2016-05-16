import pytest
from http import client
from unittest import mock
import functools

import aiohttp

from tornado import gen
from tornado import testing
from tornado import httpclient

from tests import utils
from tests.server.api.v1.utils import ServerTestCase

from waterbutler.server.utils import CORsMixin

class MockHandler(CORsMixin):

    request = None
    headers = {}

    def set_header(self, key, value):
        self.headers[key] = value

class MockRequest(object):

    def __init__(
            self,
            origin=None,
            method='GET',
            cookies=None,
            headers=None
    ):
        self.origin = origin or ''
        self.method = method
        self.cookies = cookies or True
        self.headers = headers or {
            'Origin': origin
        }

class TestCORsMixin(ServerTestCase):

    def setUp(self, *args, **kwargs):
        super(TestCORsMixin, self).setUp(*args, **kwargs)
        self.handler = MockHandler()

    @testing.gen_test
    def test_set_default_headers_options(self):
        origin = 'http://foo.com'

        self.handler.request = MockRequest(
            origin=origin,
            method='OPTIONS'
        )
        self.handler.set_default_headers()
        assert origin == self.handler.headers['Access-Control-Allow-Origin']

    @testing.gen_test
    def test_set_default_headers_cross_origin_with_cookie(self):
        origin = 'http://foo.com'

        for method in ('GET', 'POST', 'PUT', 'PATCH', 'DELETE'):
            self.handler.request = MockRequest(
                origin=origin,
                method='GET',
                cookies=True
            )
            self.handler.set_default_headers()
            assert origin not in self.handler.headers['Access-Control-Allow-Origin']

    @testing.gen_test
    def test_set_default_headers_cross_origin_no_cookie_no_auth_header(self):
        origin = 'http://foo.com'

        for method in ('GET', 'POST', 'PUT', 'PATCH', 'DELETE'):
            self.handler.request = MockRequest(
                origin=origin,
                method='GET',
                cookies=False,
                headers=None
            )
            self.handler.set_default_headers()
            assert origin not in self.handler.headers['Access-Control-Allow-Origin']

    @testing.gen_test
    def test_set_default_headers_cross_origin_no_cookie_but_auth_header(self):
        origin = 'http://foo.com'

        for method in ('GET', 'POST', 'PUT', 'PATCH', 'DELETE'):
            self.handler.request = MockRequest(
                origin=origin,
                method='GET',
                cookies=False,
                headers={
                    'Authorization': 'asdlisdfgluiwgqruf'
                }
            )
            self.handler.set_default_headers()
            assert origin not in self.handler.headers['Access-Control-Allow-Origin']
