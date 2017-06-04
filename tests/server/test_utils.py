import pytest
from unittest import mock

from tornado import testing

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
            method='GET',
            cookies=False,
            headers={}
    ):
        self.method = method
        self.cookies = cookies
        self.headers = headers


@mock.patch('waterbutler.server.settings.CORS_ALLOW_ORIGIN', '')
class TestCORsMixin(ServerTestCase):

    def setUp(self, *args, **kwargs):
        super(TestCORsMixin, self).setUp(*args, **kwargs)
        self.handler = MockHandler()

    @testing.gen_test
    def test_set_default_headers_options(self):
        origin = 'http://foo.com'

        self.handler.request = MockRequest(
            method='OPTIONS',
            headers={
                'Origin': origin,
            }
        )
        self.handler.set_default_headers()
        assert origin == self.handler.headers['Access-Control-Allow-Origin']

    @testing.gen_test
    def test_set_default_headers_options_no_cookie_but_auth_header(self):
        origin = 'http://foo.com'

        self.handler.request = MockRequest(
            method='OPTIONS',
            cookies=False,
            headers={
                'Origin': origin,
                'Authorization': 'fooo'
            }
        )
        self.handler.set_default_headers()
        assert origin == self.handler.headers['Access-Control-Allow-Origin']

    @testing.gen_test
    def test_set_default_headers_options_has_cookie_and_auth_header(self):
        origin = 'http://foo.com'

        self.handler.request = MockRequest(
            method='OPTIONS',
            cookies=True,
            headers={
                'Origin': origin,
                'Authorization': 'fooo'
            }
        )
        self.handler.set_default_headers()
        assert origin == self.handler.headers['Access-Control-Allow-Origin']

    @testing.gen_test
    def test_set_default_headers_options_has_cookie_no_auth_header(self):
        origin = 'http://foo.com'

        self.handler.request = MockRequest(
            method='OPTIONS',
            cookies=True,
            headers={
                'Origin': origin,
            }
        )
        self.handler.set_default_headers()
        assert origin == self.handler.headers['Access-Control-Allow-Origin']

    @testing.gen_test
    def test_set_default_headers_cross_origin_with_cookie(self):
        origin = 'http://foo.com'

        for method in ('HEAD', 'GET', 'POST', 'PUT', 'PATCH', 'DELETE'):
            self.handler.request = MockRequest(
                method=method,
                cookies=True,
                headers={
                    'Origin': origin,
                }
            )
            self.handler.set_default_headers()
            assert origin not in self.handler.headers['Access-Control-Allow-Origin']

    @testing.gen_test
    def test_set_default_headers_cross_origin_no_cookie_no_auth_header(self):
        origin = 'http://foo.com'

        for method in ('HEAD', 'GET', 'POST', 'PUT', 'PATCH', 'DELETE'):
            self.handler.request = MockRequest(
                method=method,
                cookies=False,
                headers={
                    'Origin': origin,
                }

            )
            self.handler.set_default_headers()
            assert origin not in self.handler.headers['Access-Control-Allow-Origin']

    @testing.gen_test
    def test_set_default_headers_cross_origin_has_cookie_and_auth_header(self):
        origin = 'http://foo.com'

        for method in ('HEAD', 'GET', 'POST', 'PUT', 'PATCH', 'DELETE'):
            self.handler.request = MockRequest(
                method=method,
                cookies=True,
                headers={
                    'Origin': origin,
                    'Authorization': 'asdlisdfgluiwgqruf'
                }
            )
            self.handler.set_default_headers()
            assert origin not in self.handler.headers['Access-Control-Allow-Origin']

    @testing.gen_test
    def test_set_default_headers_cross_origin_no_cookie_but_auth_header(self):
        origin = 'http://foo.com'

        for method in ('HEAD', 'GET', 'POST', 'PUT', 'PATCH', 'DELETE'):
            self.handler.request = MockRequest(
                method=method,
                cookies=False,
                headers={
                    'Origin': origin,
                    'Authorization': 'asdlisdfgluiwgqruf'
                }
            )
            self.handler.set_default_headers()
            assert origin in self.handler.headers['Access-Control-Allow-Origin']

    @testing.gen_test
    def test_no_origin_means_no_cors(self):
        for method in ('OPTIONS', 'HEAD' 'GET', 'POST', 'PUT', 'PATCH', 'DELETE'):
            self.handler.request = MockRequest(
                method=method,
            )
            self.handler.set_default_headers()
            assert 'Access-Control-Allow-Origin' not in self.handler.headers
            assert 'Access-Control-Allow-Credentials' not in self.handler.headers
            assert 'Access-Control-Allow-Headers' not in self.handler.headers
            assert 'Access-Control-Expose-Headers' not in self.handler.headers
