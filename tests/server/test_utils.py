from unittest import mock, TestCase

import pytest

from waterbutler.server.utils import CORsMixin, parse_request_range


class MockHandler(CORsMixin):

    request = None
    headers = {}

    def set_header(self, key, value):
        self.headers[key] = value


class MockRequest:

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
class TestCORsMixin(TestCase):

    def setUp(self, *args, **kwargs):
        super().setUp(*args, **kwargs)
        self.handler = MockHandler()

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


class TestRangeParsing():

    @pytest.mark.parametrize("range_header,expected", [
        ('bytes=0-1',      (0, 1)),
        ('bytes=28-45',    (28, 45)),
        ('bytes=2-2',      (2, 2)),
        ('bytes=0-',       (0, None)),
        ('bytes=6-',       (6, None)),
        ('bytes=',         None),
        ('foo=42',         None),
        ('bytes=1-2,6-10', None),
        ('bytes=-6',       None),
        ('bytes=-0',       None),
        ('bytes=6-1',      None),
        ('bytes=-3-5',     None),
    ])
    def test_range_parsing(self, range_header, expected):
        result = parse_request_range(range_header)
        assert result == expected

