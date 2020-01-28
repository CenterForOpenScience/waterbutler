from http import client
from unittest import mock

import pytest

from aiohttp import ClientError

from tornado import gen
from tornado import testing
from tornado import httpclient

from tests import utils
from tests.server.api.v1.utils import ServerTestCase


class TestServerFuzzing(ServerTestCase):

    @testing.gen_test
    def test_head_no_auth_server(self):
        with mock.patch('waterbutler.auth.osf.handler.aiohttp.request') as mock_auth:
            mock_auth.side_effect = ClientError

            with pytest.raises(httpclient.HTTPError) as exc:
                yield self.http_client.fetch(
                    self.get_url('/resources//providers//'),
                    method='HEAD'
                )
            assert exc.value.code == client.NOT_FOUND

            with pytest.raises(httpclient.HTTPError) as exc:
                yield self.http_client.fetch(
                    self.get_url('/resources/jaaaaaaank/providers//'),
                    method='HEAD'
                )
            assert exc.value.code == client.NOT_FOUND

            with pytest.raises(httpclient.HTTPError) as exc:
                yield self.http_client.fetch(
                    self.get_url('/resources//providers/jaaaaank/'),
                    method='HEAD'
                )
            assert exc.value.code == client.NOT_FOUND

            with pytest.raises(httpclient.HTTPError) as exc:
                yield self.http_client.fetch(
                    self.get_url('/resources/jernk/providers/jaaaaank/'),
                    method='HEAD'
                )

            assert exc.value.code == client.SERVICE_UNAVAILABLE

    @testing.gen_test
    def test_movecopy_requires_contentlength(self):
        with pytest.raises(httpclient.HTTPError) as exc:
            yield self.http_client.fetch(
                self.get_url('/resources/jernk/providers/jaaaaank/'),
                method='POST', allow_nonstandard_methods=True
            )
        assert exc.value.code == client.LENGTH_REQUIRED
        # Make sure the message returned is correct

    @testing.gen_test
    def test_large_body_copy_rejected(self):
        @gen.coroutine
        def body_producer(write):
            x, total = 0, 1048580
            msg = b'a' * 10000
            while x < total:
                if x + len(msg) > total:
                    msg = b'a' * (total - x)
                x += len(msg)
                yield write(msg)

        with pytest.raises(httpclient.HTTPError):
            yield self.http_client.fetch(
                self.get_url('/resources/jernk/providers/jaaaaank/'),
                headers={'Content-Length': '1048580'},
                method='POST', body_producer=body_producer,
            )
        # Maybe a bug in tornado?
        # Responses not properly sent back
        # assert exc.value.code == 413

    @testing.gen_test
    def test_options(self):
        resp = yield self.http_client.fetch(
            self.get_url('/resources/jernk/providers/jaaaaank/'),
            method='OPTIONS',
        )
        assert resp.code == client.NO_CONTENT

        with pytest.raises(httpclient.HTTPError) as exc:
            yield self.http_client.fetch(
                self.get_url('/reders/jaaaaank/'),
                method='OPTIONS',
            )
        assert exc.value.code == client.NOT_FOUND


class TestServerFuzzingMocks(ServerTestCase):

    def setUp(self):
        super().setUp()
        self.mock_auth = utils.MockCoroutine(return_value={'auth': {}, 'settings': {}, 'credentials': {}})
        self.mock_provider = mock.Mock(return_value=utils.MockProvider1({}, {}, {}))

        self.mock_auth_patcher = mock.patch('waterbutler.server.api.v1.provider.auth_handler.get', self.mock_auth)
        self.mock_provider_patcher = mock.patch('waterbutler.server.api.v1.provider.utils.make_provider', self.mock_provider)
        self.mock_auth_patcher.start()
        self.mock_provider_patcher.start()

    def tearDown(self):
        super().tearDown()
        self.mock_auth_patcher.stop()
        self.mock_provider_patcher.stop()

    @testing.gen_test
    def test_head(self):
        self.mock_provider_patcher.stop()
        with pytest.raises(httpclient.HTTPError) as exc:
            yield self.http_client.fetch(
                self.get_url('/resources/jernk/providers/jaaaaank/'),
                method='HEAD'
            )
        assert exc.value.code == client.NOT_FOUND
        # Dont have access to the body here?
        # assert exc.value.message == 'Provider "jaaaaank" not found'
        self.mock_provider_patcher.start()  # Has to be started to stop...

    @testing.gen_test
    def test_head_with_folder(self):
        with pytest.raises(httpclient.HTTPError) as exc:
            yield self.http_client.fetch(
                self.get_url('/resources/jernk/providers/jaaaaank/'),
                method='HEAD'
            )

        assert exc.value.code == client.NOT_IMPLEMENTED
