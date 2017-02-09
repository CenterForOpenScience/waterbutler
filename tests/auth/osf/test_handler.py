from unittest import mock

import pytest
import tornado

from tests import utils
from tests.server.api.v1.utils import ServerTestCase

from waterbutler.auth.osf import settings
from waterbutler.auth.osf.handler import OsfAuthHandler
from waterbutler.core.exceptions import UnsupportedHTTPMethodError


class TestOsfAuthHandler(ServerTestCase):

    def setUp(self):
        super().setUp()

        self.handler = OsfAuthHandler()
        self.request = tornado.httputil.HTTPServerRequest(uri=settings.API_URL)

    @tornado.testing.gen_test
    async def test_supported_and_unsupported_methods(self):

        mock_auth = utils.MockCoroutine(return_value={'auth': {}, 'callback_url': 'test.com'})
        mock_auth_patcher = mock.patch('waterbutler.auth.osf.handler.OsfAuthHandler.make_request', mock_auth)
        mock_auth_patcher.start()

        supported_methods = ['put', 'post', 'get', 'head', 'delete']
        unsupported_methods = ['trace', 'connect', 'patch', 'ma1f0rmed']

        assert all(method in self.handler.ACTION_MAP.keys() for method in supported_methods)

        for method in supported_methods:
            self.request.method = method
            await self.handler.get("test", "test", self.request)

        for method in unsupported_methods:
            self.request.method = method
            with pytest.raises(UnsupportedHTTPMethodError):
                await self.handler.get("test", "test", self.request)

        mock_auth_patcher.stop()
