from waterbutler.auth.osf.handler import OsfAuthHandler
from waterbutler.auth.osf import settings
from waterbutler.core.exceptions import AuthError, UnsupportedMethodError
from tests.utils import HandlerTestCase
import pytest
import tornado


class TestOsfAuthHandler(HandlerTestCase):

    def setUp(self):
        super().setUp()
        self.handler = OsfAuthHandler()
        self.request = tornado.httputil.HTTPServerRequest(uri=settings.API_URL)


    @tornado.testing.gen_test
    async def test_supported_methods(self):

        supported_methods = ['put', 'post', 'get', 'head', 'delete']
        assert all(method in self.handler.ACTION_MAP.keys() for method in supported_methods)

        for method in supported_methods:
            self.request.method = method

            with pytest.raises(AuthError):
                await self.handler.get("test", "test", self.request)


    @tornado.testing.gen_test
    async def test_unsupported_methods(self):
        unsupported_methods = ['trace', 'connect', 'patch', 'ma1f0rmed']

        for method in unsupported_methods:
            self.request.method = method
            with pytest.raises(UnsupportedMethodError):
                await self.handler.get("test", "test", self.request)

