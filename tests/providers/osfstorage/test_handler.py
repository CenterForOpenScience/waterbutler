from waterbutler.auth.osf.handler import OsfAuthHandler
from waterbutler.server.settings import DOMAIN
import unittest
import requests


class TestOsfAuthHandler(unittest.TestCase):

    def setUp(self):
        self.handler = OsfAuthHandler

    def test_supported_methods(self):

        supported_methods = ['put', 'post', 'get', 'head', 'delete']
        assert list(self.handler.ACTION_MAP.keys()) == supported_methods

        for method in supported_methods:
            assert requests.request(method, DOMAIN + '/v1/resources/test/providers/test/').status_code == 404

        unsupported_methods = ['trace', 'connect', 'patch', 'ma1f0rmed']
        for method in unsupported_methods:
            assert requests.request(method, DOMAIN + '/v1/resources/test/providers/test/').status_code == 405

