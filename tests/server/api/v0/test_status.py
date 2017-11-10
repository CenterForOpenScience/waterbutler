import json

from tornado import testing

from version import __version__

from tests import utils


class TestStatusHandler(utils.HandlerTestCase):

    @testing.gen_test
    def test_get_coro(self):
        expected = {
            'status': 'up',
            'version': __version__,
        }
        resp = yield self.http_client.fetch(
            self.get_url('/status'),
        )
        assert resp.code == 200
        assert expected == json.loads(resp.body.decode())
