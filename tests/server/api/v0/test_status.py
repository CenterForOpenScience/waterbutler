import json
from http import HTTPStatus

from tornado import testing

from tests import utils
from waterbutler.version import __version__


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
        assert resp.code == HTTPStatus.OK
        assert expected == json.loads(resp.body.decode())
