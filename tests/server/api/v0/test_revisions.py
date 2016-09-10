import json
from unittest import mock

from tornado import testing

from tests import utils


class TestRevisionHandler(utils.HandlerTestCase):

    HOOK_PATH = 'waterbutler.server.api.v0.revisions.RevisionHandler._send_hook'

    @testing.gen_test
    def test_get_coro(self):
        expected = [
            utils.MockFileMetadata(),
            utils.MockFolderMetadata()
        ]

        self.mock_provider.revisions = utils.MockCoroutine(return_value=expected)

        resp = yield self.http_client.fetch(
            self.get_url('/revisions?provider=queenhub&path=/brian.tiff'),
        )

        assert {'data': [m.serialized() for m in expected]} == json.loads(resp.body.decode())

    @testing.gen_test
    def test_get_not_coro(self):
        expected = [
            utils.MockFileMetadata(),
            utils.MockFolderMetadata()
        ]

        self.mock_provider.revisions = mock.Mock(return_value=expected)

        resp = yield self.http_client.fetch(
            self.get_url('/revisions?provider=queenhub&path=/brian.tiff'),
        )

        assert {'data': [m.serialized() for m in expected]} == json.loads(resp.body.decode())

    @testing.gen_test
    def test_get_empty(self):
        expected = []

        self.mock_provider.revisions = mock.Mock(return_value=expected)

        resp = yield self.http_client.fetch(
            self.get_url('/revisions?provider=queenhub&path=/brian.tiff'),
        )

        assert {'data': [m.serialized() for m in expected]} == json.loads(resp.body.decode())
