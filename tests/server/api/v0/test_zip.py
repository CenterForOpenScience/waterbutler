import io
import zipfile

from tornado import testing

from waterbutler.core import streams
from waterbutler.core.utils import AsyncIterator

from tests import utils


class TestZipHandler(utils.HandlerTestCase):

    HOOK_PATH = 'waterbutler.server.api.v0.zip.ZipHandler._send_hook'

    @testing.gen_test
    def test_download_stream(self):
        data = b'freddie brian john roger'
        stream = streams.StringStream(data)
        stream.content_type = 'application/octet-stream'

        zipstream = streams.ZipStreamReader(AsyncIterator([('file.txt', stream)]))

        self.mock_provider.zip = utils.MockCoroutine(return_value=zipstream)

        resp = yield self.http_client.fetch(
            self.get_url('/zip?provider=queenhub&path=/freddie.png'),
        )

        zip = zipfile.ZipFile(io.BytesIO(resp.body))

        assert zip.testzip() is None

        assert zip.open('file.txt').read() == data
