import tornado.gen

from waterbutler.server import utils
from waterbutler.server.api.v0 import core


class ZipHandler(core.BaseProviderHandler):

    ACTION_MAP = {
        'GET': 'download',
    }

    @tornado.gen.coroutine
    def get(self):
        """Download as a Zip archive."""

        self.set_header('Content-Type', 'application/zip')
        self.set_header(
            'Content-Disposition',
            utils.make_disposition(self.path.name + '.zip')
        )

        result = yield from self.provider.zip(**self.arguments)

        yield self.write_stream(result)
