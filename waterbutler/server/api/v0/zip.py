from waterbutler.server import utils
from waterbutler.server.api.v0 import core


class ZipHandler(core.BaseProviderHandler):

    ACTION_MAP = {
        'GET': 'download',
    }

    async def get(self):
        """Download as a Zip archive."""

        self.set_header('Content-Type', 'application/zip')
        self.set_header(
            'Content-Disposition',
            utils.make_disposition(self.path.name + '.zip')
        )

        result = await self.provider.zip(**self.arguments)

        await self.write_stream(result)
