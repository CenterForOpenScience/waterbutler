from waterbutler.server.api.v0 import core
from waterbutler.core.utils import make_disposition


class ZipHandler(core.BaseProviderHandler):

    ACTION_MAP = {
        'GET': 'download',
    }

    async def get(self):
        """Download as a Zip archive."""

        zipfile_name = self.path.name or '{}-archive'.format(self.provider.NAME)
        self.set_header('Content-Type', 'application/zip')
        self.set_header(
            'Content-Disposition',
            make_disposition(zipfile_name + '.zip')
        )

        result = await self.provider.zip(**self.arguments)

        await self.write_stream(result)
        self._send_hook('download_zip', path=self.path)
