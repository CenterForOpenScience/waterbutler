from waterbutler.server.api.v0 import core


class MetadataHandler(core.BaseProviderHandler):

    ACTION_MAP = {
        'GET': 'metadata',
    }

    async def get(self):
        """List information about a file or folder"""
        result = await self.provider.metadata(**self.arguments)

        if isinstance(result, list):
            result = [m.serialized() for m in result]
        else:
            result = result.serialized()

        self.write({'data': result})
