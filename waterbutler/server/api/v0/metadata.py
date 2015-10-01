import tornado.gen

from waterbutler.server.api.v0 import core


class MetadataHandler(core.BaseProviderHandler):

    ACTION_MAP = {
        'GET': 'metadata',
    }

    @tornado.gen.coroutine
    def get(self):
        """List information about a file or folder"""
        result = yield from self.provider.metadata(**self.arguments)

        if isinstance(result, list):
            result = [m.serialized() for m in result]
        else:
            result = result.serialized()

        self.write({'data': result})
