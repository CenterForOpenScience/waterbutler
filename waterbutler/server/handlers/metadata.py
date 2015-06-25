import tornado.gen

from waterbutler.server.handlers import core


class MetadataHandler(core.BaseProviderHandler):

    ACTION_MAP = {
        'GET': 'metadata',
    }

    @tornado.gen.coroutine
    def get(self):
        """List information about a file or folder"""
        result = yield from self.provider.metadata(**self.arguments)
        self.write({'data': result})
