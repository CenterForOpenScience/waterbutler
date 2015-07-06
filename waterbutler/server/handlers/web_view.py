import tornado.gen

from waterbutler.server.handlers import core


class WebViewHandler(core.BaseProviderHandler):

    ACTION_MAP = {
        'GET': 'webview',
    }

    @tornado.gen.coroutine
    def get(self):
        """Get or create external link for a file"""
        result = yield from self.provider.web_view_link(**self.arguments)
        self.write({'data': result})
