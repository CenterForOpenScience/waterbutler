import tornado.web

from waterbutler.version import __version__


class StatusHandler(tornado.web.RequestHandler):

    def get(self):
        """List information about waterbutler status"""
        self.write({
            'status': 'up',
            'version': __version__
        })
