import tornado.web
import tornado.gen
import tornado.iostream
from raven.contrib.tornado import SentryMixin

from waterbutler import tasks
from waterbutler.server import utils
from waterbutler.core import exceptions


class BaseHandler(utils.CORsMixin, utils.UtilMixin, tornado.web.RequestHandler, SentryMixin):

    @classmethod
    def as_entry(cls):
        return (cls.PATTERN, cls)

    def write_error(self, status_code, exc_info):
        self.captureException(exc_info)
        etype, exc, _ = exc_info

        if issubclass(etype, exceptions.WaterButlerError):
            self.set_status(int(exc.code))
            if exc.data:
                self.finish(exc.data)
            else:
                self.finish({
                    'code': exc.code,
                    'message': exc.message
                })

        elif issubclass(etype, tasks.WaitTimeOutError):
            # TODO
            self.set_status(202)
        else:
            self.finish({
                'code': status_code,
                'message': self._reason,
            })
