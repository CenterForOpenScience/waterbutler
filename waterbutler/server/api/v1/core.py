import logging

import tornado.web
import tornado.gen
import tornado.iostream
from raven.contrib.tornado import SentryMixin

from waterbutler import tasks
from waterbutler.server import utils
from waterbutler.core import exceptions

logger = logging.getLogger(__name__)


class BaseHandler(utils.CORsMixin, utils.UtilMixin, tornado.web.RequestHandler, SentryMixin):

    @classmethod
    def as_entry(cls):
        return (cls.PATTERN, cls)

    def write_error(self, status_code, exc_info):
        etype, exc, _ = exc_info

        exception_kwargs, finish_args = {}, []
        if issubclass(etype, exceptions.WaterButlerError):
            self.set_status(int(exc.code))

            # If the exception has a `data` property then we need to handle that with care
            # Th expectation is that we need to return a structured response.  For now, assume that
            # involves setting the response headers to the value of the `headers` attribute of the
            # `data`, while also serializing the entire `data` data structure.
            if exc.data:
                self.set_header('Content-Type', 'application/json')
                headers = exc.data.get('headers', None)
                if headers:
                    for key, value in headers.items():
                        self.set_header(key, value)
                self.write(exc.data)
                finish_args = [exc.data]
            else:
                finish_args = [{'code': exc.code, 'message': exc.message}]
                self.write(exc.message)
            exception_kwargs = {'data': {'level': 'info'}} if exc.is_user_error else {}
        elif issubclass(etype, tasks.WaitTimeOutError):
            self.set_status(202)
            exception_kwargs = {'data': {'level': 'info'}}
        else:
            finish_args = [{'code': status_code, 'message': self._reason}]

        self.captureException(exc_info, **exception_kwargs)
        self.finish(*finish_args)

    # avoid dumping duplicate information to application log
    def log_exception(self, typ, value, tb):
        if isinstance(value, tornado.web.HTTPError):
            if value.log_message:
                format = "%d %s: " + value.log_message
                args = ([value.status_code, self._request_summary()] +
                        list(value.args))
                tornado.web.gen_log.warning(format, *args)
        else:
            tornado.web.app_log.error("Uncaught exception %s\n", self._request_summary(),
                                      exc_info=(typ, value, tb))
