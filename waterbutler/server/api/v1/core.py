import tornado.web
import tornado.gen
import tornado.iostream
from raven.contrib.tornado import SentryMixin

from waterbutler import tasks
from waterbutler.server import utils, settings
from waterbutler.core import exceptions


class BaseHandler(utils.CORsMixin, utils.UtilMixin, tornado.web.RequestHandler, SentryMixin):

    @classmethod
    def as_entry(cls):
        return (cls.PATTERN, cls)

    def write_error(self, status_code, exc_info):
        etype, exc, _ = exc_info

        exception_kwargs, finish_args = {}, []
        if issubclass(etype, exceptions.WaterButlerError):
            self.set_status(int(exc.code))
            exception_kwargs = {'data': {'level': 'info'}} if exc.is_user_error else {}
            finish_args = [exc.data] if exc.data else [{'code': exc.code, 'message': exc.message}]
        elif issubclass(etype, tasks.WaitTimeOutError):
            self.set_status(202)
            pending_url = '{}/pending/{}/{}'.format(settings.DOMAIN, exc.args[1], exc.args[0])
            self.add_header('Content-Location', pending_url)
            exception_kwargs = {'data': {'level': 'info'}}
            finish_args = [{'data': {'attributes': {'status': 'Accepted',
                                                    'id': exc.args[0],
                                                    'resource': exc.args[1]
                                                    },
                                     'links': {'status': pending_url}
                                     }
                            }]
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
