import os
import signal
import asyncio
import logging
from functools import partial

import tornado.web
import tornado.platform.asyncio

import sentry_sdk
from sentry_sdk.integrations.tornado import TornadoIntegration
from sentry_sdk.integrations.logging import LoggingIntegration

from waterbutler import settings
from waterbutler.server.api import v0
from waterbutler.server.api import v1
from waterbutler.server import handlers
from waterbutler.version import __version__
from waterbutler.server import settings as server_settings

logger = logging.getLogger(__name__)


def sig_handler(sig, frame):
    io_loop = asyncio.get_event_loop()

    def stop_loop():
        if len(asyncio.Task.all_tasks(io_loop)) == 0:
            io_loop.stop()
        else:
            io_loop.call_later(1, stop_loop)

    io_loop.add_callback_from_signal(stop_loop)


def api_to_handlers(api):
    return [
        (os.path.join('/', api.PREFIX, pattern.lstrip('/')), handler)
        for (pattern, handler) in api.HANDLERS
    ]


def make_app(debug):

    sentry_logging = LoggingIntegration(
        level=logging.INFO,  # Capture INFO level and above as breadcrumbs
        event_level=None,   # Do not send logs of any level as events
    )
    sentry_sdk.init(
        dsn=settings.SENTRY_DSN,
        release=__version__,
        integrations=[TornadoIntegration(), sentry_logging, ],
    )

    app = tornado.web.Application(
        api_to_handlers(v0) +
        api_to_handlers(v1) +
        [(r'/status', handlers.StatusHandler)],
        debug=debug,
        autoreload=False,
    )
    return app


def serve():
    app = make_app(server_settings.DEBUG)

    ssl_options = None
    if server_settings.SSL_CERT_FILE and server_settings.SSL_KEY_FILE:
        ssl_options = {
            'certfile': server_settings.SSL_CERT_FILE,
            'keyfile': server_settings.SSL_KEY_FILE,
        }

    app.listen(
        server_settings.PORT,
        address=server_settings.ADDRESS,
        xheaders=server_settings.XHEADERS,
        max_body_size=server_settings.MAX_BODY_SIZE,
        ssl_options=ssl_options,
    )

    logger.info("Listening on {0}:{1}".format(server_settings.ADDRESS, server_settings.PORT))

    signal.signal(signal.SIGTERM, partial(sig_handler))
    asyncio.get_event_loop().set_debug(server_settings.DEBUG)
    asyncio.get_event_loop().run_forever()
