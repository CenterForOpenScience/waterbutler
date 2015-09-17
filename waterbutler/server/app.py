import os
import asyncio

import tornado.web
import tornado.httpserver
import tornado.platform.asyncio

from waterbutler import settings
from waterbutler.server.api import v0
from waterbutler.server.api import v1
from waterbutler.server import handlers
from waterbutler.core.utils import AioSentryClient
from waterbutler.server import settings as server_settings


def api_to_handlers(api):
    return [
        (os.path.join('/', api.PREFIX, pattern.lstrip('/')), handler)
        for (pattern, handler) in api.HANDLERS
    ]


def make_app(debug):
    app = tornado.web.Application(
        api_to_handlers(v0) +
        api_to_handlers(v1) +
        [(r'/status', handlers.StatusHandler)],
        debug=debug,
    )
    app.sentry_client = AioSentryClient(settings.get('SENTRY_DSN', None))
    return app


def serve():
    tornado.platform.asyncio.AsyncIOMainLoop().install()

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

    asyncio.get_event_loop().set_debug(server_settings.DEBUG)
    asyncio.get_event_loop().run_forever()
