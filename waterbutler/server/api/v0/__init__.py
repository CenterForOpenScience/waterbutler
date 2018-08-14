import logging

from raven.contrib.tornado import SentryMixin
import tornado.web
import tornado.gen
import tornado.platform.asyncio

from waterbutler.server.utils import CORsMixin


logger = logging.getLogger(__name__)


def list_or_value(value):
    assert isinstance(value, list)
    if len(value) == 0:
        return None
    if len(value) == 1:
        # Remove leading slashes as they break things
        return value[0].decode('utf-8')
    return [item.decode('utf-8') for item in value]


@tornado.web.stream_request_body
class DownloadRedirectHandler(tornado.web.RequestHandler, CORsMixin, SentryMixin):

    async def prepare(self):

        # The only requests we redirect are downloads.
        if self.request.method != 'GET':
            raise tornado.web.HTTPError(status_code=410)

        # Get the query parameters so we can build a redirect url
        self.arguments = {
            key: list_or_value(value)
            for key, value in self.request.query_arguments.items()
        }

        resource = self.arguments['nid']
        provider = self.arguments['provider']
        path = self.arguments['path']
        direct = TRUTH_MAP[self.arguments.get('accept_url', 'true').lower()]
        version = self.arguments.get('version', self.arguments.get('revision', None))

        v1_url = '/v1/resources/{resource}/providers/{provider}/{path}?{direct}{version}'.format(
            resource=resource,
            provider=provider,
            path=path,
            direct='direct=&' if direct else '',
            version='version={}&'.format(version) if version else ''
        )

        logger.info('Redirecting a v0 download request to v1')

        self.redirect(v1_url, permanent=True)


PREFIX = ''
HANDLERS = [
    (r'/file', DownloadRedirectHandler)
]

TRUTH_MAP = {
    'true': True,
    'false': False,
}
