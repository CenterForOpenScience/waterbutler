import json
import time
import logging

import tornado.web
import tornado.gen
import tornado.iostream
from raven.contrib.tornado import SentryMixin

from waterbutler import tasks
from waterbutler.core import utils
from waterbutler.core import signing
from waterbutler.core import exceptions
from waterbutler.server import settings
from waterbutler.server.auth import AuthHandler
from waterbutler.constants import IDENTIFIER_PATHS
from waterbutler.server import utils as server_utils


def list_or_value(value):
    assert isinstance(value, list)
    if len(value) == 0:
        return None
    if len(value) == 1:
        # Remove leading slashes as they break things
        return value[0].decode('utf-8')
    return [item.decode('utf-8') for item in value]


logger = logging.getLogger(__name__)
auth_handler = AuthHandler(settings.AUTH_HANDLERS)
signer = signing.Signer(settings.HMAC_SECRET, settings.HMAC_ALGORITHM)


class BaseHandler(server_utils.CORsMixin, server_utils.UtilMixin, tornado.web.RequestHandler, SentryMixin):
    """Base Handler to inherit from when defining a new view.
    Handles CORs headers, additional status codes, and translating
    :class:`waterbutler.core.exceptions.ProviderError`s into http responses

    .. note::
        For IE compatability passing a ?method=<httpmethod> will cause that request, regardless of the
        actual method, to be interpreted as the specified method.
    """

    ACTION_MAP = {}

    def write_error(self, status_code, exc_info):
        self.captureException(exc_info)
        etype, exc, _ = exc_info

        if issubclass(etype, exceptions.PluginError):
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


class BaseProviderHandler(BaseHandler):

    async def prepare(self):
        self.arguments = {
            key: list_or_value(value)
            for key, value in self.request.query_arguments.items()
        }
        try:
            self.arguments['action'] = self.ACTION_MAP[self.request.method]
        except KeyError:
            return

        self.payload = await auth_handler.fetch(self.request, self.arguments)

        self.provider = utils.make_provider(
            self.arguments['provider'],
            self.payload['auth'],
            self.payload['credentials'],
            self.payload['settings'],
        )

        self.path = await self.provider.validate_path(**self.arguments)
        self.arguments['path'] = self.path  # TODO Not this

    @utils.async_retry(retries=5, backoff=5)
    async def _send_hook(self, action, metadata):
        resp = await utils.send_signed_request('PUT', self.payload['callback_url'], {
            'action': action,
            'metadata': metadata,
            'auth': self.payload['auth'],
            'provider': self.arguments['provider'],
            'time': time.time() + 60
        })
        data = await resp.read()

        if resp.status != 200:
            raise Exception('Callback was unsuccessful, got {}, {}'.format(resp, data.decode('utf-8')))

        logger.info('Successfully sent callback for a {} request'.format(action))
        logger.info('Callback succeeded with {}'.format(data.decode('utf-8')))


class BaseCrossProviderHandler(BaseHandler):
    JSON_REQUIRED = False

    async def prepare(self):
        try:
            self.action = self.ACTION_MAP[self.request.method]
        except KeyError:
            return

        self.source_provider = await self.make_provider(prefix='from', **self.json['source'])
        self.destination_provider = await self.make_provider(prefix='to', **self.json['destination'])

        self.json['source']['path'] = await self.source_provider.validate_path(**self.json['source'])
        self.json['destination']['path'] = await self.destination_provider.validate_path(**self.json['destination'])

    async def make_provider(self, provider, prefix='', **kwargs):
        payload = await auth_handler.fetch(
            self.request,
            dict(kwargs, provider=provider, action=self.action + prefix)
        )
        self.auth = payload
        self.callback_url = payload.pop('callback_url')
        return utils.make_provider(provider, **payload)

    @property
    def json(self):
        try:
            return self._json
        except AttributeError:
            pass
        try:
            self._json = json.loads(self.request.body.decode('utf-8'))
        except ValueError:
            if self.JSON_REQUIRED:
                raise Exception  # TODO
            self._json = None

        return self._json

    @utils.async_retry(retries=0, backoff=5)
    async def _send_hook(self, action, data):
        src_path = None
        if self.source_provider.NAME in IDENTIFIER_PATHS:
            src_path = self.json['source']['path'].identifier_path
        else:
            src_path = '/' + self.json['source']['path'].raw_path

        resp = await utils.send_signed_request('PUT', self.callback_url, {
            'action': action,
            'source': {
                'nid': self.json['source']['nid'],
                'provider': self.source_provider.NAME,
                'path': src_path,
                'name': self.json['source']['path'].name,
                'materialized': str(self.json['source']['path']),
                'kind': self.json['source']['path'].kind,
            },
            'destination': dict(data, nid=self.json['destination']['nid']),
            'auth': self.auth['auth'],
            'time': time.time() + 60
        })
        resp_data = await resp.read()

        if resp.status != 200:
            raise Exception(
                'Callback was unsuccessful, got {}, {}'.format(resp, resp_data.decode('utf-8'))
            )

        logger.info('Successfully sent callback for a {} request'.format(action))
        logger.info('Callback succeeded with {}'.format(resp_data.decode('utf-8')))
