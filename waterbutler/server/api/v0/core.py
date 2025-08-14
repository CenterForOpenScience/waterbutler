import json
import logging

import tornado.web
import tornado.gen
import tornado.iostream

import sentry_sdk

from waterbutler import tasks
from waterbutler.core import utils
from waterbutler.core import signing
from waterbutler.core import exceptions
from waterbutler.server import settings
from waterbutler.core import remote_logging
from waterbutler.server.auth import AuthHandler
from waterbutler.core.log_payload import LogPayload
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


class BaseHandler(server_utils.CORsMixin, server_utils.UtilMixin, tornado.web.RequestHandler):
    """Base Handler to inherit from when defining a new view.
    Handles CORs headers, additional status codes, and translating
    :class:`waterbutler.core.exceptions.ProviderError`s into http responses

    .. note::
        For IE compatability passing a ?method=<httpmethod> will cause that request, regardless of the
        actual method, to be interpreted as the specified method.
    """

    ACTION_MAP = {}  # type: dict

    def write_error(self, status_code, exc_info):
        sentry_sdk.capture_exception(exc_info)
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

    def _send_hook(self, action, metadata=None, path=None):
        source = LogPayload(self.arguments['nid'], self.provider, metadata=metadata, path=path)
        remote_logging.log_file_action(action, source=source, api_version='v0',
                                       request=remote_logging._serialize_request(self.request),
                                       bytes_downloaded=self.bytes_downloaded,
                                       bytes_uploaded=self.bytes_uploaded)


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

    def _send_hook(self, action, metadata):
        source = LogPayload(self.json['source']['nid'], self.source_provider,
                            path=self.json['source']['path'])
        destination = LogPayload(self.json['destination']['nid'], self.destination_provider,
                                 metadata=metadata)
        remote_logging.log_file_action(action, source=source, destination=destination, api_version='v0',
                                       request=remote_logging._serialize_request(self.request),
                                       bytes_downloaded=self.bytes_downloaded,
                                       bytes_uploaded=self.bytes_uploaded)
