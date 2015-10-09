import http
import time
import socket
import asyncio
import logging

import tornado.gen

from waterbutler.core import utils
from waterbutler.server import settings
from waterbutler.server.api.v1 import core
from waterbutler.server.auth import AuthHandler
from waterbutler.core.streams import RequestStreamReader
from waterbutler.server.api.v1.provider.create import CreateMixin
from waterbutler.server.api.v1.provider.metadata import MetadataMixin
from waterbutler.server.api.v1.provider.movecopy import MoveCopyMixin


logger = logging.getLogger(__name__)
auth_handler = AuthHandler(settings.AUTH_HANDLERS)

IDENTIFIER_PATHS = ('box', 'osfstorage')


@tornado.web.stream_request_body
class ProviderHandler(core.BaseHandler, CreateMixin, MetadataMixin, MoveCopyMixin):
    VALIDATORS = {'put': 'validate_put', 'post': 'validate_post'}
    PATTERN = r'/resources/(?P<resource>(?:\w|\d)+)/providers/(?P<provider>(?:\w|\d)+)(?P<path>/.*/?)'

    @tornado.gen.coroutine
    def prepare(self, *args, **kwargs):
        # TODO Find a nicer way to handle this
        if self.request.method.lower() == 'options':
            return

        self.path = self.path_kwargs['path'] or '/'
        provider = self.path_kwargs['provider']
        self.resource = self.path_kwargs['resource']

        if self.request.method.lower() in self.VALIDATORS:
            # create must validate before accepting files
            getattr(self, self.VALIDATORS[self.request.method.lower()])()

        self.auth = yield from auth_handler.get(self.resource, provider, self.request)
        self.provider = utils.make_provider(provider, self.auth['auth'], self.auth['credentials'], self.auth['settings'])
        self.path = yield from self.provider.validate_path(self.path)

        # The one special case
        if self.request.method == 'PUT' and self.path.is_file:
            yield from self.prepare_stream()
        else:
            self.stream = None
        self.body = b''

    @tornado.gen.coroutine
    def head(self, **_):
        """Get metadata for a folder or file
        """
        if self.path.is_dir:
            return self.set_status(http.client.NOT_IMPLEMENTED)  # Metadata on the folder itself TODO
        return (yield from self.header_file_metadata())

    @tornado.gen.coroutine
    def get(self, **_):
        """Download a file
        Will redirect to a signed URL if possible and accept_url is not False
        :raises: MustBeFileError if path is not a file
        """
        if self.path.is_dir:
            return (yield from self.get_folder())
        return (yield from self.get_file())

    @tornado.gen.coroutine
    def put(self, **_):
        """Defined in CreateMixin"""
        if self.path.is_file:
            return (yield from self.upload_file())
        return (yield from self.create_folder())

    @tornado.gen.coroutine
    def post(self, **_):
        return (yield from self.move_or_copy())

    @tornado.gen.coroutine
    def delete(self, **_):
        yield from self.provider.delete(self.path)
        self.set_status(http.client.NO_CONTENT)

    @tornado.gen.coroutine
    def data_received(self, chunk):
        """Note: Only called during uploads."""
        if self.stream:
            self.writer.write(chunk)
            yield from self.writer.drain()
        else:
            self.body += chunk

    @asyncio.coroutine
    def prepare_stream(self):
        """Sets up an asyncio pipe from client to server
        Only called on PUT when path is to a file
        """
        self.rsock, self.wsock = socket.socketpair()

        self.reader, _ = yield from asyncio.open_unix_connection(sock=self.rsock)
        _, self.writer = yield from asyncio.open_unix_connection(sock=self.wsock)

        self.stream = RequestStreamReader(self.request, self.reader)
        self.uploader = asyncio.async(self.provider.upload(self.stream, self.path))

    def on_finish(self):
        status, method = self.get_status(), self.request.method.upper()
        # If the response code is not within the 200 range,
        # the request was a GET, HEAD, or OPTIONS,
        # or the response code is 202, celery will send its own callback
        # no callbacks should be sent.
        if any((method in ('GET', 'HEAD', 'OPTIONS'), status == 202, status // 100 != 2)):
            return

        # Done here just because method is defined
        action = {
            'PUT': lambda: ('create' if self.path.is_file else 'create_folder') if status == 201 else 'update',
            'POST': lambda: 'move' if self.json['action'] == 'rename' else self.json['action'],
            'DELETE': lambda: 'delete'
        }[method]()

        self._send_hook(action)

    @utils.async_retry(retries=5, backoff=5)
    def _send_hook(self, action):
        payload = {
            'action': action,
            'time': time.time() + 60,
            'auth': self.auth['auth'],
        }

        if action in ('move', 'copy'):
            payload.update({
                'source': {
                    'nid': self.resource,
                    'kind': self.path.kind,
                    'name': self.path.name,
                    'path': self.path.identifier_path if self.provider.NAME in IDENTIFIER_PATHS else self.path.path,
                    'provider': self.provider.NAME,  # TODO rename to name
                    'materialized': str(self.path),
                },
                'destination': {
                    'nid': self.dest_resource,
                    'kind': self.dest_path.kind,
                    'name': self.dest_path.name,
                    'path': self.dest_path.identifier_path if self.dest_provider.NAME in IDENTIFIER_PATHS else self.dest_path.path,
                    'provider': self.dest_provider.NAME,
                    'materialized': str(self.dest_path),
                }
            })
        else:
            # This is adequate for everything but github
            # If extra can be included it will link to the given sha
            payload.update({
                'metadata': {
                    # Hack: OSF and box use identifiers to refer to files
                    'path': self.path.identifier_path if self.provider.NAME in IDENTIFIER_PATHS else self.path.path,
                    'name': self.path.name,
                    'materialized': str(self.path),
                }
            })

        resp = (yield from utils.send_signed_request('PUT', self.auth['callback_url'], payload))

        if resp.status != 200:
            data = yield from resp.read()
            raise Exception('Callback was unsuccessful, got {}, {}'.format(resp, data.decode('utf-8')))
        logger.info('Successfully sent callback for a {} request'.format(action))
