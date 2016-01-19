import http
import time
import socket
import asyncio
import logging

import tornado.gen

from waterbutler.core import utils
from waterbutler.core import exceptions
from waterbutler.server import settings
from waterbutler.server.api.v1 import core
from waterbutler.server.auth import AuthHandler
from waterbutler.constants import IDENTIFIER_PATHS
from waterbutler.core.streams import RequestStreamReader
from waterbutler.server.api.v1.provider.create import CreateMixin
from waterbutler.server.api.v1.provider.metadata import MetadataMixin
from waterbutler.server.api.v1.provider.movecopy import MoveCopyMixin

logger = logging.getLogger(__name__)
auth_handler = AuthHandler(settings.AUTH_HANDLERS)


@tornado.web.stream_request_body
class ProviderHandler(core.BaseHandler, CreateMixin, MetadataMixin, MoveCopyMixin):
    PRE_VALIDATORS = {'put': 'prevalidate_put', 'post': 'prevalidate_post'}
    POST_VALIDATORS = {'put': 'postvalidate_put'}
    PATTERN = r'/resources/(?P<resource>(?:\w|\d)+)/providers/(?P<provider>(?:\w|\d)+)(?P<path>/.*/?)'

    @tornado.gen.coroutine
    def prepare(self, *args, **kwargs):
        method = self.request.method.lower()

        # TODO Find a nicer way to handle this
        if method == 'options':
            return

        self.path = self.path_kwargs['path'] or '/'
        provider = self.path_kwargs['provider']
        self.resource = self.path_kwargs['resource']

        # pre-validator methods perform validations that can be performed before ensuring that the
        # path given by the url is valid.  An example would be making sure that a particular query
        # parameter matches and allowed value.  We do this because validating the path requires
        # issuing one or more API calls to the provider, and some providers are quite stingy with
        # their rate limits.
        if method in self.PRE_VALIDATORS:
            getattr(self, self.PRE_VALIDATORS[method])()

        self.auth = yield from auth_handler.get(self.resource, provider, self.request)
        self.provider = utils.make_provider(provider, self.auth['auth'], self.auth['credentials'], self.auth['settings'])
        self.path = yield from self.provider.validate_v1_path(self.path)

        self.target_path = None

        # post-validator methods perform validations that expect that the path given in the url has
        # been verified for existence and type.
        if method in self.POST_VALIDATORS:
            getattr(self, self.POST_VALIDATORS[method])()

        # The one special case
        if method == 'put' and self.target_path.is_file:
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
        # handle newfile vs. newfolder naming conflicts
        if self.path.is_dir:
            my_type_exists = yield from self.provider.exists(self.target_path)
            if my_type_exists:
                raise exceptions.NamingConflict(self.target_path)

            if not self.provider.can_duplicate_names():
                target_flipped = self.path.child(self.childs_name, folder=(self.kind != 'folder'))
                other_exists = yield from self.provider.exists(target_flipped)
                # the dropbox provider's metadata() method returns a [] here instead of True
                if not isinstance(other_exists, bool) or other_exists:
                    raise exceptions.NamingConflict(self.target_path)

        if self.target_path.is_file:
            return (yield from self.upload_file())
        return(yield from self.create_folder())

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
        self.uploader = asyncio.async(self.provider.upload(self.stream, self.target_path))

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
            'PUT': lambda: ('create' if self.target_path.is_file else 'create_folder') if status == 201 else 'update',
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
            'provider': self.provider.NAME,
        }

        callback_url = None
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
                    'kind': self.dest_meta.kind,
                    'name': self.dest_meta.name,
                    'path': self.dest_meta.path,
                    'provider': self.dest_provider.NAME,
                    'materialized': self.dest_meta.materialized_path,
                }
            })
            callback_url = self.dest_auth['callback_url']
        else:
            # This is adequate for everything but github
            # If extra can be included it will link to the given sha
            payload_path = self.target_path or self.path
            payload.update({
                'metadata': {
                    # Hack: OSF and box use identifiers to refer to files
                    'path': payload_path.identifier_path if self.provider.NAME in IDENTIFIER_PATHS else payload_path.path,
                    'name': payload_path.name,
                    'materialized': str(payload_path),
                    'provider': self.provider.NAME,
                }
            })
            callback_url = self.auth['callback_url']

        resp = (yield from utils.send_signed_request('PUT', callback_url, payload))

        if resp.status != 200:
            data = yield from resp.read()
            raise Exception('Callback was unsuccessful, got {}, {}'.format(resp, data.decode('utf-8')))
        logger.info('Successfully sent callback for a {} request'.format(action))
