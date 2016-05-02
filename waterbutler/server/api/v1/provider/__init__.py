import http
import socket
import asyncio
import logging

import tornado.gen

from waterbutler.core import utils
from waterbutler.server import settings
from waterbutler.server.api.v1 import core
from waterbutler.server.auth import AuthHandler
from waterbutler.core.log_payload import LogPayload
from waterbutler.core.streams import RequestStreamReader
from waterbutler.server.api.v1.provider.create import CreateMixin
from waterbutler.server.api.v1.provider.metadata import MetadataMixin
from waterbutler.server.api.v1.provider.movecopy import MoveCopyMixin

logger = logging.getLogger(__name__)
auth_handler = AuthHandler(settings.AUTH_HANDLERS)


def list_or_value(value):
    assert isinstance(value, list)
    if len(value) == 0:
        return None
    if len(value) == 1:
        # Remove leading slashes as they break things
        return value[0].decode('utf-8')
    return [item.decode('utf-8') for item in value]


@tornado.web.stream_request_body
class ProviderHandler(core.BaseHandler, CreateMixin, MetadataMixin, MoveCopyMixin):
    PRE_VALIDATORS = {'put': 'prevalidate_put', 'post': 'prevalidate_post'}
    POST_VALIDATORS = {'put': 'postvalidate_put'}
    PATTERN = r'/resources/(?P<resource>(?:\w|\d)+)/providers/(?P<provider>(?:\w|\d)+)(?P<path>/.*/?)'

    async def prepare(self, *args, **kwargs):
        method = self.request.method.lower()

        # TODO Find a nicer way to handle this
        if method == 'options':
            return

        self.arguments = {
            key: list_or_value(value)
            for key, value in self.request.query_arguments.items()
        }

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

        self.auth = await auth_handler.get(self.resource, provider, self.request)
        self.provider = utils.make_provider(provider, self.auth['auth'], self.auth['credentials'], self.auth['settings'])
        self.path = await self.provider.validate_v1_path(self.path, **self.arguments)

        self.target_path = None

        # post-validator methods perform validations that expect that the path given in the url has
        # been verified for existence and type.
        if method in self.POST_VALIDATORS:
            await getattr(self, self.POST_VALIDATORS[method])()

        # The one special case
        if method == 'put' and self.target_path.is_file:
            await self.prepare_stream()
        else:
            self.stream = None
        self.body = b''

    async def head(self, **_):
        """Get metadata for a folder or file
        """
        if self.path.is_dir:
            return self.set_status(int(http.client.NOT_IMPLEMENTED))  # Metadata on the folder itself TODO
        return (await self.header_file_metadata())

    async def get(self, **_):
        """Download a file
        Will redirect to a signed URL if possible and accept_url is not False
        :raises: MustBeFileError if path is not a file
        """
        if self.path.is_dir:
            return (await self.get_folder())
        return (await self.get_file())

    async def put(self, **_):
        """Defined in CreateMixin"""
        if self.target_path.is_file:
            return (await self.upload_file())
        return (await self.create_folder())

    async def post(self, **_):
        return (await self.move_or_copy())

    async def delete(self, **_):
        self.confirm_delete = int(self.get_query_argument('confirm_delete',
                                                          default=0))
        await self.provider.delete(self.path,
                                        confirm_delete=self.confirm_delete)
        self.set_status(int(http.client.NO_CONTENT))

    async def data_received(self, chunk):
        """Note: Only called during uploads."""
        if self.stream:
            self.writer.write(chunk)
            await self.writer.drain()
        else:
            self.body += chunk

    async def prepare_stream(self):
        """Sets up an asyncio pipe from client to server
        Only called on PUT when path is to a file
        """
        self.rsock, self.wsock = socket.socketpair()

        self.reader, _ = await asyncio.open_unix_connection(sock=self.rsock)
        _, self.writer = await asyncio.open_unix_connection(sock=self.wsock)

        self.stream = RequestStreamReader(self.request, self.reader)
        self.uploader = asyncio.ensure_future(self.provider.upload(self.stream, self.target_path))

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
    async def _send_hook(self, action):
        source = None
        destination = None

        if action in ('move', 'copy'):
            # if provider can't intra_move or copy, then the celery task will take care of logging
            if not getattr(self.provider, 'can_intra_' + action)(self.dest_provider, self.path):
                return

            source = LogPayload(self.resource, self.provider, path=self.path)
            destination = LogPayload(
                self.dest_resource,
                self.dest_provider,
                metadata=self.dest_meta,
            )
        elif action in ('create', 'create_folder', 'update'):
            source = LogPayload(self.resource, self.provider, metadata=self.metadata)
        elif action in ('delete',):
            source = LogPayload(self.resource, self.provider, path=self.path)
        else:
            return

        await utils.log_to_callback(action, source=source, destination=destination)
