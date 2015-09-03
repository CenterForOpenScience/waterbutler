import http
import socket
import asyncio

import tornado.gen

from waterbutler.server import settings
from waterbutler.server.api.v1 import core
from waterbutler.server.auth import AuthHandler
from waterbutler.core.utils import make_provider
from waterbutler.core.streams import RequestStreamReader
from waterbutler.server.api.v1.provider.create import CreateMixin
from waterbutler.server.api.v1.provider.metadata import MetadataMixin
from waterbutler.server.api.v1.provider.movecopy import MoveCopyMixin


auth_handler = AuthHandler(settings.AUTH_HANDLERS)


@tornado.web.stream_request_body
class ProviderHandler(core.BaseHandler, CreateMixin, MetadataMixin, MoveCopyMixin):
    VALIDATORS = {'put': 'validate_put', 'post': 'validate_post'}
    PATTERN = r'/resources/(?P<resource>(?:\w|\d)+)/providers/(?P<provider>(?:\w|\d)+)(?P<path>/.*/?)'

    @tornado.gen.coroutine
    def prepare(self, *args, **kwargs):
        path = self.path_kwargs['path']
        provider = self.path_kwargs['provider']
        self.resource = self.path_kwargs['resource']

        if self.request.method.lower() in self.VALIDATORS:
            # create must validate before accepting files
            getattr(self, self.VALIDATORS[self.request.method.lower()])()

        self.auth = yield from auth_handler.get(self.resource, provider, self.request)
        self.provider = make_provider(provider, self.auth['auth'], self.auth['credentials'], self.auth['settings'])
        self.path = yield from self.provider.validate_path(path or '/')

        # The one special case
        if self.request.method == 'PUT' and self.path.is_file:
            yield from self.prepare_stream()

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
