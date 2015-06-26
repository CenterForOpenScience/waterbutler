import http
import asyncio
import socket

import tornado.web
import tornado.gen
import tornado.platform.asyncio

from waterbutler.core import mime_types
from waterbutler.server import utils
from waterbutler.server.handlers import core
from waterbutler.core.streams import RequestStreamReader


TRUTH_MAP = {
    'true': True,
    'false': False,
}


@tornado.web.stream_request_body
class CRUDHandler(core.BaseProviderHandler):

    ACTION_MAP = {
        'GET': 'download',
        'PUT': 'upload',
        'DELETE': 'delete',
        'POST': 'create_folder',
    }
    STREAM_METHODS = ('PUT', )

    @tornado.gen.coroutine
    def prepare(self):
        yield super().prepare()
        yield from self.prepare_stream()

    @asyncio.coroutine
    def prepare_stream(self):
        if self.request.method in self.STREAM_METHODS:
            self.rsock, self.wsock = socket.socketpair()

            self.reader, _ = yield from asyncio.open_unix_connection(sock=self.rsock)
            _, self.writer = yield from asyncio.open_unix_connection(sock=self.wsock)

            self.stream = RequestStreamReader(self.request, self.reader)

            self.uploader = asyncio.async(
                self.provider.upload(self.stream, **self.arguments)
            )
        else:
            self.stream = None

    @tornado.gen.coroutine
    def data_received(self, chunk):
        """Note: Only called during uploads."""
        if self.stream:
            self.writer.write(chunk)
            yield from self.writer.drain()

    @tornado.gen.coroutine
    def get(self):
        """Download a file."""
        try:
            self.arguments['accept_url'] = TRUTH_MAP[self.arguments.get('accept_url', 'true').lower()]
        except KeyError:
            raise tornado.web.HTTPError(status_code=400)

        result = yield from self.provider.download(**self.arguments)

        if isinstance(result, str):
            return self.redirect(result)

        if hasattr(result, 'content_type'):
            self.set_header('Content-Type', result.content_type)

        if hasattr(result, 'size') and result.size is not None:
            self.set_header('Content-Length', str(result.size))

        # Build `Content-Disposition` header from `displayName` override,
        # headers of provider response, or file path, whichever is truthy first
        if self.arguments.get('displayName'):
            disposition = utils.make_disposition(self.arguments['displayName'])
        else:
            # If the file extention is in mime_types
            # override the content type to fix issues with safari shoving in new file extensions
            if self.arguments['path'].ext in mime_types:
                self.set_header('Content-Type', mime_types[self.arguments['path'].ext])

            disposition = utils.make_disposition(self.arguments['path'].name)

        self.set_header('Content-Disposition', disposition)

        yield from self.write_stream(result)

    @tornado.gen.coroutine
    def post(self):
        """Create a folder"""
        metadata = yield from self.provider.create_folder(**self.arguments)

        self.set_status(201)
        self.write(metadata)

        self._send_hook('create_folder', metadata)

    @tornado.gen.coroutine
    def put(self):
        """Upload a file."""
        self.writer.write_eof()

        metadata, created = yield from self.uploader
        if created:
            self.set_status(201)
        self.write(metadata)

        self.writer.close()
        self.wsock.close()

        self._send_hook(
            'create' if created else 'update',
            metadata,
        )

    @tornado.gen.coroutine
    def delete(self):
        """Delete a file."""

        yield from self.provider.delete(**self.arguments)
        self.set_status(http.client.NO_CONTENT)

        self._send_hook(
            'delete',
            {
                'path': str(self.arguments['path']),
                'materialized': str(self.arguments['path'])
            }
        )
