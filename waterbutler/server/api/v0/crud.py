import os
import http
import asyncio
import socket

import tornado.web
import tornado.gen
import tornado.httputil
import tornado.platform.asyncio

from waterbutler.core import mime_types
from waterbutler.server import utils
from waterbutler.server.api.v0 import core
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

        if 'Range' in self.request.headers:
            request_range = tornado.httputil._parse_request_range(self.request.headers['Range'])
        else:
            request_range = None

        result = yield from self.provider.download(range=request_range, **self.arguments)

        if isinstance(result, str):
            return self.redirect(result)

        if getattr(result, 'partial', None):
            # Use getattr here as not all stream may have a partial attribute
            # Plus it fixes tests
            self.set_status(206)
            self.set_header('Content-Range', result.content_range)

        if result.content_type is not None:
            self.set_header('Content-Type', result.content_type)

        if result.size is not None:
            self.set_header('Content-Length', str(result.size))

        # Build `Content-Disposition` header from `displayName` override,
        # headers of provider response, or file path, whichever is truthy first
        name = self.arguments.get('displayName') or getattr(result, 'name', None) or self.path.name
        self.set_header('Content-Disposition', utils.make_disposition(name))

        _, ext = os.path.splitext(name)
        # If the file extention is in mime_types
        # override the content type to fix issues with safari shoving in new file extensions
        if ext in mime_types:
            self.set_header('Content-Type', mime_types[ext])

        yield self.write_stream(result)

    @tornado.gen.coroutine
    def post(self):
        """Create a folder"""
        metadata = (yield from self.provider.create_folder(**self.arguments)).serialized()

        self.set_status(201)
        self.write(metadata)

        self._send_hook('create_folder', metadata)

    @tornado.gen.coroutine
    def put(self):
        """Upload a file."""
        self.writer.write_eof()

        metadata, created = yield from self.uploader
        metadata = metadata.serialized()

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
