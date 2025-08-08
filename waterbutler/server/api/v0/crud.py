import os
import asyncio
from http import HTTPStatus

import tornado.web

from waterbutler.core import mime_types
from waterbutler.server import utils
from waterbutler.server.api.v0 import core
from waterbutler.core.utils import make_disposition
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

    async def prepare(self):
        await super().prepare()
        await self._prep_stream()

    async def _prep_stream(self):
        if self.request.method not in self.STREAM_METHODS:
            self.stream = None
            return

        self.reader = asyncio.StreamReader()
        self.stream = RequestStreamReader(self.request, self.reader)

    async def data_received(self, chunk: bytes):
        """Note: Only called during uploads."""
        self.bytes_uploaded += len(chunk)
        if self.stream:
            self.reader.feed_data(chunk)

    async def get(self):
        """Download a file."""
        try:
            self.arguments['accept_url'] = TRUTH_MAP[
                self.arguments.get('accept_url', 'true').lower()
            ]
        except KeyError:
            raise tornado.web.HTTPError(status_code=400)

        req_range = utils.parse_request_range(
            self.request.headers['Range']) if 'Range' in self.request.headers else None

        result = await self.provider.download(range=req_range, **self.arguments)

        if isinstance(result, str):
            self.redirect(result)
            self._send_hook('download_file', path=self.path)
            return

        if getattr(result, 'partial', None):
            # Use getattr here as not all stream may have a partial attribute
            # Plus it fixes tests
            self.set_status(206)
            self.set_header('Content-Range', result.content_range)
        if result.content_type:
            self.set_header('Content-Type', result.content_type)

        if result.size is not None:
            self.set_header('Content-Length', str(result.size))

        # Build `Content-Disposition` header from `displayName` override,
        # headers of provider response, or file path, whichever is truthy first
        name = self.arguments.get('displayName') or getattr(result, 'name', None) or self.path.name
        self.set_header('Content-Disposition', make_disposition(name))

        _, ext = os.path.splitext(name)
        # If the file extention is in mime_types
        # override the content type to fix issues with safari shoving in new file extensions
        if ext in mime_types:
            self.set_header('Content-Type', mime_types[ext])

        await self.write_stream(result)
        self._send_hook('download_file', path=self.path)

    async def post(self):
        """Create a folder"""
        metadata = await self.provider.create_folder(**self.arguments)

        self.set_status(201)
        self.write(metadata.serialized())

        self._send_hook('create_folder', metadata)

    async def put(self):
        """Handle upload once body is complete."""
        self.reader.feed_eof()

        metadata, created = await self.provider.upload(self.stream, **self.arguments)

        if created:
            self.set_status(201)
        self.write(metadata.serialized())

        self._send_hook(
            'create' if created else 'update',
            metadata,
        )

    async def delete(self):
        """Delete a file."""

        await self.provider.delete(**self.arguments)
        self.set_status(int(HTTPStatus.NO_CONTENT))

        self._send_hook('delete', path=self.path)
