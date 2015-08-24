import json
import asyncio

import tornado.gen

from waterbutler.server import utils
from waterbutler.server import settings
from waterbutler.server.auth import AuthHandler
from waterbutler.server.api.v1 import core
from waterbutler.core.utils import make_provider


auth_handler = AuthHandler(settings.AUTH_HANDLERS)


@tornado.web.stream_request_body
class ProviderHandler(core.BaseHandler):
    PATTERN = r'/resources/(?P<resource>(?:\w|\d)+)/providers/(?P<provider>(?:\w|\d)+)/(?P<path>.*/?)?'

    @tornado.gen.coroutine
    def prepare(self, *args, **kwargs):
        path = self.path_kwargs['path']
        provider = self.path_kwargs['provider']
        resource = self.path_kwargs['resource']

        self.auth = yield from auth_handler.get(resource, provider, self.request)
        self.provider = make_provider(provider, self.auth['auth'], self.auth['credentials'], self.auth['settings'])
        self.path = yield from self.provider.validate_path(path or '/')

        if self.request.method == 'PUT' and self.path.is_file:
            yield from self.prepare_stream()

    @asyncio.coroutine
    def prepare_stream(self):
        self.rsock, self.wsock = socket.socketpair()

        self.reader, _ = yield from asyncio.open_unix_connection(sock=self.rsock)
        _, self.writer = yield from asyncio.open_unix_connection(sock=self.wsock)

        self.stream = RequestStreamReader(self.request, self.reader)

        self.uploader = asyncio.async(
            self.provider.upload(self.stream, **self.arguments)
        )

    @tornado.gen.coroutine
    def head(self, **_):
        """Get metadata for a folder or file
        """
        if self.path.is_dir:
            self.set_status(405)  # Metadata on the folder itself TODO
            return

        data = yield from self.provider.metadata(self.path)

        self.set_header('Etag', data.etag)  # This may not be appropriate
        self.set_header('Content-Length', data.size)
        self.set_header('Last-Modified', data.modified)
        self.set_header('Content-Type', data.content_type)
        self.set_header('X-Waterbutler-Metadata', json.dumps(data.serialized()))

    @tornado.gen.coroutine
    def get(self, **_):
        """Download a file
        Will redirect to a signed URL if possible and accept_url is not False
        :raises: MustBeFileError if path is not a file
        """
        if self.path.is_dir:
            yield from self._get_folder()
        else:
            yield from self._get_file()

    @tornado.gen.coroutine
    def put(self, **_):
        if self.path.is_file:
            return (yield from self._do_upload())

        metadata = yield from self.provider.create_folder(self.path)
        self.set_status(201)
        self.write(metadata.serialized())

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
    def _do_upload(self):
        self.writer.write_eof()

        metadata, created = yield from self.uploader
        self.writer.close()
        self.wsock.close()
        if created:
            self.set_status(201)

        self.write(metadata.serialized())

    @asyncio.coroutine
    def _get_folder(self):
        if 'zip' in self.request.query_arguments:
            return (yield from self._get_folder_zip())
        data = yield from self.provider.metadata(self.path)
        return self.write({'data': [x.serialized() for x in data]})

    @asyncio.coroutine
    def _get_file(self):
        if 'meta' in self.request.query_arguments:
            return self.write({
                'data': (yield from self.provider.metadata(self.path)).serialized()
            })

        if 'versions' in self.request.query_arguments:
            return (yield from self._get_file_versions())

        if 'Range' in self.request.headers:
            request_range = tornado.httputil._parse_request_range(self.request.headers['Range'])
        else:
            request_range = None

        stream = yield from self.provider.download(
            self.path,
            range=request_range,
            accept_url='direct' not in self.request.query_arguments
        )

        if isinstance(stream, str):
            return self.redirect(stream)

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

        yield self.write_stream(stream)

    @asyncio.coroutine
    def _get_file_versions(self):
        result = self.provider.revisions(**self.arguments)

        if asyncio.iscoroutine(result):
            result = yield from result

        return self.write({'data': [r.serialized() for r in result]})

    @asyncio.coroutine
    def _get_folder_zip(self):
        self.set_header('Content-Type', 'application/zip')
        self.set_header(
            'Content-Disposition',
            utils.make_disposition((self.path.name or 'download') + '.zip')
        )

        result = yield from self.provider.zip(self.path)

        yield self.write_stream(result)
