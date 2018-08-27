import uuid
import asyncio
from asyncio import Future
from asyncio.streams import _DEFAULT_LIMIT


from tornado import gen, ioloop

from waterbutler.core.streams.base import BaseStream, MultiStream, StringStream

import logging
logger = logging.getLogger(__name__)


print(_DEFAULT_LIMIT)


class FormDataStream(MultiStream):
    """A child of MultiSteam used to create stream friendly multipart form data requests.
    Usage:

        >>> stream = FormDataStream(key1='value1', file=FileStream(...))

    Or:

        >>> stream = FormDataStream()
        >>> stream.add_field('key1', 'value1')
        >>> stream.add_file('file', FileStream(...), mime='text/plain')

    Additional options for files can be passed as a tuple ordered as:

        >>> FormDataStream(fieldName=(FileStream(...), 'fileName', 'Mime', 'encoding'))

    Auto generates boundaries and properly concatenates them
    Use FormDataStream.headers to get the proper headers to be included with requests
    Namely Content-Length, Content-Type
    """

    @classmethod
    def make_boundary(cls):
        """Creates a random-ish boundary for form data separator"""
        return uuid.uuid4().hex

    @classmethod
    def make_header(cls, name, disposition='form-data', additional_headers=None, **extra):
        additional_headers = additional_headers or {}
        header = 'Content-Disposition: {}; name="{}"'.format(disposition, name)

        header += ''.join([
            '; {}="{}"'.format(key, value)
            for key, value in extra.items() if value is not None
        ])

        additional = '\r\n'.join([
            '{}: {}'.format(key, value)
            for key, value in additional_headers.items() if value is not None
        ])

        header += '\r\n'

        if additional:
            header += additional
            header += '\r\n'

        return header + '\r\n'

    def __init__(self, **fields):
        """:param dict fields: A dict of fieldname: value to create the body of the stream"""
        self.can_add_more = True
        self.boundary = self.make_boundary()
        super().__init__()

        for key, value in fields.items():
            if isinstance(value, tuple):
                self.add_file(key, *value)
            elif isinstance(value, asyncio.StreamReader):
                self.add_file(key, value)
            else:
                self.add_field(key, value)

    @property
    def end_boundary(self):
        return StringStream('--{}--\r\n'.format(self.boundary))

    @property
    def headers(self):
        """The headers required to make a proper multipart form request
        Implicitly calls finalize as accessing headers will often indicate sending of the request
        Meaning nothing else will be added to the stream"""
        self.finalize()

        return {
            'Content-Length': str(self.size),
            'Content-Type': 'multipart/form-data; boundary={}'.format(self.boundary)
        }

    async def read(self, n=-1):
        if self.can_add_more:
            self.finalize()
        return (await super().read(n=n))

    def finalize(self):
        assert self.stream, 'Must add at least one stream to finalize'

        if self.can_add_more:
            self.can_add_more = False
            self.add_streams(self.end_boundary)

    def add_fields(self, **fields):
        for key, value in fields.items():
            self.add_field(key, value)

    def add_field(self, key, value):
        assert self.can_add_more, 'Cannot add more fields after calling finalize or read'

        self.add_streams(
            self._make_boundary_stream(),
            StringStream(self.make_header(key) + value + '\r\n')
        )

    def add_file(self, field_name, file_stream, file_name=None, mime='application/octet-stream',
                 disposition='file', transcoding='binary'):
        assert self.can_add_more, 'Cannot add more fields after calling finalize or read'

        header = self.make_header(
            field_name,
            disposition=disposition,
            filename=file_name,
            additional_headers={
                'Content-Type': mime,
                'Content-Transfer-Encoding': transcoding
            }
        )

        self.add_streams(
            self._make_boundary_stream(),
            StringStream(header),
            file_stream,
            StringStream('\r\n')
        )

    def _make_boundary_stream(self):
        return StringStream('--{}\r\n'.format(self.boundary))


class ResponseStreamReader(BaseStream):

    def __init__(self, response, size=None, name=None):
        super().__init__()
        if 'Content-Length' in response.headers:
            self._size = int(response.headers['Content-Length'])
        else:
            self._size = size
        self._name = name
        self.response = response

    @property
    def partial(self):
        return self.response.status == 206

    @property
    def content_type(self):
        return self.response.headers.get('Content-Type', 'application/octet-stream')

    @property
    def content_range(self):
        return self.response.headers['Content-Range']

    @property
    def name(self):
        return self._name

    @property
    def size(self):
        return self._size

    async def _read(self, size):
        chunk = (await self.response.content.read(size))

        if not chunk:
            self.feed_eof()
            await self.response.release()

        return chunk


class WritePendingError():
    pass


class RequestStreamReader(BaseStream):

    def __init__(self, request, max_buffer_size=_DEFAULT_LIMIT):
        super().__init__()
        self.request = request
        self.max_buffer_size = max_buffer_size
        self.pending_feed = None

    @property
    def size(self):
        return int(self.request.headers.get('Content-Length'))

    def feed_data(self, chunk, timeout=None):
        assert not self._eof, 'feed_data after feed_eof'
        # Trying to write to the stream from several coroutines doesn't seem
        # like a great idea, so limit it to one event loop, one coroutine.
        if self.pending_feed is not None:
            # Make sure the pending future is complete.
            future, chunk = self.pending_feed
            if not future.done():
                raise WritePendingError('Another coroutine is alreading waiting to write to this stream.')
            self.pending_feed = None

        if not chunk:
            # Nothing to add to the stream.
            return

        future = Future()

        if len(self._buffer) > self.max_buffer_size:
            # The buffer is full, and no more can be written to it until some
            # of it has been consumed. We will always be able to write
            # something to the buffer, because we don't check it for overflow.
            # (Default limit still remains)
            assert self.pending_feed is None
            self.pending_feed = (future, chunk)

            future.add_done_callback(lambda _: self.clear_pending_feed())

            if timeout:
                # Let a caller specify a maximum amount of time to wait.
                def on_timeout():
                    if not future.done():
                        future.set_exception(gen.TimeoutError())
                io_loop = ioloop.IOLoop.current()
                timeout_handle = io_loop.add_timeout(timeout, on_timeout)
                future.add_done_callback(lambda _: io_loop.remove_timeout(timeout_handle))

        else:
            # Sets the result of the Future.
            self.feed_nowait(future, chunk)

        # Give the future back for it to get awaited somewhere.
        return future

    def clear_pending_feed(self):
        self.pending_feed = None

    def feed_nowait(self, future, chunk):
        # We can put the chunk on the buffer.
        self._buffer.extend(chunk)
        future.set_result(None)
        self.clear_pending_feed()

        # Let a waiting read know there's data.
        self._wakeup_waiter()

    async def _read(self, n=-1):
        data = await asyncio.StreamReader.read(self, n)
        if self.pending_feed is not None and len(self._buffer) <= self.max_buffer_size:
            future, chunk = self.pending_feed
            if not future.done():
                self.feed_nowait(future, chunk)
        return data
