import uuid
import asyncio

from waterbutler.core.streams.base import BaseStream, MultiStream, StringStream


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


class RequestStreamReader(BaseStream):

    def __init__(self, request, inner):
        super().__init__()
        self.inner = inner
        self.request = request

    @property
    def size(self):
        return int(self.request.headers.get('Content-Length'))

    def at_eof(self):
        return self.inner.at_eof()

    async def _read(self, size):
        if self.inner.at_eof():
            return b''
        if size < 0:
            return (await self.inner.read(size))
        try:
            return (await self.inner.readexactly(size))
        except asyncio.IncompleteReadError as e:
            return e.partial
