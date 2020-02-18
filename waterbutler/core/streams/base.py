import abc
import asyncio

from waterbutler.server.settings import CHUNK_SIZE


class BaseStream(asyncio.StreamReader, metaclass=abc.ABCMeta):
    """A wrapper class around an existing stream that supports teeing to multiple reader and writer
    objects.  Though it inherits from `asyncio.StreamReader` it does not implement/augment all of
    its methods.  Only ``read()`` implements the teeing behavior; ``readexactly``, ``readline``,
    and ``readuntil`` do not.

    Classes that inherit from `BaseStream` must implement a ``_read()`` method that reads ``size``
    bytes from its source and returns it.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.readers = {}
        self.writers = {}

    def __aiter__(self):
        return self

    # TODO: Add more note on `AsyncIterablePayload` and its `write()` method in aiohttp3
    # TODO: Improve the BaseStream with `aiohttp.streams.AsyncStreamReaderMixin`
    async def __anext__(self):
        try:
            chunk = await self.read(CHUNK_SIZE)
        except EOFError:
            raise StopAsyncIteration
        if chunk == b'':
            raise StopAsyncIteration
        return chunk

    @abc.abstractproperty
    def size(self):
        pass

    def add_reader(self, name, reader):
        self.readers[name] = reader

    def remove_reader(self, name):
        del self.readers[name]

    def add_writer(self, name, writer):
        self.writers[name] = writer

    def remove_writer(self, name):
        del self.writers[name]

    def feed_eof(self):
        super().feed_eof()
        for reader in self.readers.values():
            reader.feed_eof()
        for writer in self.writers.values():
            if hasattr(writer, 'can_write_eof') and writer.can_write_eof():
                writer.write_eof()

    async def read(self, size=-1):
        eof = self.at_eof()
        data = await self._read(size)
        if not eof:
            for reader in self.readers.values():
                reader.feed_data(data)
            for writer in self.writers.values():
                writer.write(data)
        return data

    @abc.abstractmethod
    async def _read(self, size):
        pass


class MultiStream(asyncio.StreamReader):
    """Concatenate a series of `StreamReader` objects into a single stream.
    Reads from the current stream until exhausted, then continues to the next,
    etc. Used to build streaming form data for Figshare uploads.
    Originally written by @jmcarp
    """
    def __init__(self, *streams):
        super().__init__()
        self._size = 0
        self.stream = []
        self._streams = []

        self.add_streams(*streams)

    def __aiter__(self):
        return self

    # TODO: Add more note on `AsyncIterablePayload` and its `write()` method in aiohttp3
    # TODO: Improve the BaseStream with `aiohttp.streams.AsyncStreamReaderMixin`
    async def __anext__(self):
        try:
            chunk = await self.read(CHUNK_SIZE)
        except EOFError:
            raise StopAsyncIteration
        if chunk == b'':
            raise StopAsyncIteration
        return chunk

    @property
    def size(self):
        return self._size

    @property
    def streams(self):
        return self._streams

    def add_streams(self, *streams):
        self._size += sum(x.size for x in streams)
        self._streams.extend(streams)

        if not self.stream:
            self._cycle()

    async def read(self, n=-1):
        if n < 0:
            return (await super().read(n))

        chunk = b''

        while self.stream and (len(chunk) < n or n == -1):
            if n == -1:
                chunk += await self.stream.read(-1)
            else:
                chunk += await self.stream.read(n - len(chunk))

            if self.stream.at_eof():
                self._cycle()

        return chunk

    def _cycle(self):
        try:
            self.stream = self.streams.pop(0)
        except IndexError:
            self.stream = None
            self.feed_eof()


class CutoffStream(asyncio.StreamReader):
    """A wrapper around an existing stream that terminates after pulling off the specified number
    of bytes.  Useful for segmenting an existing stream into parts suitable for chunked upload
    interfaces.

    This class only subclasses `asyncio.StreamReader` to take advantage of the `isinstance`-based
    stream-reading interface of aiohttp v0.18.2. It implements a ``read()`` method with the same
    signature as `StreamReader` that does the bookkeeping to know how many bytes to request from
    the stream attribute.

    :param stream: a stream object to wrap
    :param int cutoff: number of bytes to read before stopping
    """

    def __init__(self, stream, cutoff):
        super().__init__()
        self.stream = stream
        self._cutoff = cutoff
        self._thus_far = 0
        self._size = min(cutoff, stream.size)

    def __aiter__(self):
        return self

    # TODO: Add more note on `AsyncIterablePayload` and its `write()` method in aiohttp3
    # TODO: Improve the BaseStream with `aiohttp.streams.AsyncStreamReaderMixin`
    async def __anext__(self):
        try:
            chunk = await self.read(CHUNK_SIZE)
        except EOFError:
            raise StopAsyncIteration
        if chunk == b'':
            raise StopAsyncIteration
        return chunk

    @property
    def size(self):
        """The lesser of the wrapped stream's size or the cutoff."""
        return self._size

    async def read(self, n=-1):
        """Read ``n`` bytes from the stream. ``n`` is a chunk size, not the full size of the
        stream.  If ``n`` is -1, read ``cutoff`` bytes.  If ``n`` is a positive integer, read
        that many bytes as long as the total number of bytes read so far does not exceed
        ``cutoff``.
        """
        if n < 0:
            return await self.stream.read(self._cutoff)

        n = min(n, self._cutoff - self._thus_far)

        chunk = b''
        while self.stream and (len(chunk) < n):
            subchunk = await self.stream.read(n - len(chunk))
            chunk += subchunk
            self._thus_far += len(subchunk)

        return chunk


class StringStream(BaseStream):
    def __init__(self, data):
        super().__init__()
        if isinstance(data, str):
            data = data.encode('UTF-8')
        elif not isinstance(data, bytes):
            raise TypeError('Data must be either str or bytes, found {!r}'.format(type(data)))

        self._size = len(data)
        self.feed_data(data)
        self.feed_eof()

    @property
    def size(self):
        return self._size

    async def _read(self, n=-1):
        return (await asyncio.StreamReader.read(self, n))


class EmptyStream(BaseStream):
    """An empty stream with size 0 that returns nothing when read. Useful for representing
    empty folders when building zipfiles.
    """
    def __init__(self):
        super().__init__()
        self._eof = False

    def size(self):
        return 0

    def at_eof(self):
        return self._eof

    async def _read(self, n):
        self._eof = True
        return bytearray()
