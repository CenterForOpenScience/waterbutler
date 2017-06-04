import abc
import asyncio


class BaseStream(asyncio.StreamReader, metaclass=abc.ABCMeta):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.readers = {}
        self.writers = {}

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
