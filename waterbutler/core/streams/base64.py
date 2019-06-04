import base64
import asyncio

from waterbutler.server.settings import CHUNK_SIZE


class Base64EncodeStream(asyncio.StreamReader):

    @staticmethod
    def calculate_encoded_size(size):
        size = 4 * size / 3
        if size % 4:
            size += 4 - size % 4
        return int(size)

    def __init__(self, stream, **kwargs):
        self.extra = b''
        self.stream = stream
        if stream.size is None:
            self._size = None
        else:
            self._size = Base64EncodeStream.calculate_encoded_size(stream.size)

        super().__init__(**kwargs)

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

    async def read(self, n=-1):
        if n < 0:
            return (await super().read(n))

        nog = n
        padding = n % 3
        if padding:
            n += (3 - padding)

        chunk = self.extra + base64.b64encode((await self.stream.read(n)))

        if len(chunk) <= nog:
            self.extra = b''
            return chunk

        chunk, self.extra = chunk[:nog], chunk[nog:]

        return chunk

    def at_eof(self):
        return len(self.extra) == 0 and self.stream.at_eof()
