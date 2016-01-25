import os
# import asyncio

from waterbutler.core.streams import BaseStream


class FileStreamReader(BaseStream):

    def __init__(self, file_pointer):
        super().__init__()
        self.file_gen = None
        self.file_pointer = file_pointer
        self.read_size = None
        self.content_type = 'application/octet-stream'

    @property
    def size(self):
        cursor = self.file_pointer.tell()
        self.file_pointer.seek(0, os.SEEK_END)
        ret = self.file_pointer.tell()
        self.file_pointer.seek(cursor)
        return ret

    def close(self):
        self.file_pointer.close()
        self.feed_eof()

    class read_chunks:
        def __init__(self, read_size, fp):
            self.done = False
            self.read_size = read_size
            self.fp = fp

        async def __aiter__(self):
            return self

        async def __anext__(self):
            if self.done:
                raise StopAsyncIteration
            return await self.get_chunk()

        async def get_chunk(self):
            while True:
                chunk = self.fp.read(self.read_size)
                if not chunk:
                    chunk = b''
                    self.done = True
                return chunk

    async def _read(self, read_size):
        async for chunk in self.read_chunks(read_size, self.file_pointer):
            if chunk == b'':
                self.feed_eof()
            return chunk
