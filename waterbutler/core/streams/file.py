import os
import asyncio

import agent

from waterbutler.core.streams.base import BaseStream


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

    @agent.async_generator
    def chunk_reader(self):
        self.file_pointer.seek(0)
        while True:
            chunk = self.file_pointer.read(self.read_size)
            if not chunk:
                self.feed_eof()
                yield b''

            yield chunk

    async def _read(self, size):
        self.file_gen = self.file_gen or self.chunk_reader()
        self.read_size = size
        # add sleep of 0 so read will yield and continue in next io loop iteration
        # asyncio.sleep(0) yields None by default, which displeases tornado
        await asyncio.sleep(0.001)
        async for chunk in self.file_gen:
            return chunk
