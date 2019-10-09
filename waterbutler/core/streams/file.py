import os
import asyncio

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

    async def chunk_reader(self):
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


class PartialFileStreamReader(FileStreamReader):
    """Awful class, used to avoid messing with FileStreamReader.  Extends FSR with start and end
    byte offsets to indicate a byte range of the file to return.  Reading from this stream will
    only return the requested range, never data outside of it.
    """

    def __init__(self, file_pointer, byte_range):
        super().__init__(file_pointer)
        self.start = byte_range[0]
        self.end = byte_range[1]
        self.bytes_read = 0

    @property
    def size(self):
        return self.end - self.start + 1

    @property
    def total_size(self):
        cursor = self.file_pointer.tell()
        self.file_pointer.seek(0, os.SEEK_END)
        ret = self.file_pointer.tell()
        self.file_pointer.seek(cursor)
        return ret

    @property
    def partial(self):
        return self.size < self.total_size

    @property
    def content_range(self):
        return 'bytes {}-{}/{}'.format(self.start, self.end, self.total_size)

    async def chunk_reader(self):
        self.file_pointer.seek(self.start)
        while True:
            chunk = self.file_pointer.read(self.read_size)
            self.bytes_read += self.read_size
            if not chunk:
                self.feed_eof()
                yield b''

            yield chunk

    async def _read(self, size):
        self.file_gen = self.file_gen or self.chunk_reader()
        bytes_remaining = self.size - self.bytes_read
        self.read_size = bytes_remaining if size == -1 else min(size, bytes_remaining)
        # add sleep of 0 so read will yield and continue in next io loop iteration
        # asyncio.sleep(0) yields None by default, which displeases tornado
        await asyncio.sleep(0.001)
        async for chunk in self.file_gen:
            return chunk
