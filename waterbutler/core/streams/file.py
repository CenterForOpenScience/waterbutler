import os
import agent

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

    @agent.async_generator
    def chunk_reader(self):
        self.done = False
        while True:
            chunk = self.file_pointer.read(self.read_size)
            if self.done:
                raise StopIteration
            if not chunk:
                chunk = b''
                self.done = True
                self.feed_eof()
            yield chunk

    async def _read(self, read_size):
        self.read_size = read_size
        async for chunk in self.chunk_reader():
            return chunk
