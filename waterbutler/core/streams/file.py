import os

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

    def read_as_gen(self):
        self.file_pointer.seek(0)
        while True:
            chunk = self.file_pointer.read(self.read_size)
            if not chunk:
                self.feed_eof()
                chunk = b''
            yield chunk

    async def _read(self, size):
        self.file_gen = self.file_gen or self.read_as_gen()
        self.read_size = size
        return next(self.file_gen)
