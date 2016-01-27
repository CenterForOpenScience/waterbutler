import asyncio
import os

from waterbutler.core.streams import BaseStream, RequestStreamReader, FileStreamReader, ResponseStreamReader
from waterbutler.encryption.blockcipher import decrypt_block, encrypt_block

# CHUNK_SIZE must be the same as
#   CHUNK_SIZE in server/settings.py
#   filesystem/settings.py
#   DEFAULT_LIMIT = 65536 in aiohttp/streams.py
# Note: aiohttp/client.py uses the DEFAULT_LIMIT when read from streams
CHUNK_SIZE = 65536


def stream_wrapper(stream):
    """
    Wrap around a stream to add encryption/decryption
    Can be extended for other functionality
        :param stream:
        :rtype: EncFileStreamReader, EncRequestStreamReader, EncResponseStreamReader
    """
    if isinstance(stream, BaseStream):
        if isinstance(stream, RequestStreamReader):
            stream.__class__ = EncRequestStreamReader
            return stream
        elif isinstance(stream, FileStreamReader):
            stream.__class__ = EncFileStreamReader
            return stream
        elif isinstance(stream, ResponseStreamReader):
            stream.__class__ = EncResponseReader
            return stream
    else:
        pass

    return stream


# EncFileStreamReader inherits from FileStreamReader which handles local file
class EncFileStreamReader(FileStreamReader):

    @property
    def original_size(self):
        # file size before encryption
        # necessary for setting correct http header 'Content-Length'

        # calculate the file size
        cursor = self.file_pointer.tell()
        self.file_pointer.seek(0, os.SEEK_END)
        size = self.file_pointer.tell()
        # check the Base64 encoding padding
        pad = 0
        self.file_pointer.seek(-27, os.SEEK_END)
        if self.file_pointer.read(1) == b'=':
            pad = 2
        elif self.file_pointer.read(1) == b'=':
            pad = 1
        # go back to the beginning of the file
        self.file_pointer.seek(cursor)
        # calculate the original file size
        chunk_size = length_b64encode(CHUNK_SIZE) + 96
        num_of_chunks = int(size / chunk_size)
        partial_size = size % chunk_size - 96
        ret = CHUNK_SIZE * num_of_chunks + length_b64decode(partial_size, pad)

        return ret

    @asyncio.coroutine
    def _read(self, size):
        """
        Override the _read(size) method to enable decryption
            :param size:
            :rtype: bytes
        """
        self.file_gen = self.file_gen or self.read_as_gen()
        # add sleep of 0 so read will yield and continue in next io loop iteration
        yield from asyncio.sleep(0)

        # calculate the read_size for ciphertext
        self.read_size = length_b64encode(size) + 96
        try:
            enc_chunk = next(self.file_gen)
            dec_chunk = decrypt_block(enc_chunk)
            return dec_chunk
        except StopIteration:
            self.feed_eof()
            return b''


# EncResponseReader inherits from RequestStreamReader which handles file download
class EncResponseReader(ResponseStreamReader):

    @asyncio.coroutine
    def _read(self, size):
        """
        Override the _read(size) method to enable decryption
            :param size:
            :rtype: bytes
        """

        # calculate the chunk size for stream read
        size = length_b64encode(size) + 96
        try:
            enc_chunk = (yield from self.response.content.readexactly(size))
            dec_chunk = decrypt_block(enc_chunk)
            return dec_chunk
        except asyncio.IncompleteReadError as e:
            partial_chunk = e.partial
            if partial_chunk and len(partial_chunk) > 0:
                dec_chunk = decrypt_block(partial_chunk)
                return dec_chunk
            return b''


# EncRequestStreamReader inherits from RequestStreamReader which handles file upload
class EncRequestStreamReader(RequestStreamReader):

    @property
    def size(self):
        """
        Override the size property to return the length after encryption.
        Providers use this size to set http request header 'Content-Length'
        """
        size = int(self.request.headers.get('Content-Length'))
        new_size = 0
        while size > CHUNK_SIZE:
            new_size += length_b64encode(CHUNK_SIZE) + 96
            size -= CHUNK_SIZE
        new_size += length_b64encode(size) + 96
        return new_size

    @asyncio.coroutine
    def _read(self, size):
        """
        Override the _read(size) method to enable encryption
            :param size:
            :rtype: bytes
        """
        if self.inner.at_eof():
            return b''
        if size < 0:
            chunk = yield from self.inner.read(size)
            enc_chunk = encrypt_block(chunk)
            return enc_chunk
        try:
            plain_chunk = yield from self.inner.readexactly(size)
            enc_chunk = encrypt_block(plain_chunk)
            return enc_chunk
        except asyncio.IncompleteReadError as e:
            partial_chunk = e.partial
            if partial_chunk and len(partial_chunk) > 0:
                enc_chunk = encrypt_block(partial_chunk)
            else:
                enc_chunk = b''
            return enc_chunk


# two helper method to calculate length for Base64 En/Decoding
def length_b64encode(size):
    return (int((size - 1) / 3) + 1) * 4


def length_b64decode(size, pad):
    return int(size / 4) * 3 - pad
