import asyncio

from botocore.response import StreamingBody

from waterbutler.core.streams.base import BaseStream


class S3ResponseBodyStream(BaseStream):
    def __init__(self, data):
        super().__init__()

        if not isinstance(data['Body'], StreamingBody):
            raise TypeError('Data must be a StreamingBody, found {!r}'.format(type(data['body'])))

        self.content_type = data['ContentType']
        self._size = data['ContentLength']
        self.streaming_body = data['Body']

    @property
    def size(self):
        return self._size

    async def _read(self, n=None):
        n = self._size if n is None else n

        chunk = self.streaming_body.read(amt=n)
        if not chunk:
            self.feed_eof()
        return chunk
