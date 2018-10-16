import pytest

from waterbutler.core import streams


class TestByteStream:

    @pytest.mark.asyncio
    async def test_works(self):
        data = b'This here be bytes yar'
        stream = streams.ByteStream(data)
        read = await stream.read()
        assert data == read

    @pytest.mark.asyncio
    async def test_1_at_a_time(self):
        data = b'This here be bytes yar'
        stream = streams.ByteStream(data)

        for i in range(len(data)):
            assert data[i:(i+1)] == await stream.read(1)

    def test_size(self):
        data = b'This here be bytes yar'
        stream = streams.ByteStream(data)
        assert stream.size == len(data)

    @pytest.mark.asyncio
    async def test_hits_eof(self):
        data = b'This here be bytes yar'
        stream = streams.ByteStream(data)
        assert stream.at_eof() is False
        await stream.read()
        assert stream.at_eof() is True

    def test_must_be_bytes(self):
        with pytest.raises(TypeError):
            streams.ByteStream(object())
        with pytest.raises(TypeError):
            streams.ByteStream('string')
