import pytest

from waterbutler.core import streams


class TestStringStream:

    @pytest.mark.asyncio
    async def test_works(self):
        data = b'This here be a string yar'
        stream = streams.StringStream(data)
        read = await stream.read()
        assert data == read

    @pytest.mark.asyncio
    async def test_converts_strings(self):
        data = 'This here be a string yar'
        stream = streams.StringStream(data)
        read = await stream.read()
        assert data.encode('utf-8') == read
        assert data == read.decode('utf-8')

    @pytest.mark.asyncio
    async def test_1_at_a_time(self):
        data = 'This here be a string yar'
        stream = streams.StringStream(data)

        for letter in data:
            assert letter.encode('utf-8') == await stream.read(1)

    def test_size(self):
        data = 'This here be a string yar'
        stream = streams.StringStream(data)
        assert stream.size == len(data)

    @pytest.mark.asyncio
    async def test_hits_eof(self):
        data = 'This here be a string yar'
        stream = streams.StringStream(data)
        assert stream.at_eof() is False
        await stream.read()
        assert stream.at_eof() is True

    def test_must_be_str_or_bytes(self):
        with pytest.raises(TypeError):
            streams.StringStream(object())
