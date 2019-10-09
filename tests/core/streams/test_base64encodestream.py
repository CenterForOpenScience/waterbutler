import pytest
import base64
import functools
from unittest import mock

from waterbutler.core import streams


class TestBase64Stream:

    @pytest.mark.asyncio
    async def test_doesnt_crash_with_none(self):
        stream = streams.Base64EncodeStream(streams.StringStream(b''))
        data = await stream.read()

        assert data == b''

    @pytest.mark.asyncio
    async def test_read(self):
        data = b'this is a test'
        expected = base64.b64encode(data)
        stream = streams.Base64EncodeStream(streams.StringStream(data))

        actual = await stream.read()

        assert expected == actual

    @pytest.mark.asyncio
    async def test_chunking(self):
        for chunk_size in range(1, 10):
            data = b'the ode to carp'
            expected = streams.StringStream(base64.b64encode(data))
            stream = streams.Base64EncodeStream(streams.StringStream(data))

            hoped = await expected.read(chunk_size)

            while hoped:
                actual = await stream.read(chunk_size)
                assert actual == hoped
                hoped = await expected.read(chunk_size)

            left_overs = await stream.read()

            assert left_overs == b''

    def test_size(self):
        data = b'the ode to carp'
        expected = base64.b64encode(data)
        stream = streams.Base64EncodeStream(streams.StringStream(data))

        assert len(expected) == int(stream.size)

