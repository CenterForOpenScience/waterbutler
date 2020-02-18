import functools
import random
import string
from unittest import mock

import pytest

from waterbutler.core import streams


@pytest.fixture
def blob():
    return ''.join(random.sample(string.printable, 50)).encode('utf-8')


class TestCutoffStream:

    @pytest.mark.asyncio
    async def test_one_chunk(self, blob):
        stream = streams.StringStream(blob)
        cutoff_stream = streams.CutoffStream(stream, len(blob))
        data = await cutoff_stream.read()
        assert len(data) == len(blob)
        assert data == blob

    @pytest.mark.asyncio
    async def test_multi_chunk(self, blob):
        stream = streams.StringStream(blob)

        cutoff_stream_one = streams.CutoffStream(stream, 10)
        data_one = await cutoff_stream_one.read()
        assert len(data_one) == 10
        assert data_one == blob[0:10]

        cutoff_stream_two = streams.CutoffStream(stream, 10)
        data_two = await cutoff_stream_two.read()
        assert len(data_two) == 10
        assert data_two == blob[10:20]

        remainder = await stream.read()
        assert len(remainder) == 30
        assert remainder == blob[20:50]

    @pytest.mark.asyncio
    async def test_subchunk(self, blob):
        stream = streams.StringStream(blob)
        cutoff_stream = streams.CutoffStream(stream, 20)

        subchunk_one = await cutoff_stream.read(7)
        assert len(subchunk_one) == 7
        assert subchunk_one == blob[0:7]

        subchunk_two = await cutoff_stream.read(7)
        assert len(subchunk_two) == 7
        assert subchunk_two == blob[7:14]

        subchunk_three = await cutoff_stream.read(7)
        assert len(subchunk_three) == 6
        assert subchunk_three == blob[14:20]

        subchunk_four = await cutoff_stream.read(7)
        assert len(subchunk_four) == 0
        assert subchunk_four == b''

        remainder = await stream.read()
        assert len(remainder) == 30
        assert remainder == blob[20:50]

    def test_no_cutoff_exception(self, blob):
        stream = streams.StringStream(blob)
        with pytest.raises(TypeError):
            streams.CutoffStream(stream)
