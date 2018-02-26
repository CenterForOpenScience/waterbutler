import os

import pytest

from waterbutler.core import streams


DUMMY_FILE = os.path.join(os.path.dirname(__file__), 'fixtures/dummy.txt')


class TestFileStreamReader:

    @pytest.mark.asyncio
    async def test_file_stream_reader(self):
        with open(DUMMY_FILE, 'r') as fp:
            reader = streams.FileStreamReader(fp)
            assert reader.size == 27

            data = await reader.read()
            assert data == 'abcdefghijklmnopqrstuvwxyz\n'
            at_eof = reader.at_eof()
            assert not at_eof

            data = await reader.read()
            assert data == b''
            at_eof = reader.at_eof()
            assert at_eof

            reader.close()
            at_eof = reader.at_eof()
            assert at_eof
            with pytest.raises(ValueError):
                fp.read()

    @pytest.mark.asyncio
    async def test_file_stream_reader_after_seek(self):
        with open(DUMMY_FILE, 'r') as fp:
            fp.seek(3)
            reader = streams.FileStreamReader(fp)
            assert reader.size == 27  # still gives full size

            assert fp.tell() == 3  # returns to original seek position
            data = await reader.read()
            assert data == 'abcdefghijklmnopqrstuvwxyz\n'  # always reads full data
            at_eof = reader.at_eof()
            assert not at_eof

            data = await reader.read()
            assert data == b''
            at_eof = reader.at_eof()
            assert at_eof

    @pytest.mark.asyncio
    async def test_file_stream_reader_subset(self):
        with open(DUMMY_FILE, 'r') as fp:
            reader = streams.FileStreamReader(fp)

            data = await reader.read(10)
            assert data == 'abcdefghij'
            at_eof = reader.at_eof()
            assert not at_eof

            data = await reader.read(2)
            assert data == 'kl'
            at_eof = reader.at_eof()
            assert not at_eof

            data = await reader.read()
            assert data == 'mnopqrstuvwxyz\n'
            at_eof = reader.at_eof()
            assert not at_eof

            data = await reader.read()
            assert data == b''
            at_eof = reader.at_eof()
            assert at_eof


class TestPartialFileStreamReader:

    @pytest.mark.asyncio
    @pytest.mark.parametrize("byte_range,size,is_partial,content_range,expected", [
        ((0, 26), 27, False, 'bytes 0-26/27', 'abcdefghijklmnopqrstuvwxyz\n'),
        ((0, 5), 6, True, 'bytes 0-5/27', 'abcdef'),
        ((2, 10), 9, True, 'bytes 2-10/27', 'cdefghijk'),
        ((20, 26), 7, True, 'bytes 20-26/27', 'uvwxyz\n'),
        ((2, 2), 1, True, 'bytes 2-2/27', 'c'),
    ])
    async def test_partial_file_stream_reader(self, byte_range, size, is_partial, content_range,
                                              expected):
        with open(DUMMY_FILE, 'r') as fp:
            reader = streams.PartialFileStreamReader(fp, byte_range)
            assert reader.size == size
            assert reader.total_size == 27
            assert reader.partial == is_partial
            assert reader.content_range == content_range

            data = await reader.read()
            assert data == expected
            at_eof = reader.at_eof()
            assert not at_eof

            data = await reader.read()
            assert data == b''
            at_eof = reader.at_eof()
            assert at_eof

    @pytest.mark.asyncio
    @pytest.mark.parametrize("byte_range,size,is_partial,content_range,expected", [
        ((0, 26), 27, False, 'bytes 0-26/27', 'abcdefghijklmnopqrstuvwxyz\n'),
        ((0, 5), 6, True, 'bytes 0-5/27', 'abcdef'),
        ((2, 10), 9, True, 'bytes 2-10/27', 'cdefghijk'),
        ((20, 26), 7, True, 'bytes 20-26/27', 'uvwxyz\n'),
        ((2, 2), 1, True, 'bytes 2-2/27', 'c'),
    ])
    async def test_partial_file_stream_reader_with_size(self, byte_range, size, is_partial,
                                                        content_range, expected):
        """Test that range is respected even when large size values are passed to ``.read()``."""

        with open(DUMMY_FILE, 'r') as fp:
            reader = streams.PartialFileStreamReader(fp, byte_range)
            assert reader.size == size
            assert reader.total_size == 27
            assert reader.partial == is_partial
            assert reader.content_range == content_range

            data = await reader.read(500)
            assert data == expected
            at_eof = reader.at_eof()
            assert not at_eof

            data = await reader.read(500)
            assert data == b''
            at_eof = reader.at_eof()
            assert at_eof
