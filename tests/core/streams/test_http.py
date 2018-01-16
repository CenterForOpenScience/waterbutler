from unittest import mock

import pytest

from tests.core.streams.fixtures import (mock_content_eof, MockResponseNoContent,
                                         mock_content, MockResponseNoContentLength,
                                         mock_response_stream_reader, MockResponse,
                                         mock_response_stream_reader_no_size,
                                         mock_response_stream_reader_no_content)


class TestResponseStreamReader:

    @pytest.mark.asyncio
    async def test_response_stream_reader(self, mock_response_stream_reader):
        assert mock_response_stream_reader.name == 'test stream'
        assert mock_response_stream_reader.size == 100
        assert not mock_response_stream_reader.partial
        assert mock_response_stream_reader.content_type == 'application/octet-stream'
        assert mock_response_stream_reader.content_range == '0-100'
        assert (await mock_response_stream_reader.read()) == b'data'

    @pytest.mark.asyncio
    async def test_response_stream_reader_no_size(self, mock_response_stream_reader_no_size):
        assert mock_response_stream_reader_no_size.name == 'test stream'
        assert mock_response_stream_reader_no_size.size is None
        assert not mock_response_stream_reader_no_size.partial
        assert mock_response_stream_reader_no_size.content_type == 'application/octet-stream'
        assert mock_response_stream_reader_no_size.content_range == '0-0'
        assert (await mock_response_stream_reader_no_size.read()) == b'data'

    @pytest.mark.asyncio
    async def test_response_stream_reader_eof(self, mock_response_stream_reader_no_content):

        mock_response_stream_reader_no_content.feed_eof = mock.Mock()
        assert (await mock_response_stream_reader_no_content.read()) is None
        mock_response_stream_reader_no_content.feed_eof.assert_called_once_with()
        MockResponseNoContent.release.assert_called_once_with()
