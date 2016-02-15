import json
import pytest
from waterbutler.core import streams

class TestJSONStream:

    @pytest.mark.asyncio
    async def test_single_element_strings(self):
        data = {
            'key': 'value'
        }

        stream = streams.JSONStream(data)

        read = await stream.read()

        assert data == json.loads(read.decode('utf-8'))

    @pytest.mark.asyncio
    async def test_multielement(self):
        data = {
            'key': 'value',
            'json': 'has',
            'never': 'looked',
            'this': 'cool'
        }

        stream = streams.JSONStream(data)

        read = await stream.read()

        assert data == json.loads(read.decode('utf-8'))

    @pytest.mark.asyncio
    async def test_other_streams(self):
        stream = streams.JSONStream({
            'justAStream': streams.StringStream('These are some words')
        })

        read = await stream.read()

        assert json.loads(read.decode('utf-8')) == {
            'justAStream': 'These are some words'
        }

    @pytest.mark.asyncio
    async def test_other_streams_1_at_a_time(self):
        stream = streams.JSONStream({
            'justAStream': streams.StringStream('These are some words')
        })

        buffer = b''
        chunk = await stream.read(1)

        while chunk:
            buffer += chunk
            chunk = await stream.read(1)

        assert json.loads(buffer.decode('utf-8')) == {
            'justAStream': 'These are some words'
        }

    @pytest.mark.asyncio
    async def test_github(self):
        stream = streams.JSONStream({
            'encoding': 'base64',
            'content': streams.Base64EncodeStream(streams.StringStream('These are some words')),
        })

        buffer = b''
        chunk = await stream.read(1)

        while chunk:
            buffer += chunk
            chunk = await stream.read(1)

        assert json.loads(buffer.decode('utf-8')) == {
            'encoding': 'base64',
            'content': 'VGhlc2UgYXJlIHNvbWUgd29yZHM='
        }

    @pytest.mark.asyncio
    async def test_github_at_once(self):
        stream = streams.JSONStream({
            'encoding': 'base64',
            'content': streams.Base64EncodeStream(streams.StringStream('These are some words')),
        })

        buffer = await stream.read()

        assert json.loads(buffer.decode('utf-8')) == {
            'encoding': 'base64',
            'content': 'VGhlc2UgYXJlIHNvbWUgd29yZHM='
        }

    # TODO
    # @pytest.mark.asyncio
    # def test_nested_streams(self):
    #     data = {
    #         'key': 'value',
    #         'json': 'has',
    #         'never': 'looked',
    #         'this': 'cool',
    #     }

    #     stream = streams.JSONStream({
    #         'outer': streams.JSONStream({'inner': streams.JSONStream(data)}),
    #         'justAStream': streams.StringStream('These are some words')
    #     })

    #     read = await stream.read()

    #     assert json.loads(read.decode('utf-8')) == {
    #         'outer': {
    #             'inner': data
    #         },
    #         'justAStream': 'These are some words'
    #     }

