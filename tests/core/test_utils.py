import asyncio
from unittest import mock

import pytest

from waterbutler.core import utils


class TestAsyncRetry:

    @pytest.mark.asyncio
    async def test_returns_success(self):
        mock_func = mock.Mock(return_value='Foo')
        retryable = utils.async_retry(5, 0, raven=None)(mock_func)
        x = await retryable()
        assert x == 'Foo'
        assert mock_func.call_count == 1

    @pytest.mark.asyncio
    async def test_retries_until(self):
        mock_func = mock.Mock(side_effect=[Exception(), 'Foo'])
        retryable = utils.async_retry(5, 0, raven=None)(mock_func)

        x = await retryable()

        assert x == 'Foo'
        assert mock_func.call_count == 2

    @pytest.mark.asyncio
    async def test_retries_then_raises(self):
        mock_func = mock.Mock(side_effect=Exception('Foo'))
        retryable = utils.async_retry(5, 0, raven=None)(mock_func)

        with pytest.raises(Exception) as e:
            coro = await retryable()

        assert e.type == Exception
        assert e.value.args == ('Foo',)
        assert mock_func.call_count == 6

    @pytest.mark.asyncio
    async def test_retries_by_its_self(self):
        mock_func = mock.Mock(side_effect=Exception())
        retryable = utils.async_retry(8, 0, raven=None)(mock_func)

        retryable()

        await asyncio.sleep(.1)

        assert mock_func.call_count == 9

    async def test_docstring_survives(self):
        async def mytest():
            '''This is a docstring'''
            pass

        retryable = utils.async_retry(8, 0, raven=None)(mytest)

        assert retryable.__doc__ == '''This is a docstring'''

    @pytest.mark.asyncio
    async def test_kwargs_work(self):
        async def mytest(mack, *args, **kwargs):
            mack()
            assert args == ('test', 'Foo')
            assert kwargs == {'test': 'Foo', 'baz': 'bam'}
            return True

        retryable = utils.async_retry(8, 0, raven=None)(mytest)
        merk = mock.Mock(side_effect=[Exception(''), 5])

        fut = retryable(merk, 'test', 'Foo', test='Foo', baz='bam')
        assert await fut

        assert merk.call_count == 2

    @pytest.mark.asyncio
    async def test_all_retry(self):
        mock_func = mock.Mock(side_effect=Exception())
        retryable = utils.async_retry(8, 0, raven=None)(mock_func)

        retryable()
        retryable()

        await asyncio.sleep(.1)

        assert mock_func.call_count == 18
