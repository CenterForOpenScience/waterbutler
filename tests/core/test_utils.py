import asyncio
from unittest import mock

import pytest

from waterbutler.core import utils


class TestAsyncRetry:

    @pytest.mark.asyncio
    async def test_returns_success(self):
        """Test the scenario where a function succeeds on first attempt.
        """
        mock_func = mock.Mock(return_value='Foo')
        retryable = utils.async_retry(5, 0)(mock_func)

        x = await retryable()

        # `mock_func` succeeds on first attempt; thus it should be called only once
        assert x == 'Foo'
        assert mock_func.call_count == 1

    @pytest.mark.asyncio
    async def test_retries_success(self):
        """Test a scenario where a function fails first but succeeds after retrying.
        """
        mock_func = mock.Mock(side_effect=[Exception(), Exception(), 'Foo'])
        retryable = utils.async_retry(5, 0)(mock_func)

        x = await retryable()

        # `mock_func` fails on the first two attempts but succeeds on the third one; thus it should
        # be called exactly three times.
        assert x == 'Foo'
        assert mock_func.call_count == 3

    @pytest.mark.asyncio
    async def test_retries_failed(self):
        """Test a scenario where a function keeps failing / retrying until it reaches retry limit.
        """
        mock_func = mock.Mock(side_effect=Exception('Foo'))
        retryable = utils.async_retry(8, 0)(mock_func)

        with pytest.raises(Exception) as e:
            await retryable()

        # `mock_func` keeps failing until it reaches the maximum retry limit which is 8; thus it
        # should have been called 1 (initial) + 8 (retries) = 9 times before throwing an exception.
        assert e.type == Exception
        assert e.value.args == ('Foo',)
        assert mock_func.call_count == 9

    @pytest.mark.asyncio
    async def test_retries_by_itself(self):
        """Test ``async_retry`` decorated coroutine itself.

        This test takes care of the case where ``retryable()`` is called w/o ``await``, in which
        case it returns a future that can run as long as nothing else forces it to yield in the
        meantime.  The following ``await asyncio.sleep(.1)`` does exactly that.  As long as the
        number of retries is low, ``retryable()`` can execute them all before the sleep is done.
        However, if you remove the sleep or bump the number of retries up to a ludicrously high
        number, the test fails b/c it starts asserting before all the retries have been exhausted.
        """
        mock_func = mock.Mock(side_effect=Exception())
        retryable = utils.async_retry(8, 0)(mock_func)

        retryable()

        await asyncio.sleep(.1)

        # `mock_func` keeps failing until it reaches the maximum retry limit which is 8; thus it
        # should be called 1 (initial) + 8 (retries) = 9 times
        assert mock_func.call_count == 9

    @pytest.mark.asyncio
    async def test_all_retry(self):
        """Test multiple ``async_retry`` decorated coroutines being called at the same time.

        This test is similar to ``test_retries_by_itself()`` where it calls ``retryable()`` w/o
        ``await``. The only difference is that it tests multiple coroutines with multiple calls to
        better mimic the scenario in ``waterbutler.core.remote_logging.log_file_action()``.
        """

        mock_func_a = mock.Mock(side_effect=Exception())
        mock_func_b = mock.Mock(side_effect=Exception())
        retryable_a = utils.async_retry(4, 0)(mock_func_a)
        retryable_b = utils.async_retry(4, 0)(mock_func_b)

        retryable_a()
        retryable_b()
        retryable_a()
        retryable_b()

        await asyncio.sleep(.1)

        # Each call of `mock_func_a` or `mock_func_b` keeps failing until it reaches the maximum
        # retry limit which is 4; thus for either of the two, the total call count should be the
        # same: 2 (each has been called twice) * (1 (initial) + 4 (retries)) = 10 times.
        assert mock_func_a.call_count == 10
        assert mock_func_b.call_count == 10

    def test_docstring_survives(self):

        async def my_test():
            """This is a docstring"""
            pass

        retryable = utils.async_retry(8, 0)(my_test)

        assert retryable.__doc__ == """This is a docstring"""

    @pytest.mark.asyncio
    async def test_kwargs_work(self):

        async def mytest(mack, *args, **kwargs):
            mack()
            assert args == ('test', 'Foo')
            assert kwargs == {'test': 'Foo', 'baz': 'bam'}
            return True

        retryable = utils.async_retry(8, 0)(mytest)
        merk = mock.Mock(side_effect=[Exception(''), 5])

        fut = retryable(merk, 'test', 'Foo', test='Foo', baz='bam')
        assert await fut

        assert merk.call_count == 2


class TestContentDisposition:

    @pytest.mark.parametrize("filename,expected", [
        ('meow.txt', 'meow.txt'),
        ('résumé.txt', 'resume.txt'),
        (' ¿.surprise', ' .surprise'),
        ('a "file"', 'a \\"file\\"'),
        ('yes\\no', 'yes\\\\no'),
        ('ctrl\x09ch\x08ar', 'ctrl_ch_ar'),
    ])
    def test_strip_for_disposition(self, filename, expected):
        disposition = utils.strip_for_disposition(filename)
        assert disposition == expected

    @pytest.mark.parametrize("filename,expected", [
        (None, 'attachment'),
        ('foo.txt', "attachment; filename=\"foo.txt\"; filename*=UTF-8''foo.txt"),
        (' ¿.surprise', "attachment; filename=\" .surprise\"; filename*=UTF-8''%20%C2%BF.surprise"),
    ])
    def test_content_disposition(self, filename, expected):
        disposition = utils.make_disposition(filename)
        assert disposition == expected

    @pytest.mark.parametrize("filename,expected", [
        ('foo.txt', 'foo.txt'),
        ('résumé.docx', 'r%C3%A9sum%C3%A9.docx'),
        ('oh no/why+stop.txt', 'oh%20no/why%2Bstop.txt')
    ])
    def test_disposition_encoding(self, filename, expected):
        encoded = utils.encode_for_disposition(filename)
        assert encoded == expected
