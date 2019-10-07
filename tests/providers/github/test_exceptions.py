import pytest

from waterbutler.providers.github.exceptions import (GitHubUnsupportedRepoError,
                                                     GitHubRateLimitExceededError, )


class TestExceptionSerialization:

    @pytest.mark.parametrize(
        'exception_class',
        [(GitHubUnsupportedRepoError), (GitHubRateLimitExceededError),]
    )
    def test_tolerate_dumb_signature(self, exception_class):
        """In order for WaterButlerError-inheriting exceptions to survive pickling/unpickling, it is
        necessary for them to be able to be instantiated with a single integer arg.  The reasons for
        this are described in the docstring for `waterbutler.core.exceptions.WaterButlerError`.
        """

        try:
            i_live_but_why = exception_class(616)
        except Exception as exc:
            pytest.fail(str(exc))

        assert isinstance(i_live_but_why, exception_class)

    def test_date_formatting(self):
        """Verify two-digit padding for month, day, hour, minute, second"""
        exc = GitHubRateLimitExceededError(8)
        assert '1970-01-01T00:00:08+00:00' in exc.message
