import pytest

from waterbutler.providers.github import exceptions



class TestExceptionSerialization:

    @pytest.mark.parametrize('exception_class', [
        (exceptions.GitHubUnsupportedRepoError),
    ])
    def test_tolerate_dumb_signature(self, exception_class):
        """In order for WaterButlerError-inheriting exception classes to survive
        pickling/unpickling, it is necessary for them to be able to be instatiated with
         a single integer arg.  The reasons for this are described in the docstring for
        `waterbutler.core.exceptions.WaterButlerError`.
        """
        try:
            i_live_but_why = exception_class(616)
        except Exception as exc:
            pytest.fail(str(exc))

        assert isinstance(i_live_but_why, exception_class)
