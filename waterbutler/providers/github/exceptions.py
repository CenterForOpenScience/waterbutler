import time
from http import HTTPStatus

from waterbutler.core.exceptions import ProviderError


class GitHubUnsupportedRepoError(ProviderError):
    def __init__(self, dummy):
        """``dummy`` argument is because children of ``WaterButlerError`` must be instantiable with
        a single integer argument.  See :class:`waterbutler.core.exceptions.WaterButlerError`
        for details.
        """
        super().__init__('Some folder operations on large GitHub repositories cannot be supported '
                         'without data loss.  To carry out this operation, please perform it in a '
                         'local git repository, then push to the target repository on GitHub.',
                         code=HTTPStatus.NOT_IMPLEMENTED)


class GitHubRateLimitExceededError(ProviderError):
    def __init__(self, retry: int):
        retry_date = time.gmtime(retry)
        iso_date = '{}-{}-{}T{}:{}:{}+00:00'.format(retry_date.tm_year, retry_date.tm_mon,
                                                    retry_date.tm_mday, retry_date.tm_hour,
                                                    retry_date.tm_min, retry_date.tm_sec)
        super().__init__('Rate limit exceeded. New quota will be available after {}.'.format(iso_date), code=HTTPStatus.SERVICE_UNAVAILABLE, is_user_error=True)
