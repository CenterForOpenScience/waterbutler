import time
from http import HTTPStatus

from waterbutler.core.exceptions import ProviderError


class GitHubUnsupportedRepoError(ProviderError):

    def __init__(self, dummy) -> None:
        """``dummy`` argument is because children of ``WaterButlerError`` must be instantiable with
        a single integer argument.  See :class:`waterbutler.core.exceptions.WaterButlerError` for
        details.
        """

        super().__init__('Some folder operations on large GitHub repositories cannot be supported '
                         'without data loss.  To carry out this operation, please perform it in a '
                         'local git repository, then push to the target repository on GitHub.',
                         code=HTTPStatus.NOT_IMPLEMENTED)


class GitHubRateLimitExceededError(ProviderError):

    def __init__(self, retry: int) -> None:

        # TODO: should we add more information to this exception, e.g. which OSF user or GH account?
        retry_date = time.gmtime(retry)
        iso_date = time.strftime('%Y-%m-%dT%H:%M:%S+00:00', retry_date)

        super().__init__('WB has exceeded GitHub\'s rate limit for this user.  New quota will be '
                         'available after the limit gets reset at {}.'.format(iso_date),
                         code=HTTPStatus.SERVICE_UNAVAILABLE, is_user_error=True)
