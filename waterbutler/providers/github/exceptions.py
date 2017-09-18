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
