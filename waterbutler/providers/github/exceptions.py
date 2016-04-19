from waterbutler.core.exceptions import ProviderError


class GitHubUnsupportedRepoError(ProviderError):
    def __init__(self):
        message = ('Some folder operations on large GitHub repositories cannot be supported without'
                   ' data loss.  To carry out this operation, please perform it in a local git'
                   ' repository, then push to the target repository on GitHub.')
        super().__init__(message, code=501)
