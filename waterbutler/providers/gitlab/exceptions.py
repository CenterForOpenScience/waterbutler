from waterbutler.core.exceptions import ProviderError


class GitLabUnsupportedRepoError(ProviderError):
    def __init__(self):
        message = ('Some folder operations on large GitLab repositories cannot be supported without'
                   ' data loss.  To carry out this operation, please perform it in a local git'
                   ' repository, then push to the target repository on GitLab.')
        super().__init__(message, code=501)
