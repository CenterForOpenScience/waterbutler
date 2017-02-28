from waterbutler.core.exceptions import ProviderError


class DropboxUnhandledConflictError(ProviderError):
    def __init__(self, error_data):
        message = ('Dropbox has many unique error messages for code 409(Conflict), this one was not specifically handled in the provider: {}'.format(error_data))
        super().__init__(message, code=409)


class DropboxNamingConflictError(ProviderError):
    def __init__(self, error_data):
        message = ('Cannot complete action: file or folder already exists in this location')
        super().__init__(message, code=409)
