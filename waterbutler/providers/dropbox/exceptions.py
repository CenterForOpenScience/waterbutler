from http import HTTPStatus

from waterbutler.core.exceptions import ProviderError


class DropboxUnhandledConflictError(ProviderError):
    def __init__(self, error_data):
        super().__init__('Dropbox has many unique error messages for code 409 (Conflict), this '
                         'one was not specifically handled in the provider: {}'.format(error_data),
                         code=HTTPStatus.CONFLICT)


class DropboxNamingConflictError(ProviderError):
    def __init__(self, path):
        super().__init__('Cannot complete action: file or folder already exists at {}'.format(path),
                         code=HTTPStatus.CONFLICT)
