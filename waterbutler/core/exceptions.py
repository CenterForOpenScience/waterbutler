import os
import json
from http import HTTPStatus


DEFAULT_ERROR_MSG = 'An error occurred while making a {response.method} request to {response.url}'


class WaterButlerError(Exception):
    """The base exception that all others are subclasses of. Provides ``__str__`` and ``__repr__``.
    """

    def __init__(self, message, code=HTTPStatus.INTERNAL_SERVER_ERROR, log_message=None,
                 is_user_error=False):
        super().__init__(code)
        self.code = code
        self.log_message = log_message
        self.is_user_error = is_user_error
        if isinstance(message, dict):
            self.data = message
            self.message = json.dumps(message)
        else:
            self.data = None
            self.message = message

    def __repr__(self):
        return '<{}({}, {})>'.format(self.__class__.__name__, self.code, self.message)

    def __str__(self):
        return '{}, {}'.format(self.code, self.message)


class InvalidParameters(WaterButlerError):
    """Errors regarding incorrect data being sent to a method should raise either this
    Exception or a subclass thereof.  Defaults status code to 400, Bad Request.
    """
    def __init__(self, message, code=HTTPStatus.BAD_REQUEST):
        super().__init__(message, code=code)


class UnsupportedHTTPMethodError(WaterButlerError):
    """An unsupported HTTP method was used.
    """
    def __init__(self, method_used, supported_methods):
        supported_methods = ', '.join(list(supported_methods)).upper()
        super().__init__(
            'Method "{method_used}" not supported, currently supported methods '
            'are {supported_methods}'.format(method_used=method_used,
                                             supported_methods=supported_methods),
            code=HTTPStatus.METHOD_NOT_ALLOWED,
        )


class PluginError(WaterButlerError):
    """WaterButler-related errors raised from a plugin, such as an auth handler or provider, should
    inherit from `PluginError`.
    """
    pass


class AuthError(PluginError):
    """WaterButler-related errors raised from :class:`waterbutler.core.auth.BaseAuthHandler`
    should inherit from AuthError.
    """
    pass


class ProviderError(PluginError):
    """WaterButler-related errors raised from :class:`waterbutler.core.provider.BaseProvider`
    should inherit from ProviderError.
    """
    pass


class UnhandledProviderError(ProviderError):
    """Errors inheriting from UnhandledProviderError represent unanticipated status codes received
    from the provider's API.  These are the only ones that should be passed to the ``throws``
    argument of `make_request`.  All have the same signature, ``(message, code: int=500)`` and are
    instantiated by the `exception_from_response` method at the end of this module.

    Developer-defined errors should **not** inherit from `UnhandledProviderError`.
    """
    pass


class CopyError(UnhandledProviderError):
    pass


class CreateFolderError(UnhandledProviderError):
    pass


class DeleteError(UnhandledProviderError):
    pass


class DownloadError(UnhandledProviderError):
    pass


class IntraCopyError(UnhandledProviderError):
    pass


class IntraMoveError(UnhandledProviderError):
    pass


class MoveError(UnhandledProviderError):
    pass


class MetadataError(UnhandledProviderError):
    pass


class RevisionsError(UnhandledProviderError):
    pass


class UploadError(UnhandledProviderError):
    pass


class FolderNamingConflict(ProviderError):
    def __init__(self, path, code=HTTPStatus.CONFLICT, name=None, is_user_error=True):
        super().__init__('Cannot create folder "{name}" because a file or folder already exists '
                         'at path "{path}"'.format(
                             path=path,
                             name=name or os.path.split(path.strip('/'))[1]
                         ), code=code, is_user_error=is_user_error)


class NamingConflict(ProviderError):
    def __init__(self, path, code=HTTPStatus.CONFLICT, name=None, is_user_error=True):
        super().__init__('Cannot complete action: file or folder "{name}" already exists in this '
                         'location'.format(name=name or path.name), code=code,
                         is_user_error=is_user_error)


class ProviderNotFound(ProviderError):
    def __init__(self, provider):
        super().__init__('Provider "{}" not found'.format(provider), code=HTTPStatus.NOT_FOUND)


class UploadChecksumMismatchError(ProviderError):
    def __init__(self, message=None, code=HTTPStatus.INTERNAL_SERVER_ERROR):
        if message is None:
            message = "Calculated and received hashes don't match"
        super().__init__(message, code=code)


class NotFoundError(ProviderError):
    def __init__(self, path, code=HTTPStatus.NOT_FOUND, is_user_error=True):
        super().__init__(
            'Could not retrieve file or directory {}'.format(path),
            code=code,
            is_user_error=is_user_error,
        )


class InvalidPathError(ProviderError):
    def __init__(self, message, code=HTTPStatus.BAD_REQUEST, is_user_error=True):
        super().__init__(message, code=code, is_user_error=is_user_error)


class OverwriteSelfError(InvalidParameters):
    def __init__(self, path):
        super().__init__('Unable to move or copy \'{}\'. Moving or copying a file or '
                         'folder onto itself is not supported.'.format(path))


class UnsupportedOperationError(ProviderError):
    def __init__(self, message, code=HTTPStatus.FORBIDDEN, is_user_error=True):
        if not message:
            message = 'The requested operation is not supported by WaterButler.'
        super().__init__(message, code=code, is_user_error=is_user_error)


class ReadOnlyProviderError(ProviderError):
    def __init__(self, provider, code=501):
        super().__init__('Provider "{}" is read-only'.format(provider), code=code)


async def exception_from_response(resp, error=UnhandledProviderError, **kwargs):
    """Build and return, not raise, an exception from a response object

    :param Response resp: An aiohttp.Response stream with a non 200 range status
    :param Exception error: The type of exception to be raised
    :param dict \*\*kwargs: Additional context to extract information from

    :rtype :class:`WaterButlerError`:
    """
    try:
        # Try to make an exception from our received json
        data = await resp.json()
        return error(data, code=resp.status)
    except Exception:
        pass

    try:
        data = await resp.read()
        return error({'response': data.decode('utf-8')}, code=resp.status)
    except TypeError:
        pass

    # When all else fails return the most generic return message
    return error(DEFAULT_ERROR_MSG.format(response=resp), code=resp.status)
