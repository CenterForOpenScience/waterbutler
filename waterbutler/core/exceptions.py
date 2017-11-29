import json
from http import HTTPStatus


DEFAULT_ERROR_MSG = 'An error occurred while making a {response.method} request to {response.url}'


class WaterButlerError(Exception):
    """The base exception that all others are subclasses of. Provides ``__str__`` and ``__repr__``.

    Exceptions in WaterButler need to be able to survive a pickling/unpickling process.  Because of
    a quirk in the implementation of exceptions, an unpickled exception will have its ``__init__``
    method called with the same positional arguments that ``Exception.__init__()`` is called with.
    Since WaterButlerError calls ``Exception.__init__()`` with the integer status code, all of its
    children must handle being initialized with a single integer positional argument.  IOW,
    ``ChildOfWaterButlerError.__init__(999)`` must always succeed, even if the result error message
    is nonsense.  After calling ``__init__``, the unpickling process will update the exception's
    internal ``__dict__`` to the same state as before pickling, so the exception will end up being
    accurate/meaningful/sensible.

    **In summary:**

    * No child of WaterButlerError can have a signature with anything other than one positional
      argument.

    * It must not perform any methods on the positional arg that are not compatible with integers.

    * kwargs are not passed as part of the faux __init__ call, so a class must be able to be
      instantiated with defaults only.

    * It is not necessary that the exception be meaningful when called this way.  It will be made
      consistent after initialization.

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
    def __init__(self, method, supported=None):

        if supported is None:
            supported_methods = 'unspecified'
        else:
            supported_methods = ', '.join(list(supported)).upper()

        super().__init__('Method "{}" not supported, currently supported methods '
                         'are {}'.format(method, supported_methods),
                         code=HTTPStatus.METHOD_NOT_ALLOWED, is_user_error=True)


class UnsupportedActionError(WaterButlerError):
    """An unsupported Action was used.
    """
    def __init__(self, method, supported=None):

        if supported is None:
            supported_actions = 'unspecified'
        else:
            supported_actions = ', '.join(list(supported)).upper()

        super().__init__('Action "{}" not supported, currently supported actions '
                         'are {}'.format(method, supported_actions),
                         code=HTTPStatus.BAD_REQUEST, is_user_error=True)


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
    def __init__(self, name):
        super().__init__('Cannot create folder "{}", because a file or folder already exists '
                         'with that name'.format(name), code=HTTPStatus.CONFLICT,
                         is_user_error=True)


class NamingConflict(ProviderError):
    def __init__(self, name):
        super().__init__('Cannot complete action: file or folder "{}" already exists in this '
                         'location'.format(name), code=HTTPStatus.CONFLICT, is_user_error=True)


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


class UninitializedRepositoryError(ProviderError):
    """Error for providers that wrap VCS systems (GitHub, Bitbucket, GitLab, etc). Indicates that
    the user has not yet initialized their repository, and that WB cannot operate on it until it
    has been initialized"""
    def __init__(self, repo_name, is_user_error=True, **kwargs):
        super().__init__(('The "{}" repository has not yet been initialized. Please do so before '
                         'attempting to access it.'.format(repo_name)),
                         code=HTTPStatus.BAD_REQUEST,
                         is_user_error=is_user_error)


class UnexportableFileTypeError(DownloadError):
    def __init__(self, path, message=None, is_user_error=True):
        if not message:
            message = 'The file "{}" is not exportable'.format(path)
        super().__init__(message, code=HTTPStatus.BAD_REQUEST, is_user_error=is_user_error)


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
