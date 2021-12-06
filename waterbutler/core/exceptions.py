import json
from http import HTTPStatus

from aiohttp.client_exceptions import ContentTypeError

from waterbutler.server import settings


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


class TooManyRequests(WaterButlerError):
    """Indicates the user has sent too many requests in a given amount of time and is being
    rate-limited. Thrown as HTTP 429, ``Too Many Requests``. Exception response includes headers to
    inform user when to try again. Headers are:

    * ``Retry-After``: Epoch time after which the rate-limit is reset
    * ``X-Waterbutler-RateLimiting-Window``: The number of seconds after the first request when\
    the limit resets
    * ``X-Waterbutler-RateLimiting-Limit``: Total number of requests that may be sent within the\
    window
    * ``X-Waterbutler-RateLimiting-Remaining``: How many more requests can be sent during the window
    * ``X-Waterbutler-RateLimiting-Reset``: Seconds until the rate-limit is reset
    """
    def __init__(self, data):
        if type(data) != dict:
            message = ('Too many requests issued, but error lacks necessary data to build proper '
                       'response. Got:({})'.format(data))
        else:
            message = {
                'error': 'API rate-limiting active due to too many requests',
                'headers': {
                    'Retry-After': data['retry_after'],
                    'X-Waterbutler-RateLimiting-Window': settings.RATE_LIMITING_FIXED_WINDOW_SIZE,
                    'X-Waterbutler-RateLimiting-Limit': settings.RATE_LIMITING_FIXED_WINDOW_LIMIT,
                    'X-Waterbutler-RateLimiting-Remaining': data['remaining'],
                    'X-Waterbutler-RateLimiting-Reset': data['reset'],
                },
            }
        super().__init__(message, code=HTTPStatus.TOO_MANY_REQUESTS, is_user_error=True)


class WaterButlerRedisError(WaterButlerError):
    """Indicates the Redis server has returned an error. Thrown as HTTP 503, ``Service Unavailable``
    """
    def __init__(self, redis_command):

        message = {
            'error': 'The Redis server failed when processing command {}'.format(redis_command),
        }
        super().__init__(message, code=HTTPStatus.SERVICE_UNAVAILABLE, is_user_error=False)


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


class RetryChunkedUploadCommit(WaterButlerError):
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


class UploadFailedError(ProviderError):
    def __init__(self, message=None, code=HTTPStatus.INTERNAL_SERVER_ERROR):
        if message is None:
            message = 'Upload Failed'
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


class InvalidProviderConfigError(ProviderError):
    """Error for provider init failure due to invalid (include missing) settings and credentials"""
    def __init__(self, provider_name, message=None):
        base_message = 'Invalid provider configuration for {}'.format(provider_name)
        if not message:
            message = base_message
        else:
            message = '{}: {}'.format(base_message, message)
        super().__init__(message, code=HTTPStatus.BAD_REQUEST, is_user_error=False)


async def exception_from_response(resp, error=UnhandledProviderError, **kwargs):
    r"""Build and return, not raise, an exception from a response object.

    Quirks:

    ``resp.json()`` will throw the warning "Attempt to decode JSON with unexpected mimetype: %s" if
    the "Content-Type" is not of a json type.  The warning is an expected one.  However, providers
    such as S3 and GoogleCloud which use XML API endpoints pollute the WB celery log with this
    warning when checking file existence before each copy/move.  In addition, some 404 responses
    have a string body regardless of the provided "Content-Type".

    Current fix is skip parsing the body for HEAD requests.  We cannot simply rely on the header
    "Content-Type" to decide whether to call ``.json()`` or ``.read()``.  The warning should be
    logged at a level that it is useful but not annoying.

    :param resp: An ``aiohttp.ClientResponse`` stream with a non-200 range status
    :type resp: :class:`aiohttp.ClientResponse`
    :param Exception error: The type of exception to be raised
    :param dict \*\*kwargs: Additional context is ignored
    :rtype: :class:`WaterButlerError`
    """

    if resp.method.upper() == 'HEAD':
        # HEAD requests must have an empty body
        await resp.release()
        return error(DEFAULT_ERROR_MSG.format(response=resp), code=resp.status)

    try:
        # Try to parse response body as JSON.
        data = await resp.json()
        return error(data, code=resp.status)
    # TODO: double check whether we should remove `TypeError` after adding `ContentTypeError`
    except (TypeError, json.JSONDecodeError, ContentTypeError):
        pass

    try:
        # Try to parse response as String.
        data = await resp.read()
        return error({'response': data.decode('utf-8')}, code=resp.status)
    except TypeError:
        pass

    # When all else fails, return the most generic return message.
    await resp.release()
    return error(DEFAULT_ERROR_MSG.format(response=resp), code=resp.status)
