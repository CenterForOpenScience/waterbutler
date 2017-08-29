import os
import http
import json


DEFAULT_ERROR_MSG = 'An error occurred while making a {response.method} request to {response.url}'


class WaterButlerError(Exception):
    """The base exception that all other are a subclass of.
    Provides str repr and additional helper to convert exceptions
    to HTTPResponses.
    """

    def __init__(self, message, code=500, log_message=None, is_user_error=False):
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
    Exception or a subclass thereof
    """
    def __init__(self, message, code=400):
        super().__init__(message, code=code)


class PluginError(WaterButlerError):
    """WaterButler related errors raised
    from a plugins should inherit from PluginError
    """


class AuthError(PluginError):
    """WaterButler related errors raised
    from a :class:`waterbutler.core.auth` should
    inherit from AuthError
    """


class UnsupportedHTTPMethodError(PluginError):
    """An unsupported HTTP method was used
    """
    def __init__(self, method_used, supported_methods):
        supported_methods = ', '.join(list(supported_methods)).upper()
        super().__init__(
            'Method "{method_used}" not supported, currently supported methods '
            'are {supported_methods}'.format(method_used=method_used,
                                             supported_methods=supported_methods),
            code=405,
        )


class ProviderError(PluginError):
    """The WaterButler related errors raised
    from a :class:`waterbutler.core.provider` should
    inherit from ProviderError
    """


class ProviderNotFound(ProviderError):
    def __init__(self, provider):
        super().__init__('Provider "{}" not found'.format(provider), code=404)


class CopyError(ProviderError):
    pass


class CreateFolderError(ProviderError):
    pass


class DeleteError(ProviderError):
    pass


class DownloadError(ProviderError):
    pass


class IntraCopyError(ProviderError):
    pass


class IntraMoveError(ProviderError):
    pass


class MoveError(ProviderError):
    pass


class UploadError(ProviderError):
    pass


class MetadataError(ProviderError):
    pass


class RevisionsError(ProviderError):
    pass


class FolderNamingConflict(ProviderError):
    def __init__(self, path, name=None, is_user_error=True):
        super().__init__(
            'Cannot create folder "{name}" because a file or folder already exists at path "{path}"'.format(
                path=path,
                name=name or os.path.split(path.strip('/'))[1]
            ), code=409, is_user_error=is_user_error,
        )


class NamingConflict(ProviderError):
    def __init__(self, path, name=None, is_user_error=True):
        super().__init__(
            'Cannot complete action: file or folder "{name}" already exists in this location'.format(
                name=name or path.name
            ), code=409, is_user_error=is_user_error,
        )


class NotFoundError(ProviderError):
    def __init__(self, path, is_user_error=True):
        super().__init__(
            'Could not retrieve file or directory {}'.format(path),
            code=http.client.NOT_FOUND,
            is_user_error=is_user_error,
        )


class InvalidPathError(ProviderError):
    def __init__(self, message, is_user_error=True):
        super().__init__(message, code=http.client.BAD_REQUEST, is_user_error=is_user_error)


class OverwriteSelfError(InvalidParameters):
    def __init__(self, path):
        super().__init__('Unable to move or copy \'{}\'. Moving or copying a file or '
                         'folder onto itself is not supported.'.format(path))


class UnsupportedOperationError(ProviderError):
    def __init__(self, message, is_user_error=True):
        if not message:
            message = 'The requested operation is not supported by WaterButler.'
        super().__init__(message, code=http.client.FORBIDDEN, is_user_error=is_user_error)


class ReadOnlyProviderError(ProviderError):
    def __init__(self, provider):
        super().__init__('Provider "{}" is read-only'.format(provider), code=501)


async def exception_from_response(resp, error=ProviderError, **kwargs):
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
