import json
import asyncio
import logging
import functools
# from concurrent.futures import ProcessPoolExecutor  TODO Get this working

import aiohttp

from raven.contrib.tornado import AsyncSentryClient
from stevedore import driver

from waterbutler import settings
from waterbutler.core import exceptions
from waterbutler.server import settings as server_settings
from waterbutler.core.signing import Signer


logger = logging.getLogger(__name__)

sentry_dns = settings.get('SENTRY_DSN', None)
signer = Signer(server_settings.HMAC_SECRET, server_settings.HMAC_ALGORITHM)


class AioSentryClient(AsyncSentryClient):

    def send_remote(self, url, data, headers=None, callback=None):
        headers = headers or {}
        if not self.state.should_try():
            message = self._get_log_message(data)
            self.error_logger.error(message)
            return

        future = aiohttp.request('POST', url, data=data, headers=headers)
        asyncio.async(future)


if sentry_dns:
    client = AioSentryClient(sentry_dns)
else:
    client = None


def make_provider(name, auth, credentials, settings):
    """Returns an instance of :class:`waterbutler.core.provider.BaseProvider`

    :param str name: The name of the provider to instantiate. (s3, box, etc)
    :param dict auth:
    :param dict credentials:
    :param dict settings:

    :rtype: :class:`waterbutler.core.provider.BaseProvider`
    """
    try:
        manager = driver.DriverManager(
            namespace='waterbutler.providers',
            name=name,
            invoke_on_load=True,
            invoke_args=(auth, credentials, settings),
        )
    except RuntimeError:
        raise exceptions.ProviderNotFound(name)

    return manager.driver


def as_task(func):
    if not asyncio.iscoroutinefunction(func):
        func = asyncio.coroutine(func)

    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        return asyncio.async(func(*args, **kwargs))

    return wrapped


def async_retry(retries=5, backoff=1, exceptions=(Exception, ), raven=client):

    def _async_retry(func):

        @as_task
        @functools.wraps(func)
        def wrapped(*args, __retries=0, **kwargs):
            try:
                return (yield from asyncio.coroutine(func)(*args, **kwargs))
            except exceptions as e:
                if __retries < retries:
                    wait_time = backoff * __retries
                    logger.warning('Task {0} failed with {1!r}, {2} / {3} retries. Waiting {4} seconds before retrying'.format(func, e, __retries, retries, wait_time))

                    yield from asyncio.sleep(wait_time)
                    return wrapped(*args, __retries=__retries + 1, **kwargs)
                else:
                    # Logs before all things
                    logger.error('Task {0} failed with exception {1}'.format(func, e))

                    if raven:
                        # Only log if a raven client exists
                        client.captureException()

                    # If anything happens to be listening
                    raise e

        # Retries must be 0 to start with
        # functools partials dont preserve docstrings
        return wrapped

    return _async_retry


@asyncio.coroutine
def send_signed_request(method, url, payload):
    message, signature = signer.sign_payload(payload)
    return (yield from aiohttp.request(
        method, url,
        data=json.dumps({
            'payload': message.decode(),
            'signature': signature,
        }),
        headers={'Content-Type': 'application/json'},
    ))


class ZipStreamGenerator:
    def __init__(self, provider, parent_path, *metadata_objs):
        self.provider = provider
        self.parent_path = parent_path
        self.remaining = [
            (parent_path, metadata)
            for metadata in metadata_objs
        ]

    async def __aiter__(self):
        return self

    async def __anext__(self):
        if not self.remaining:
            raise StopAsyncIteration
        path = self.provider.path_from_metadata(*self.remaining.pop(0))
        if path.is_dir:
            self.remaining.extend([
                (path, item) for item in
                await self.provider.metadata(path)
            ])
            return await self.__anext__()

        return path.path.replace(self.parent_path.path, ''), await self.provider.download(path)


class RequestHandlerContext:

    def __init__(self, request_coro):
        self.request = None
        self.request_coro = request_coro

    async def __aenter__(self):
        self.request = await self.request_coro
        return self.request

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.request.release()
        if exc_type:
            raise exc_val.with_traceback(exc_tb)


class AsyncIterator:
    """A wrapper class that makes normal iterators
    look like async iterators
    """

    def __init__(self, iterable):
        self.iterable = iter(iterable)

    async def __aiter__(self):
        return self.iterable

    async def __anext__(self):
        try:
            return next(self.iterable)
        except StopIteration:
            raise StopAsyncIteration
