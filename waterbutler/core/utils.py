import json
import pytz
import asyncio
import logging
import functools
import dateutil.parser
# from concurrent.futures import ProcessPoolExecutor  TODO Get this working

import aiohttp
from raven import Client
from stevedore import driver

from waterbutler.settings import config
from waterbutler.core import exceptions
from waterbutler.server import settings as server_settings
from waterbutler.core.signing import Signer
from waterbutler.core.streams import EmptyStream


logger = logging.getLogger(__name__)

signer = Signer(server_settings.HMAC_SECRET, server_settings.HMAC_ALGORITHM)

sentry_dsn = config.get_nullable('SENTRY_DSN', None)
client = Client(sentry_dsn) if sentry_dsn else None


def make_provider(name: str, auth: dict, credentials: dict, settings: dict, **kwargs):
    """Returns an instance of :class:`waterbutler.core.provider.BaseProvider`

    :param str name: The name of the provider to instantiate. (s3, box, etc)
    :param dict auth:
    :param dict credentials:
    :param dict settings:
    :param dict \*\*kwargs: currently there to absorb ``callback_url``

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
        async def wrapped(*args, __retries=0, **kwargs):
            try:
                return await asyncio.coroutine(func)(*args, **kwargs)
            except exceptions as e:
                if __retries < retries:
                    wait_time = backoff * __retries
                    logger.warning('Task {0} failed with {1!r}, {2} / {3} retries. Waiting {4} seconds before retrying'.format(func, e, __retries, retries, wait_time))
                    await asyncio.sleep(wait_time)
                    return await wrapped(*args, __retries=__retries + 1, **kwargs)
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


async def send_signed_request(method, url, payload):
    message, signature = signer.sign_payload(payload)
    return (await aiohttp.request(
        method, url,
        data=json.dumps({
            'payload': message.decode(),
            'signature': signature,
        }),
        headers={'Content-Type': 'application/json'},
    ))


def normalize_datetime(date_string):
    if date_string is None:
        return None
    parsed_datetime = dateutil.parser.parse(date_string)
    if not parsed_datetime.tzinfo:
        parsed_datetime = parsed_datetime.replace(tzinfo=pytz.UTC)
    parsed_datetime = parsed_datetime.astimezone(tz=pytz.UTC)
    parsed_datetime = parsed_datetime.replace(microsecond=0)
    return parsed_datetime.isoformat()


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
        current = self.remaining.pop(0)
        path = self.provider.path_from_metadata(*current)
        if path.is_dir:
            items = await self.provider.metadata(path)
            if items:
                self.remaining.extend([
                    (path, item) for item in items
                ])
                return await self.__anext__()
            else:
                return path.path.replace(self.parent_path.path, '', 1), EmptyStream()

        return path.path.replace(self.parent_path.path, '', 1), await self.provider.download(path)


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
