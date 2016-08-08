import json
import pytz
import time
import asyncio
import logging
import functools
import dateutil.parser
# from concurrent.futures import ProcessPoolExecutor  TODO Get this working

import aiohttp
from raven import Client
from stevedore import driver

from waterbutler import settings
from waterbutler.core import exceptions
from waterbutler.tasks import settings as task_settings
from waterbutler.server import settings as server_settings
from waterbutler.core.signing import Signer


logger = logging.getLogger(__name__)

signer = Signer(server_settings.HMAC_SECRET, server_settings.HMAC_ALGORITHM)

sentry_dsn = settings.get('SENTRY_DSN', None)
client = Client(sentry_dsn) if sentry_dsn else None


def make_provider(name, auth, credentials, settings, **kwargs):
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
                return (await asyncio.coroutine(func)(*args, **kwargs))
            except exceptions as e:
                if __retries < retries:
                    wait_time = backoff * __retries
                    logger.warning('Task {0} failed with {1!r}, {2} / {3} retries. Waiting {4} seconds before retrying'.format(func, e, __retries, retries, wait_time))

                    await asyncio.sleep(wait_time)
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


async def log_to_callback(action, source=None, destination=None, start_time=None, errors=[]):
    if action in ('download_file', 'download_zip'):
        logger.debug('Not logging for {} action'.format(action))
        return

    auth = getattr(destination, 'auth', source.auth)

    log_payload = {
        'action': action,
        'auth': auth,
        'time': time.time() + 60,
        'errors': errors,
    }

    if start_time:
        log_payload['email'] = time.time() - start_time > task_settings.WAIT_TIMEOUT

    if action in ('move', 'copy'):
        log_payload['source'] = source.serialize()
        log_payload['destination'] = destination.serialize()
    else:
        log_payload['metadata'] = source.serialize()
        log_payload['provider'] = log_payload['metadata']['provider']

    resp = await send_signed_request('PUT', auth['callback_url'], log_payload)
    resp_data = await resp.read()

    if resp.status // 100 != 2:
        raise Exception(
            'Callback for {} request failed with {!r}, got {}'.format(
                action, resp, resp_data.decode('utf-8')
            )
        )

    logger.info('Callback for {} request succeeded with {}'.format(action, resp_data.decode('utf-8')))


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


def _serialize_request(request):
    if request is None:
        return {}

    # temporary for development
    headers_dict = {}
    for (k, v) in sorted(request.headers.get_all()):
        if k not in ('Authorization', 'Cookie', 'User-Agent', ):
            headers_dict[k] = v

    serialized = {
        'ip': request.remote_ip,
        'method': request.method,
        'url': request.full_url(),
        'ua': request.headers['User-Agent'],
        'time': request.request_time(),
        'headers': headers_dict,
        'is_mfr_render': settings.MFR_IDENTIFYING_HEADER in request.headers,
    }

    if 'Referer' in request.headers:
        referrer = request.headers['Referer']
        serialized['referrer'] = referrer
        if referrer.startswith('{}/render'.format(settings.MFR_DOMAIN)):
            serialized['is_mfr_render'] = True

    return serialized
