import re
import json
import pytz
import asyncio
import logging
import functools
import unicodedata
import dateutil.parser
from urllib import parse
# from concurrent.futures import ProcessPoolExecutor  TODO Get this working

import aiohttp
import sentry_sdk
from stevedore import driver

from waterbutler.core import exceptions
from waterbutler.core.signing import Signer
from waterbutler.core.streams import EmptyStream
from waterbutler.server import settings as server_settings

logger = logging.getLogger(__name__)

signer = Signer(server_settings.HMAC_SECRET, server_settings.HMAC_ALGORITHM)


def make_provider(name: str, auth: dict, credentials: dict, settings: dict, **kwargs):
    r"""Returns an instance of :class:`waterbutler.core.provider.BaseProvider`

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
            invoke_kwds=kwargs,
        )
    except RuntimeError:
        raise exceptions.ProviderNotFound(name)

    return manager.driver


def as_task(func):
    if not asyncio.iscoroutinefunction(func):
        func = asyncio.coroutine(func)

    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        return asyncio.ensure_future(func(*args, **kwargs))

    return wrapped


def async_retry(retries=5, backoff=1, exceptions=(Exception, )):

    def _async_retry(func):

        @as_task
        @functools.wraps(func)
        async def wrapped(*args, __retries=0, **kwargs):
            try:
                return await asyncio.coroutine(func)(*args, **kwargs)
            except exceptions as e:
                if __retries < retries:
                    wait_time = backoff * __retries
                    logger.warning('Task {0} failed with {1!r}, {2} / {3} retries. Waiting '
                                   '{4} seconds before retrying'.format(func, e, __retries,
                                                                        retries, wait_time))
                    await asyncio.sleep(wait_time)
                    return await wrapped(*args, __retries=__retries + 1, **kwargs)
                else:
                    # Logs before all things
                    logger.error('Task {0} failed with exception {1}'.format(func, e))

                    with sentry_sdk.configure_scope() as scope:
                        scope.set_tag('debug', False)
                    sentry_sdk.capture_exception(e)

                    # If anything happens to be listening
                    raise e

        # Retries must be 0 to start with
        # functools partials dont preserve docstrings
        return wrapped

    return _async_retry


async def send_signed_request(method, url, payload):
    """Calculates a signature for a payload, then sends a request to the given url with the payload
    and signature.

    This method will read the response into memory before returning, so **DO NOT** use it if the
    response may be very large.  As of 2019-04-06, this function is only used by the callback logging
    code in waterbutler.core.remote_logging.
    """

    message, signature = signer.sign_payload(payload)

    async with aiohttp.request(
            method,
            url,
            data=json.dumps({
                'payload': message.decode(),
                'signature': signature,
            }),
            headers={'Content-Type': 'application/json'}
    ) as response:
        return response.status, await response.read()


def normalize_datetime(date_string):
    if date_string is None:
        return None
    parsed_datetime = dateutil.parser.parse(date_string)
    if not parsed_datetime.tzinfo:
        parsed_datetime = parsed_datetime.replace(tzinfo=pytz.UTC)
    parsed_datetime = parsed_datetime.astimezone(tz=pytz.UTC)
    parsed_datetime = parsed_datetime.replace(microsecond=0)
    return parsed_datetime.isoformat()


def strip_for_disposition(filename):
    """Convert given filename to a form useable by a non-extended parameter.

    The permitted characters allowed in a non-extended parameter are defined in RFC-2616, Section
    2.2.  This is a subset of the ascii character set. This function converts non-ascii characters
    to their nearest ascii equivalent or strips them if there is no equivalent.  It then replaces
    control characters with underscores and escapes blackslashes and double quotes.

    :param str filename: a filename to encode
    """

    nfkd_form = unicodedata.normalize('NFKD', filename)
    only_ascii = nfkd_form.encode('ASCII', 'ignore')
    no_ctrl = re.sub(r'[\x00-\x1f]', '_', only_ascii.decode('ascii'))
    return no_ctrl.replace('\\', '\\\\').replace('"', '\\"')


def encode_for_disposition(filename):
    """Convert given filename into utf-8 octets, then percent encode them.

    See RFC-5987, Section 3.2.1 for description of how to encode the ``value-chars`` portion of
    ``ext-value``. WB will always use utf-8 encoding (see `make_disposition`), so that encoding
    is hard-coded here.

    :param str filename: a filename to encode
    """
    return parse.quote(filename.encode('utf-8'))


def make_disposition(filename):
    """Generate the "Content-Disposition" header.

    Refer to https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Disposition for how
    to use the header correctly.  In the case where ARGUMENT ``filename`` exists, WB should use the
    DIRECTIVE ``filename*`` which uses encoding defined in RFC 5987 (see the link below).  Do not
    use the DIRECTIVE ``filename``.  This solves the issue of file names containing non-English and
    special characters

    Refer to https://tools.ietf.org/html/rfc5987 for the RFC 5978 mentioned above.  Please note that
    it has been replaced by RFC 8187 (https://tools.ietf.org/html/rfc8187) recently in Sept. 2017.
    As expected, there is nothing to worry about (see Appendix A in RFC 8187 for detailed changes).

    :param str filename: the name of the file to be downloaded AS
    :rtype: `str`
    :return: the value of the "Content-Disposition" header with filename*
    """
    if not filename:
        return 'attachment'
    else:
        stripped_filename = strip_for_disposition(filename)
        encoded_filename = encode_for_disposition(filename)
        return 'attachment; filename="{}"; filename*=UTF-8\'\'{}'.format(stripped_filename,
                                                                         encoded_filename)


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
