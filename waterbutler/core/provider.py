import abc
import time
import typing
import asyncio
import logging
import weakref
import functools
import itertools
from urllib import parse

import furl
import aiohttp
from aiohttp.client import _RequestContextManager

from waterbutler.core import streams
from waterbutler.core import exceptions
from waterbutler.core import path as wb_path
from waterbutler import settings as wb_settings
from waterbutler.core.metrics import MetricsRecord
from waterbutler.core import metadata as wb_metadata
from waterbutler.core.utils import ZipStreamGenerator
from waterbutler.core.utils import RequestHandlerContext


logger = logging.getLogger(__name__)
_THROTTLES = weakref.WeakKeyDictionary()  # type: weakref.WeakKeyDictionary


def throttle(concurrency=10, interval=1):
    def _throttle(func):
        @functools.wraps(func)
        async def wrapped(*args, **kwargs):
            if asyncio.get_event_loop() not in _THROTTLES:
                count, last_call, event = 0, time.time(), asyncio.Event()
                _THROTTLES[asyncio.get_event_loop()] = (count, last_call, event)
                event.set()
            else:
                count, last_call, event = _THROTTLES[asyncio.get_event_loop()]

            await event.wait()
            count += 1
            if count > concurrency:
                count = 0
                if (time.time() - last_call) < interval:
                    event.clear()
                    await asyncio.sleep(interval - (time.time() - last_call))
                    event.set()

            last_call = time.time()
            _THROTTLES[asyncio.get_event_loop()] = (count, last_call, event)
            return await func(*args, **kwargs)
        return wrapped
    return _throttle


def build_url(base, *segments, **query):
    url = furl.furl(base)
    # Filters return generators
    # Cast to list to force "spin" it
    url.path.segments = list(filter(
        lambda segment: segment,
        map(
            # Furl requires everything to be quoted or not, no mixtures allowed
            # prequote everything so %signs don't break everything
            lambda segment: parse.quote(segment.strip('/')),
            # Include any segments of the original url, effectively list+list but returns a generator
            itertools.chain(url.path.segments, segments)
        )
    ))
    url.args = query
    return url.url


class BaseProvider(metaclass=abc.ABCMeta):
    """The base class for all providers. Every provider must, at the least, implement all abstract
    methods in this class.

    .. note::
        When adding a new provider you must add it to setup.py's
        `entry_points` under the `waterbutler.providers` key formatted
        as: `<provider name> = waterbutler.providers.yourprovider:<FullProviderName>`

        Keep in mind that `yourprovider` modules must export the provider class
    """

    BASE_URL = None

    def __init__(self, auth: dict,
                 credentials: dict,
                 settings: dict,
                 retry_on: typing.Set[int]={408, 502, 503, 504},
                 is_celery_task: bool=False) -> None:
        """
        :param auth: ( :class:`dict` ) Information about the user this provider will act on the behalf of
        :param credentials: ( :class:`dict` ) The credentials used to authenticate with the provider,
            ofter an OAuth 2 token
        :param settings: ( :class:`dict` ) Configuration settings for this provider,
            often folder or repo
        :param is_celery_task: ( :class:`bool` ) Was this provider built inside a celery task?
        """
        self._retry_on = retry_on
        self.auth = auth
        self.credentials = credentials
        self.settings = settings
        self.is_celery_task = is_celery_task

        self.provider_metrics = MetricsRecord('provider')
        self.provider_metrics.add('auth', auth)
        self.metrics = self.provider_metrics.new_subrecord(self.NAME)

        # The `.loop_session_map` ensures that only one session is created for one event loop per
        # provider instance.  On one hand, we can't just have one session for each provider instance
        # since actions such as move and copy are run in background probably with a different loop.
        # On the other hand, we can't have one session for each request since sessions are only
        # closed when the provider instance is destroyed. There would be too many for WB to handle.
        self.loop_session_map = weakref.WeakKeyDictionary()  # type: weakref.WeakKeyDictionary
        # The `.session_list` keeps track of all the sessions created for the provider instance so
        # that they can be properly closed upon instance destroy.
        self.session_list = []  # type: typing.List[aiohttp.ClientSession]

    def __del__(self):
        """
        Manually close all sessions created during the life of the provider instance.  Our code are
        a slightly modified version of how ``aiohttp-3.5.4`` closes sessions and connectors.

        1. sessions: https://github.com/aio-libs/aiohttp/blob/v3.5.4/aiohttp/client.py#L893
        2. connectors: https://github.com/aio-libs/aiohttp/blob/v3.5.4/aiohttp/connector.py#L389
            2.1 Update: https://github.com/aio-libs/aiohttp/pull/3417/files.

        Our implementation tries to avoid accessing protected members unless we can't.  For example,
        we use ``session.connector`` instead of ``session._connector``, and use ``session.detach()``
        instead of calling ``session._connector = None``.  We have to ``session._connector_owner``
        since it doesn't have an public property. We have to call ``connector._close()`` instead of
        ``connector.close()`` since ``aiohttp`` decided to make ``.close()`` async recently. Here is
        the PR: https://github.com/aio-libs/aiohttp/pull/3417/files.
        """
        for session in self.session_list:
            if not session.closed:
                if session.connector is not None and session._connector_owner:
                    session.connector._close()
                session.detach()

    @property
    @abc.abstractmethod
    def NAME(self) -> str:
        raise NotImplementedError

    def __eq__(self, other):
        try:
            return (
                type(self) == type(other) and
                self.credentials == other.credentials
            )
        except AttributeError:
            return False

    def serialized(self) -> dict:
        return {
            'name': self.NAME,
            'auth': self.auth,
            'settings': self.settings,
            'credentials': self.credentials,
        }

    def build_url(self, *segments, **query) -> str:
        r"""A nice wrapper around furl, builds urls based on self.BASE_URL

        :param \*segments: ( :class:`tuple` ) A tuple of strings joined into /foo/bar/..
        :param \*\*query: ( :class:`dict` ) A dictionary that will be turned into query parameters
        :rtype: :class:`str`
        """
        return build_url(self.BASE_URL, *segments, **query)

    @property
    def default_headers(self) -> dict:
        """Headers to be included with every request
        Commonly OAuth headers or Content-Type
        """
        return {}

    def build_headers(self, **kwargs) -> dict:
        headers = self.default_headers
        headers.update(kwargs)
        return {
            key: value
            for key, value in headers.items()
            if value is not None
        }

    def get_or_create_session(self, connector=None):
        """
        Obtain an existing session or create a new one for making requests.

        Quirks:

        Sessions must be carefully managed by WB.  On one hand, we can't just have one session for
        each provider instance since actions such as move and copy are run in background probably
        with a different loop.  On the other hand, we can't have one session for each request since
        sessions are only closed when the provider instance is destroyed.

        For providers that use a customized connector such as owncloud, the new session is created
        with the given connector; while an existing session simply ignores (and closes) the new
        connector.  Given that the session is per event loop and instance, the existing session if
        found must already have a connector with qualified customizations.

        :param connector: a customized connector
        :return: the one session that belongs to the current event loop
        :rtype: :class:`aiohttp.ClientSession`
        """
        loop = asyncio.get_event_loop()
        session = self.loop_session_map.get(loop, None)
        if not session:
            session = aiohttp.ClientSession(connector=connector)
            self.loop_session_map[loop] = session
            self.session_list.append(session)
        elif connector:
            # Ignore and close the kwarg connector if an existing session exists.
            connector._close()

        return session

    @throttle()
    async def make_request(self, method, url, *args, **kwargs):
        r"""
        A wrapper around seven HTTP request methods in :class:`aiohttp.ClientSession`.  It replaces
        the original ``.make_request()`` method which was a wrapper around :func:`aiohttp.request`.
        This change is due to aiohttp triple-major-version upgrade from version 0.18 to 3.5.4 where
        the main difference is the context manager (CM).

        Core Quirk:

        ``aiohttp3`` has explicitly provided two examples of making requests in the documentation.
        Using :func:`aiohttp.request` directly with CM and using :class:`aiohttp.ClientSession` and
        its HTTP methods with CM.  Unfortunately, an unpleasant side-effect of CM is that sessions
        and connections are closed outside CM.  This breaks WB's design where responses are passed
        from one provider to another.

        Not-so-smart Solution:

        By taking a look at the source code of ``aiohttp3``, it is discovered that requests can be
        made without CM although we are not sure why the documentation does not mention it at all.
        The trick / hack of this non-CM approach is that sessions must be carefully managed by WB.
        Please take a look at the following methods for detailed implementation.

        :func:`__init__()`: session list and event loop map initialization
        :func:`__del__()`: session and connection closing
        :func:`get_or_create_session()`: either get the current session or create a new one if not
        found when making a request

        :param method: ( :class:`str` ) The HTTP method
        :param url: The URL or URL-to-be to send the request to
        :type url: :class:`str` for the built URL or a :class:`functools.partial` object that will
            be build when it is called
        :param \*args: args passed to methods of :class:`aiohttp.ClientSession`
        :param \*\*kwargs: kwargs passed to methods of :class:`aiohttp.ClientSession` except the
            following ones that will be popped and used for Waterbutler specific purposes
        :keyword no_auth_header: ( :class:`bool` ) An optional boolean flag that determines whether
            to drop the default authorization header provided by the provider
        :keyword range: ( :class:`tuple` ) An optional tuple (start, end) that is transformed into
            a Range header
        :keyword expects: ( :class:`tuple` ) An optional tuple of HTTP status codes as integers
            raises an exception if the returned status code is not in it
        :keyword retry: ( :class:`int` ) An optional integer with default value 2 that determines
            how further to retry failed requests with the exponential back-off algorithm
        :keyword throws: ( :class:`Exception` ) The exception to be raised from expects
        :return: The HTTP response
        :rtype: :class:`aiohttp.ClientResponse`
        :raises: :class:`.UnhandledProviderError` Raised if expects is defined
        :raises: :class:`.WaterButlerError` Raised if invalid HTTP method is provided
        """

        kwargs['headers'] = self.build_headers(**kwargs.get('headers', {}))
        no_auth_header = kwargs.pop('no_auth_header', False)
        if no_auth_header:
            kwargs['headers'].pop('Authorization')
        retry = _retry = kwargs.pop('retry', 2)
        expects = kwargs.pop('expects', None)
        throws = kwargs.pop('throws', exceptions.UnhandledProviderError)
        byte_range = kwargs.pop('range', None)
        if byte_range:
            kwargs['headers']['Range'] = self._build_range_header(byte_range)
        connector = kwargs.pop('connector', None)
        session = self.get_or_create_session(connector=connector)

        method = method.upper()
        while retry >= 0:
            # Don't overwrite the callable ``url`` so that signed URLs are refreshed for every retry
            non_callable_url = url() if callable(url) else url
            try:
                self.provider_metrics.incr('requests.count')
                # TODO: use a `dict` to select methods with either `lambda` or `functools.partial`
                if method == 'GET':
                    response = await session.get(non_callable_url,
                                                 timeout=wb_settings.AIOHTTP_TIMEOUT,
                                                 *args, **kwargs)
                elif method == 'PUT':
                    response = await session.put(non_callable_url,
                                                 timeout=wb_settings.AIOHTTP_TIMEOUT,
                                                 *args, **kwargs)
                elif method == 'POST':
                    response = await session.post(non_callable_url,
                                                  timeout=wb_settings.AIOHTTP_TIMEOUT,
                                                  *args, **kwargs)
                elif method == 'HEAD':
                    response = await session.head(non_callable_url, *args, **kwargs)
                elif method == 'DELETE':
                    response = await session.delete(non_callable_url, **kwargs)
                elif method == 'PATCH':
                    response = await session.patch(non_callable_url, *args, **kwargs)
                elif method == 'OPTIONS':
                    response = await session.options(non_callable_url, *args, **kwargs)
                elif method in wb_settings.WEBDAV_METHODS:
                    # `aiohttp.ClientSession` only has functions available for native HTTP methods.
                    # For WebDAV (a protocol that extends HTTP) ones, WB lets the `ClientSession`
                    # instance call `_request()` directly and then wraps the return object with
                    # `aiohttp.client._RequestContextManager`.
                    response = await _RequestContextManager(
                        session._request(method, url, *args, **kwargs)
                    )
                else:
                    raise exceptions.WaterButlerError('Unsupported HTTP method ...')
                self.provider_metrics.incr('requests.tally.ok')
                if expects and response.status not in expects:
                    unexpected = await exceptions.exception_from_response(response,
                                                                          error=throws, **kwargs)
                    raise unexpected
                return response
            except throws as e:
                self.provider_metrics.incr('requests.tally.nok')
                if retry <= 0 or e.code not in self._retry_on:
                    raise
                await asyncio.sleep((1 + _retry - retry) * 2)
                retry -= 1

    def request(self, *args, **kwargs):
        return RequestHandlerContext(self.make_request(*args, **kwargs))

    async def move(self,
                   dest_provider: 'BaseProvider',
                   src_path: wb_path.WaterButlerPath,
                   dest_path: wb_path.WaterButlerPath,
                   rename: str=None,
                   conflict: str='replace',
                   handle_naming: bool=True) -> typing.Tuple[wb_metadata.BaseMetadata, bool]:
        """Moves a file or folder from the current provider to the specified one
        Performs a copy and then a delete.
        Calls :func:`BaseProvider.intra_move` if possible.

        :param dest_provider: ( :class:`.BaseProvider` ) The provider to move to
        :param src_path: ( :class:`.WaterButlerPath` ) Path to where the resource can be found
        :param dest_path: ( :class:`.WaterButlerPath` ) Path to where the resource will be moved
        :param rename: ( :class:`str` ) The desired name of the resulting path, may be incremented
        :param conflict: ( :class:`str` ) What to do in the event of a name conflict, ``replace`` or ``keep``
        :param handle_naming: ( :class:`bool` ) If a naming conflict is detected, should it be automatically handled?
        """
        args = (dest_provider, src_path, dest_path)
        kwargs = {'rename': rename, 'conflict': conflict}

        self.provider_metrics.add('move', {
            'got_handle_naming': handle_naming,
            'conflict': conflict,
            'got_rename': rename is not None,
        })

        if handle_naming:
            dest_path = await dest_provider.handle_naming(
                src_path,
                dest_path,
                rename=rename,
                conflict=conflict,
            )
            args = (dest_provider, src_path, dest_path)
            kwargs = {}

        # files and folders shouldn't overwrite themselves
        if (
            self.shares_storage_root(dest_provider) and
            src_path.materialized_path == dest_path.materialized_path
        ):
            raise exceptions.OverwriteSelfError(src_path)

        self.provider_metrics.add('move.can_intra_move', False)
        if self.can_intra_move(dest_provider, src_path):
            self.provider_metrics.add('move.can_intra_move', True)
            return await self.intra_move(*args)

        if src_path.is_dir:
            meta_data, created = await self._folder_file_op(self.move, *args, **kwargs)  # type: ignore
        else:
            meta_data, created = await self.copy(*args, handle_naming=False, **kwargs)  # type: ignore

        await self.delete(src_path)

        return meta_data, created

    async def copy(self,
                   dest_provider: 'BaseProvider',
                   src_path: wb_path.WaterButlerPath,
                   dest_path: wb_path.WaterButlerPath,
                   rename: str=None, conflict: str='replace',
                   handle_naming: bool=True) \
            -> typing.Tuple[wb_metadata.BaseMetadata, bool]:
        args = (dest_provider, src_path, dest_path)
        kwargs = {'rename': rename, 'conflict': conflict, 'handle_naming': handle_naming}

        self.provider_metrics.add('copy', {
            'got_handle_naming': handle_naming,
            'conflict': conflict,
            'got_rename': rename is not None,
        })
        if handle_naming:
            dest_path = await dest_provider.handle_naming(
                src_path,
                dest_path,
                rename=rename,
                conflict=conflict,
            )
            args = (dest_provider, src_path, dest_path)
            kwargs = {}

        # files and folders shouldn't overwrite themselves
        if (
                self.shares_storage_root(dest_provider) and
                src_path.materialized_path == dest_path.materialized_path
        ):
            raise exceptions.OverwriteSelfError(src_path)

        self.provider_metrics.add('copy.can_intra_copy', False)
        if self.can_intra_copy(dest_provider, src_path):
            self.provider_metrics.add('copy.can_intra_copy', True)
            return await self.intra_copy(*args)

        if src_path.is_dir:
            return await self._folder_file_op(self.copy, *args, **kwargs)  # type: ignore

        download_stream = await self.download(src_path)

        if getattr(download_stream, 'name', None):
            dest_path.rename(download_stream.name)

        return await dest_provider.upload(download_stream, dest_path)

    async def _folder_file_op(self,
                              func: typing.Callable,
                              dest_provider: 'BaseProvider',
                              src_path: wb_path.WaterButlerPath,
                              dest_path: wb_path.WaterButlerPath,
                              **kwargs) -> typing.Tuple[wb_metadata.BaseFolderMetadata, bool]:
        """Recursively apply func to src/dest path.

        Called from: func: copy and move if src_path.is_dir.

        Calls: func: dest_provider.delete and notes result for bool: created
               func: dest_provider.create_folder
               func: dest_provider.revalidate_path
               func: self.metadata

        :param coroutine func: to be applied to src/dest path
        :param *Provider dest_provider: Destination provider
        :param *ProviderPath src_path: Source path
        :param *ProviderPath dest_path: Destination path
        """
        assert src_path.is_dir, 'src_path must be a directory'
        assert asyncio.iscoroutinefunction(func), 'func must be a coroutine'

        try:
            await dest_provider.delete(dest_path)
            created = False
        except exceptions.ProviderError as e:
            if e.code != 404:
                raise
            created = True

        folder = await dest_provider.create_folder(dest_path, folder_precheck=False)

        dest_path = await dest_provider.revalidate_path(dest_path.parent, dest_path.name, folder=dest_path.is_dir)

        folder.children = []
        items = await self.metadata(src_path)  # type: ignore

        # Metadata returns a union, which confuses mypy
        self.provider_metrics.append('_folder_file_ops.item_counts', len(items))  # type: ignore

        for i in range(0, len(items), wb_settings.OP_CONCURRENCY):  # type: ignore
            futures = []
            for item in items[i:i + wb_settings.OP_CONCURRENCY]:  # type: ignore
                futures.append(asyncio.ensure_future(
                    func(
                        dest_provider,
                        # TODO figure out a way to cut down on all the requests made here
                        (await self.revalidate_path(src_path, item.name, folder=item.is_folder)),
                        (await dest_provider.revalidate_path(dest_path, item.name, folder=item.is_folder)),
                        handle_naming=False,
                    )
                ))

                if item.is_folder:
                    await futures[-1]

            if not futures:
                continue

            done, _ = await asyncio.wait(futures, return_when=asyncio.FIRST_EXCEPTION)

            for fut in done:
                folder.children.append(fut.result()[0])

        return folder, created

    async def handle_naming(self,
                            src_path: wb_path.WaterButlerPath,
                            dest_path: wb_path.WaterButlerPath,
                            rename: str=None,
                            conflict: str='replace') -> wb_path.WaterButlerPath:
        """Given a :class:`.WaterButlerPath` and the desired name, handle any potential naming issues.

        i.e.:

        ::

            cp /file.txt /folder/           ->    /folder/file.txt
            cp /folder/ /folder/            ->    /folder/folder/
            cp /file.txt /folder/file.txt   ->    /folder/file.txt
            cp /file.txt /folder/file.txt   ->    /folder/file (1).txt
            cp /file.txt /folder/doc.txt    ->    /folder/doc.txt


        :param src_path: ( :class:`.WaterButlerPath` ) The object that is being copied
        :param dest_path: ( :class:`.WaterButlerPath` ) The path that is being copied to or into
        :param rename: ( :class:`str` ) The desired name of the resulting path, may be incremented
        :param conflict: ( :class:`str` ) The conflict resolution strategy, ``replace`` or ``keep``

        :rtype: :class:`.WaterButlerPath`
        """
        if src_path.is_dir and dest_path.is_file:
            # Cant copy a directory to a file
            raise ValueError('Destination must be a directory if the source is')

        if not dest_path.is_file:
            # Directories always are going to be copied into
            # cp /folder1/ /folder2/ -> /folder1/folder2/
            dest_path = await self.revalidate_path(
                dest_path,
                rename or src_path.name,
                folder=src_path.is_dir
            )

        dest_path, _ = await self.handle_name_conflict(dest_path, conflict=conflict)

        return dest_path

    def can_intra_copy(self,
                       other: 'BaseProvider',
                       path: wb_path.WaterButlerPath=None) -> bool:
        """Indicates if a quick copy can be performed between the current provider and `other`.

        .. note::
            Defaults to False

        :param other: ( :class:`.BaseProvider` ) The provider to check against
        :param  path: ( :class:`.WaterButlerPath` ) The path of the desired resource
        :rtype: :class:`bool`
        """
        return False

    def can_intra_move(self,
                       other: 'BaseProvider',
                       path: wb_path.WaterButlerPath=None) -> bool:
        """Indicates if a quick move can be performed between the current provider and `other`.

        .. note::
            Defaults to False

        :param other: ( :class:`.BaseProvider` ) The provider to check against
        :param path: ( :class:`.WaterButlerPath` ) The path of the desired resource
        :rtype: :class:`bool`
        """
        return False

    async def intra_copy(self,
                         dest_provider: 'BaseProvider',
                         source_path: wb_path.WaterButlerPath,
                         dest_path: wb_path.WaterButlerPath) -> typing.Tuple[wb_metadata.BaseFileMetadata, bool]:
        """If the provider supports copying files and/or folders within itself by some means other
        than download/upload, then ``can_intra_copy`` should return ``True``.  This method will
        implement the copy.  It accepts the destination provider, a source path, and the
        destination path.  Returns the metadata for the newly created file and a boolean indicating
        whether the copied entity is completely new (``True``) or overwrote a previously-existing
        file (``False``).

        :param  dest_provider: ( :class:`.BaseProvider` )  a provider instance for the destination
        :param  src_path: ( :class:`.WaterButlerPath` )  the Path of the entity being copied
        :param  dest_path: ( :class:`.WaterButlerPath` ) the Path of the destination being copied to
        :rtype: (:class:`.BaseFileMetadata`, :class:`bool`)
        """
        raise NotImplementedError

    async def intra_move(self,
                         dest_provider: 'BaseProvider',
                         src_path: wb_path.WaterButlerPath,
                         dest_path: wb_path.WaterButlerPath) -> typing.Tuple[wb_metadata.BaseFileMetadata, bool]:
        """If the provider supports moving files and/or folders within itself by some means other
        than download/upload/delete, then ``can_intra_move`` should return ``True``.  This method
        will implement the move.  It accepts the destination provider, a source path, and the
        destination path.  Returns the metadata for the newly created file and a boolean indicating
        whether the moved entity is completely new (``True``) or overwrote a previously-existing
        file (``False``).

        :param  dest_provider: ( :class:`.BaseProvider` ) a provider instance for the destination
        :param  src_path: ( :class:`.WaterButlerPath` ) the Path of the entity being moved
        :param  dest_path: ( :class:`.WaterButlerPath` ) the Path of the destination being moved to
        :rtype: (:class:`.BaseFileMetadata`, :class:`bool`)
        """
        data, created = await self.intra_copy(dest_provider, src_path, dest_path)
        await self.delete(src_path)
        return data, created

    async def exists(self, path: wb_path.WaterButlerPath, **kwargs) \
            -> typing.Union[bool, wb_metadata.BaseMetadata, typing.List[wb_metadata.BaseMetadata]]:
        """Check for existence of WaterButlerPath

        Attempt to retrieve provider metadata to determine existence of a WaterButlerPath.  If
        successful, will return the result of `self.metadata()` which may be `[]` for empty
        folders.

        :param  path: ( :class:`.WaterButlerPath` ) path to check for
        :rtype: (`self.metadata()` or False)
        """
        try:
            return await self.metadata(path, **kwargs)
        except exceptions.NotFoundError:
            return False
        except exceptions.MetadataError as e:
            if e.code != 404:
                raise
        return False

    async def handle_name_conflict(self,
                                   path: wb_path.WaterButlerPath,
                                   conflict: str='replace',
                                   **kwargs) -> typing.Tuple[wb_path.WaterButlerPath, bool]:
        """Check WaterButlerPath and resolve conflicts

        Given a WaterButlerPath and a conflict resolution pattern determine
        the correct file path to upload to and indicate if that file exists or not

        :param  path: ( :class:`.WaterButlerPath` ) Desired path to check for conflict
        :param conflict: ( :class:`str` ) replace, keep, warn
        :rtype: (:class:`.WaterButlerPath` or False)
        :raises: :class:`.NamingConflict`
        """
        exists = await self.exists(path, **kwargs)
        if (not exists and not exists == []) or conflict == 'replace':
            return path, exists  # type: ignore
        if conflict == 'warn':
            raise exceptions.NamingConflict(path.name)

        while True:
            path.increment_name()
            test_path = await self.revalidate_path(
                path.parent,
                path.name,
                folder=path.is_dir
            )

            exists = await self.exists(test_path, **kwargs)
            if not (exists or exists == []):
                break

        return path, False

    async def revalidate_path(self,
                              base: wb_path.WaterButlerPath,
                              path: str,
                              folder: bool=False) -> wb_path.WaterButlerPath:
        """Take a path and a base path and build a WaterButlerPath representing `/base/path`.  For
        id-based providers, this will need to lookup the id of the new child object.

        :param  base: ( :class:`.WaterButlerPath` ) The base folder to look under
        :param path: ( :class:`str`) the path of a child of `base`, relative to `base`
        :param folder: ( :class:`bool` ) whether the returned WaterButlerPath should be a folder
        :rtype: :class:`.WaterButlerPath`
        """
        return base.child(path, folder=folder)

    async def zip(self, path: wb_path.WaterButlerPath, **kwargs) -> asyncio.StreamReader:
        """Streams a Zip archive of the given folder

        :param  path: ( :class:`.WaterButlerPath` ) The folder to compress
        """

        meta_data = await self.metadata(path)  # type: ignore
        if path.is_file:
            meta_data = [meta_data]  # type: ignore
            path = path.parent

        return streams.ZipStreamReader(ZipStreamGenerator(self, path, *meta_data))  # type: ignore

    def shares_storage_root(self, other: 'BaseProvider') -> bool:
        """Returns True if ``self`` and ``other`` both point to the same storage root.  Used to
        detect when a file move/copy action might result in the file overwriting itself. Most
        providers have enough uniquely identifing information in the settings to detect this,
        but some providers may need to override this to do further detection.

        :param  other: ( :class:`.BaseProvider`) another provider instance to compare with
        :rtype: :class:`bool`  (True if both providers use the same storage root)
        """
        return self.NAME == other.NAME and self.settings == other.settings

    @abc.abstractmethod
    def can_duplicate_names(self) -> bool:
        """Returns True if a file and a folder in the same directory can have identical names."""
        raise NotImplementedError

    @abc.abstractmethod
    async def download(self, src_path: wb_path.WaterButlerPath, **kwargs) \
              -> streams.ResponseStreamReader:
        r"""Download a file from this provider.

        :param src_path: ( :class:`.WaterButlerPath` ) Path to the file to be downloaded
        :param \*\*kwargs: ( :class:`dict` ) Arguments to be parsed by child classes
        :rtype: :class:`.ResponseStreamReader`
        :raises: :class:`.DownloadError`
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def upload(self, stream: streams.BaseStream, path: wb_path.WaterButlerPath, *args,
                     **kwargs) -> typing.Tuple[wb_metadata.BaseFileMetadata, bool]:
        r"""Uploads the given stream to the provider.  Returns the metadata for the newly created
        file and a boolean indicating whether the file is completely new (``True``) or overwrote
        a previously-existing file (``False``)

        :param path: ( :class:`.WaterButlerPath` ) Where to upload the file to
        :param  stream: ( :class:`.BaseStream` ) The content to be uploaded
        :param \*\*kwargs: ( :class:`dict` ) Arguments to be parsed by child classes
        :rtype: (:class:`.BaseFileMetadata`, :class:`bool`)
        :raises: :class:`.DeleteError`
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def delete(self, src_path: wb_path.WaterButlerPath, **kwargs) -> None:
        r"""
        :param src_path: ( :class:`.WaterButlerPath` ) Path to be deleted
        :param \*\*kwargs: ( :class:`dict` ) Arguments to be parsed by child classes
        :rtype: :class:`None`
        :raises: :class:`.DeleteError`
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def metadata(self, path: wb_path.WaterButlerPath, **kwargs) \
            -> typing.Union[wb_metadata.BaseMetadata, typing.List[wb_metadata.BaseMetadata]]:
        r"""Get metadata about the specified resource from this provider. Will be a :class:`list`
        if the resource is a directory otherwise an instance of
        :class:`.BaseFileMetadata`

        .. note::
            Mypy doesn't seem to do very well with functions that can return more than one type of
            thing. See: https://github.com/python/mypy/issues/1693

        :param path: ( :class:`.WaterButlerPath` ) The path to a file or folder
        :param \*\*kwargs: ( :class:`dict` ) Arguments to be parsed by child classes
        :rtype: :class:`.BaseMetadata`
        :rtype: :class:`list` of :class:`.BaseMetadata`
        :raises: :class:`.MetadataError`
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def validate_v1_path(self, path: str, **kwargs) -> wb_path.WaterButlerPath:
        """API v1 requires that requests against folder endpoints always end with a slash, and
        requests against files never end with a slash.  This method checks the provider's metadata
        for the given id and throws a 404 Not Found if the implicit and explicit types don't
        match.  This method duplicates the logic in the provider's validate_path method, but
        validate_path must currently accomodate v0 AND v1 semantics.  After v0's retirement, this
        method can replace validate_path.

        ``path`` is the string in the url after the provider name and refers to the entity to be
        acted on. For v1, this must *always exist*.  If it does not, ``validate_v1_path`` should
        return a 404.  Creating a new file in v1 is done by making a PUT request against the parent
        folder and specifying the file name as a query parameter.  If a user attempts to create a
        file by PUTting to its inferred path, validate_v1_path should reject this request with a
        404.

        :param path: ( :class:`str` ) user-supplied path to validate
        :rtype: :class:`.WaterButlerPath`
        :raises: :class:`.NotFoundError`
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def validate_path(self, path: str, **kwargs) -> wb_path.WaterButlerPath:
        """Validates paths passed in via the v0 API.  v0 paths are much less strict than v1 paths.
        They may represent things that exist or something that should be created.  As such, the goal
        of ``validate_path`` is to split the path into its component parts and attempt to determine
        the ID of each part on the external provider.  For instance, if the ``googledrive`` provider
        receives a path of ``/foo/bar/baz.txt``, it will split those into ``/``, ``foo/``, ``bar/``,
        and ``baz.txt``, and query Google Drive for the ID of each.  ``validate_path`` then builds a
        WaterButlerPath object with an ID, name tuple for each path part.  The last part is
        permitted to not have an ID, since it may represent a file that has not yet been created.
        All other parts should have an ID.

        The WaterButler v0 API is deprecated and will be removed in a future release.  At that time
        this method will be obsolete and will be removed from all providers.

        :param path: ( :class:`str` ) user-supplied path to validate
        :rtype: :class:`.WaterButlerPath`
        :raises: :class:`.NotFoundError`
        """
        raise NotImplementedError

    def path_from_metadata(self,
                           parent_path: wb_path.WaterButlerPath,
                           meta_data: wb_metadata.BaseMetadata) -> wb_path.WaterButlerPath:
        return parent_path.child(meta_data.name, _id=meta_data.path.strip('/'),
                                 folder=meta_data.is_folder)

    async def revisions(self, path: wb_path.WaterButlerPath, **kwargs):
        """Return a list of :class:`.BaseFileRevisionMetadata` objects representing the revisions
        available for the file at ``path``.
        """
        return []  # TODO Raise 405 by default h/t @rliebz

    async def create_folder(self, path: wb_path.WaterButlerPath,
                            **kwargs) -> wb_metadata.BaseFolderMetadata:
        """Create a folder in the current provider at `path`. Returns a `BaseFolderMetadata` object
        if successful.  May throw a 409 Conflict if a directory with the same name already exists.

        :param path: ( :class:`.WaterButlerPath` ) User-supplied path to create. Must be a directory.
        :rtype: :class:`.BaseFileMetadata`
        :raises: :class:`.CreateFolderError`
        """
        raise exceptions.ProviderError({'message': 'Folder creation not supported.'}, code=405)

    def _build_range_header(self, slice_tup: typing.Tuple[int, int]) -> str:
        start, end = slice_tup
        return 'bytes={}-{}'.format(
            '' if start is None else start,
            '' if end is None else end
        )

    def __repr__(self):
        # Note: credentials are not included on purpose.
        return '<{}({}, {})>'.format(self.__class__.__name__, self.auth, self.settings)
