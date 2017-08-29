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
                 retry_on: typing.Set[int]={408, 502, 503, 504}) -> None:
        """
        :param dict auth: Information about the user this provider will act on the behalf of
        :param dict credentials: The credentials used to authenticate with the provider,
            ofter an OAuth 2 token
        :param dict settings: Configuration settings for this provider,
            often folder or repo
        """
        self._retry_on = retry_on
        self.auth = auth
        self.credentials = credentials
        self.settings = settings

        self.provider_metrics = MetricsRecord('provider')
        self.provider_metrics.add('auth', auth)
        self.metrics = self.provider_metrics.new_subrecord(self.NAME)

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
        """A nice wrapper around furl, builds urls based on self.BASE_URL

        :param tuple \*segments: A tuple of strings joined into /foo/bar/..
        :param dict \*\*query: A dictionary that will be turned into query parameters ?foo=bar
        :rtype: str
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

    @throttle()
    async def make_request(self, method: str, url: str, *args, **kwargs) -> aiohttp.client.ClientResponse:
        """A wrapper around :func:`aiohttp.request`. Inserts default headers.

        :param str method: The HTTP method
        :param str url: The url to send the request to
        :keyword range: An optional tuple (start, end) that is transformed into a Range header
        :keyword expects: An optional tuple of HTTP status codes as integers raises an exception
            if the returned status code is not in it.
        :type expects: tuple of ints
        :param Exception throws: The exception to be raised from expects
        :param tuple \*args: args passed to :func:`aiohttp.request`
        :param dict \*\*kwargs: kwargs passed to :func:`aiohttp.request`
        :rtype: :class:`aiohttp.Response`
        :raises ProviderError: Raised if expects is defined
        """
        kwargs['headers'] = self.build_headers(**kwargs.get('headers', {}))
        retry = _retry = kwargs.pop('retry', 2)
        range = kwargs.pop('range', None)
        expects = kwargs.pop('expects', None)
        throws = kwargs.pop('throws', exceptions.ProviderError)
        if range:
            kwargs['headers']['Range'] = self._build_range_header(range)

        if callable(url):
            url = url()
        while retry >= 0:
            try:
                self.provider_metrics.incr('requests.count')
                self.provider_metrics.append('requests.urls', url)
                response = await aiohttp.request(method, url, *args, **kwargs)
                self.provider_metrics.append('requests.verbose', ['OK', response.status, url])
                if expects and response.status not in expects:
                    raise (await exceptions.exception_from_response(response, error=throws, **kwargs))
                return response
            except throws as e:
                self.provider_metrics.append('requests.verbose', ['NO', e.code, url])
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

        :param BaseProvider dest_provider: The provider to move to
        :param wb_path.WaterButlerPath src_path: Path to where the resource can be found
        :param wb_path.WaterButlerPath dest_path: Path to where the resource will be moved
        :param str rename: The desired name of the resulting path, may be incremented
        :param str conflict: What to do in the event of a name conflict, ``replace`` or ``keep``
        :param bool handle_naming: If a naming conflict is detected, should it be automatically handled?
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
        """Given a WaterButlerPath and the desired name, handle any potential naming issues.

        i.e.:
            cp /file.txt /folder/ -> /folder/file.txt
            cp /folder/ /folder/ -> /folder/folder/
            cp /file.txt /folder/file.txt -> /folder/file.txt
            cp /file.txt /folder/file.txt -> /folder/file (1).txt
            cp /file.txt /folder/doc.txt -> /folder/doc.txt

        :param WaterButlerPath src_path: The object that is being copied
        :param WaterButlerPath dest_path: The path that is being copied to or into
        :param str rename: The desired name of the resulting path, may be incremented
        :param str conflict: The conflict resolution strategy, ``replace`` or ``keep``

        Returns: WaterButlerPath dest_path: The path of the desired result.
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

        :param waterbutler.core.provider.BaseProvider other: The provider to check against
        :param waterbutler.core.path.WaterButlerPath path: The path of the desired resource
        :rtype: bool
        """
        return False

    def can_intra_move(self,
                       other: 'BaseProvider',
                       path: wb_path.WaterButlerPath=None) -> bool:
        """Indicates if a quick move can be performed between the current provider and `other`.

        .. note::
            Defaults to False

        :param waterbutler.core.provider.BaseProvider other: The provider to check against
        :param waterbutler.core.path.WaterButlerPath path: The path of the desired resource
        :rtype: bool
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

        :param BaseProvider dest_provider: a provider instance for the destination
        :param WaterButlerPath source_path: the Path of the entity being copied
        :param WaterButlerPath dest_path: the Path of the destination being copied to
        :rtype: (:class:`waterbutler.core.metadata.BaseFileMetadata`, :class:`bool`)
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

        :param BaseProvider dest_provider: a provider instance for the destination
        :param WaterButlerPath src_path: the Path of the entity being moved
        :param WaterButlerPath dest_path: the Path of the destination being moved to
        :rtype: (:class:`waterbutler.core.metadata.BaseFileMetadata`, :class:`bool`)
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

        :param WaterButlerPath path: path to check for
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

        :param WaterButlerPath path: Desired path to check for conflict
        :param str conflict: replace, keep, warn
        :rtype: (WaterButlerPath or False)
        :raises: NamingConflict
        """
        exists = await self.exists(path, **kwargs)
        if (not exists and not exists == []) or conflict == 'replace':
            return path, exists  # type: ignore
        if conflict == 'warn':
            raise exceptions.NamingConflict(path)

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

        :param WaterButlerPath base: The base folder to look under
        :param str path: the path of a child of `base`, relative to `base`
        :param bool folder: whether the returned WaterButlerPath should represent a folder
        :rtype: WaterButlerPath
        """
        return base.child(path, folder=folder)

    async def zip(self, path: wb_path.WaterButlerPath, **kwargs) -> asyncio.StreamReader:
        """Streams a Zip archive of the given folder

        :param WaterButlerPath path: The folder to compress
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

        :param BaseProvider other: another provider instance to compare with
        :returns bool: True if both providers use the same storage root.
        """
        return self.NAME == other.NAME and self.settings == other.settings

    @abc.abstractmethod
    def can_duplicate_names(self) -> bool:
        """Returns True if a file and a folder in the same directory can have identical names."""
        raise NotImplementedError

    @abc.abstractmethod
    async def download(self, src_path: wb_path.WaterButlerPath, **kwargs) -> streams.ResponseStreamReader:
        """Download a file from this provider.
        :param WaterButlerPath src_path: Path to the file to be downloaded
        :param dict \*\*kwargs: Arguments to be parsed by child classes
        :rtype: :class:`waterbutler.core.streams.ResponseStreamReader`
        :raises: :class:`waterbutler.core.exceptions.DownloadError`
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def upload(self, stream: streams.BaseStream, path: wb_path.WaterButlerPath, *args, **kwargs) \
            -> typing.Tuple[wb_metadata.BaseFileMetadata, bool]:
        """Uploads the given stream to the provider.  Returns the metadata for the newly created
        file and a boolean indicating whether the file is completely new (``True``) or overwrote
        a previously-existing file (``False``)

        :param :class:`WaterButlerPath` path: Where to upload the file to
        :param :class:`waterbutler.core.streams.BaseStream` stream: The content to be uploaded
        :param dict \*\*kwargs: Arguments to be parsed by child classes
        :rtype: (:class:`waterbutler.core.metadata.BaseFileMetadata`, :class:`bool`)
        :raises: :class:`waterbutler.core.exceptions.DeleteError`
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def delete(self, src_path: wb_path.WaterButlerPath, **kwargs) -> None:
        """
        :param WaterButlerPath src_path: Path to be deleted
        :param dict \*\*kwargs: Arguments to be parsed by child classes
        :rtype: :class:`None`
        :raises: :class:`waterbutler.core.exceptions.DeleteError`
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def metadata(self, path: wb_path.WaterButlerPath, **kwargs) \
            -> typing.Union[wb_metadata.BaseMetadata, typing.List[wb_metadata.BaseMetadata]]:
        """Get metadata about the specified resource from this provider. Will be a :class:`list`
        if the resource is a directory otherwise an instance of
        :class:`waterbutler.core.metadata.BaseFileMetadata`

        Developer note: Mypy doesn't seem to do very well with functions that can return more than one type of thing.
        See: https://github.com/python/mypy/issues/1693

        :param WaterButlerPath path: The path to a file or folder
        :param dict \*\*kwargs: Arguments to be parsed by child classes
        :rtype: :class:`waterbutler.core.metadata.BaseMetadata`
        :rtype: :class:`list` of :class:`waterbutler.core.metadata.BaseMetadata`
        :raises: :class:`waterbutler.core.exceptions.MetadataError`
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
        file by PUTting to its inferred path, validate_v1_path should reject this request with a 404.

        :param str path: user-supplied path to validate
        :rtype: :class:`waterbutler.core.path.WaterButlerPath`
        :raises: :class:`waterbutler.core.exceptions.NotFoundError`
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

        :param str path: user-supplied path to validate
        :rtype: :class:`waterbutler.core.path.WaterButlerPath`
        :raises: :class:`waterbutler.core.exceptions.NotFoundError`
        """
        raise NotImplementedError

    def path_from_metadata(self,
                           parent_path: wb_path.WaterButlerPath,
                           meta_data: wb_metadata.BaseMetadata) -> wb_path.WaterButlerPath:
        return parent_path.child(meta_data.name, _id=meta_data.path.strip('/'),
                                 folder=meta_data.is_folder)

    async def revisions(self, path: wb_path.WaterButlerPath, **kwargs):
        """Return a list of `metadata.BaseFileRevisionMetadata` objects representing the revisions
        available for the file at ``path``.
        """
        return []  # TODO Raise 405 by default h/t @rliebz

    async def create_folder(self, path: wb_path.WaterButlerPath,
                            **kwargs) -> wb_metadata.BaseFolderMetadata:
        """Create a folder in the current provider at `path`. Returns a `BaseFolderMetadata` object
        if successful.  May throw a 409 Conflict if a directory with the same name already exists.

        :param WaterButlerPath path: User-supplied path to create. Must be a directory.
        :rtype: :class:`waterbutler.core.metadata.BaseFolderMetadata`
        :raises: :class:`waterbutler.core.exceptions.FolderCreationError`
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
