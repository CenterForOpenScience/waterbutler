import uuid
import socket
import asyncio
import logging
from http import HTTPStatus
import os

import tornado.gen

import sentry_sdk

from waterbutler.core import utils
from waterbutler.server import settings
from waterbutler.server.api.v1 import core
from waterbutler.core import remote_logging
from waterbutler.server.auth import AuthHandler
from waterbutler.core.log_payload import LogPayload
from waterbutler.core.exceptions import TooManyRequests
from waterbutler.core.streams import RequestStreamReader
from waterbutler.server.settings import ENABLE_RATE_LIMITING
from waterbutler.server.api.v1.provider.create import CreateMixin
from waterbutler.server.api.v1.provider.metadata import MetadataMixin
from waterbutler.server.api.v1.provider.movecopy import MoveCopyMixin
from waterbutler.server.api.v1.provider.ratelimiting import RateLimitingMixin

logger = logging.getLogger(__name__)
auth_handler = AuthHandler(settings.AUTH_HANDLERS)


def list_or_value(value):
    assert isinstance(value, list)
    if len(value) == 0:
        return None
    if len(value) == 1:
        return value[0].decode('utf-8')
    return [item.decode('utf-8') for item in value]


# TODO: the order should be reverted though it doesn't have any functional effect for this class.
@tornado.web.stream_request_body
class ProviderHandler(core.BaseHandler, CreateMixin, MetadataMixin, MoveCopyMixin, RateLimitingMixin):
    PRE_VALIDATORS = {'put': 'prevalidate_put', 'post': 'prevalidate_post'}
    POST_VALIDATORS = {'put': 'postvalidate_put'}
    PATTERN = r'/resources/(?P<resource>(?:\w|\d)+)/providers/(?P<provider>(?:\w|\d)+)(?P<path>/.*/?)'

    async def prepare(self, *args, **kwargs):
        if ENABLE_RATE_LIMITING:
            logger.debug('>>> checking for rate-limiting')
            limit_hit, data = self.rate_limit()
            if limit_hit:
                raise TooManyRequests(data=data)
            logger.debug('>>> rate limiting check passed ...')

        method = self.request.method.lower()

        # TODO Find a nicer way to handle this
        if method == 'options':
            return

        self.arguments = {
            key: list_or_value(value)
            for key, value in self.request.query_arguments.items()
        }

        # Going with version as its the most correct term
        # TODO Change all references of revision to version @chrisseto
        # revisions will still be accepted until necessary changes are made to OSF
        self.requested_version = (
            self.get_query_argument('version', default=None) or
            self.get_query_argument('revision', default=None)
        )

        self.path = self.path_kwargs['path'] or '/'
        provider = self.path_kwargs['provider']
        self.resource = self.path_kwargs['resource']

        scope = sentry_sdk.get_current_scope()
        scope.set_tag('resource.id', self.resource)
        scope.set_tag('src_provider', self.path_kwargs['provider'])

        # pre-validator methods perform validations that can be performed before ensuring that the
        # path given by the url is valid.  An example would be making sure that a particular query
        # parameter matches and allowed value.  We do this because validating the path requires
        # issuing one or more API calls to the provider, and some providers are quite stingy with
        # their rate limits.
        if method in self.PRE_VALIDATORS:
            getattr(self, self.PRE_VALIDATORS[method])()

        # Delay setup of the provider when method is post, as we need to evaluate the json body
        # action.
        if method != 'post':
            self.auth = await auth_handler.get(self.resource, provider, self.request,
                                               path=self.path, version=self.requested_version)
            self.provider = utils.make_provider(provider, self.auth['auth'],
                                                self.auth['credentials'], self.auth['settings'])
            self.path = await self.provider.validate_v1_path(self.path, **self.arguments)

        self.target_path = None

        # post-validator methods perform validations that expect that the path given in the url has
        # been verified for existence and type.
        if method in self.POST_VALIDATORS:
            await getattr(self, self.POST_VALIDATORS[method])()

        # The one special case
        if method == 'put' and self.target_path.is_file:
            await self.prepare_stream()
        else:
            self.stream = None
        self.body = b''

        self.add_header('X-WATERBUTLER-REQUEST-ID', str(uuid.uuid4()))

    async def head(self, **_):
        """Get metadata for a folder or file
        """
        if self.path.is_dir:  # Metadata on the folder itself TODO
            return self.set_status(int(HTTPStatus.NOT_IMPLEMENTED))
        return await self.header_file_metadata()

    async def get(self, **_):
        """Download a file
        Will redirect to a signed URL if possible and accept_url is not False
        :raises: MustBeFileError if path is not a file
        """
        if self.path.is_dir:
            return await self.get_folder()
        return await self.get_file()

    async def put(self, **_):
        """Defined in CreateMixin"""
        if self.target_path.is_file:
            return await self.upload_file()
        return await self.create_folder()

    async def post(self, **_):
        return await self.move_or_copy()

    async def delete(self, **_):
        self.confirm_delete = int(self.get_query_argument('confirm_delete', default=0))
        await self.provider.delete(self.path, confirm_delete=self.confirm_delete)
        self.set_status(int(HTTPStatus.NO_CONTENT))

    async def data_received(self, chunk):
        """Note: Only called during uploads."""
        self.bytes_uploaded += len(chunk)
        if self.stream:
            try:
                self.writer.write(chunk)
                await self.writer.drain()
            except Exception as e:
                logger.error(f"Upload stream reset: possible early EOF or cancellation. {e}")
                # Cancel the uploader task if it exists and is not done
                if hasattr(self, 'uploader') and not self.uploader.done():
                    self.uploader.cancel()
                    # Wait a bit for the cancellation to take effect
                    await asyncio.sleep(0.1)

                # Clean up the feed_reader task
                if hasattr(self, 'feed_task') and not self.feed_task.done():
                    # Signal the feed_reader task to stop
                    if hasattr(self, 'queue'):
                        try:
                            await self.queue.put(None)
                            # Give the feed_reader task a chance to process the None
                            await asyncio.sleep(0.1)
                        except Exception as queue_error:
                            logger.error(f"Error putting None in queue during error handling: {queue_error}")

                    # If the feed_reader task is still not done, cancel it
                    if not self.feed_task.done():
                        try:
                            self.feed_task.cancel()
                        except Exception as cancel_error:
                            logger.error(f"Error cancelling feed_task during error handling: {cancel_error}")
        else:
            self.body += chunk

    async def prepare_stream(self):
        """Sets up an asyncio pipe from client to server
        Only called on PUT when path is to a file

        This implementation uses asyncio.Queue instead of socket pairs to be compatible with uvloop.
        """
        # Create a custom stream reader that will read from our queue
        self.reader = asyncio.StreamReader()
        self.queue = asyncio.Queue()

        # Create a task that reads from the queue and feeds the reader
        async def feed_reader():
            try:
                while True:
                    chunk = await self.queue.get()
                    if chunk is None:  # None is our EOF marker
                        self.queue.task_done()  # Mark the None chunk as done
                        break
                    self.reader.feed_data(chunk)
                    self.queue.task_done()
            finally:
                self.reader.feed_eof()

        self.feed_task = asyncio.create_task(feed_reader())

        # Create a writer that puts data into the queue
        class QueueWriter:
            def __init__(self, queue):
                self.queue = queue
                self._closed = False
                self._pending_writes = []

            def write(self, data):
                # Create a task and track it so we can wait for it in drain
                task = asyncio.create_task(self.queue.put(data))
                self._pending_writes.append(task)

            async def write_eof(self):
                # Signal EOF by putting None in the queue
                # Put None directly and wait for it to be processed
                await self.queue.put(None)
                # No need to track this as a pending write since we've already awaited it

            def close(self):
                self._closed = True

            async def wait_closed(self):
                # Wait for all pending writes to complete
                if self._pending_writes:
                    await asyncio.gather(*self._pending_writes)
                # Wait for the queue to be empty
                await self.queue.join()

            async def drain(self):
                # Wait for all pending writes to complete
                if self._pending_writes:
                    await asyncio.gather(*self._pending_writes)
                    self._pending_writes = []

        self.writer = QueueWriter(self.queue)

        self.stream = RequestStreamReader(self.request, self.reader)
        self.uploader = asyncio.ensure_future(self.provider.upload(self.stream, self.target_path))

    async def _cleanup_feed_task(self):
        """Clean up the feed_reader task and any pending writes."""
        try:
            # Only signal the feed_reader task to stop if it's not already done
            if hasattr(self, 'feed_task') and not self.feed_task.done():
                # Signal the feed_reader task to stop by putting None in the queue
                await self.queue.put(None)

                # Wait for the feed_reader task to complete
                try:
                    # Use a shorter timeout to avoid blocking for too long
                    await asyncio.wait_for(asyncio.shield(self.feed_task), 0.5)
                except asyncio.TimeoutError:
                    # If it doesn't complete in time, cancel it
                    self.feed_task.cancel()
                    # Wait a bit for the cancellation to take effect
                    await asyncio.sleep(0.1)

            # Wait for any pending writes to complete
            if hasattr(self, 'writer') and hasattr(self.writer, '_pending_writes') and self.writer._pending_writes:
                # Wait for all pending writes with a timeout
                try:
                    await asyncio.wait_for(asyncio.gather(*self.writer._pending_writes, return_exceptions=True), 0.5)
                except asyncio.TimeoutError:
                    # If timeout occurs, log it but continue
                    logger.warning("Timeout waiting for pending writes to complete during cleanup")
        except Exception as e:
            logger.error(f"Error cleaning up feed task: {e}")

    def on_finish(self):
        # Clean up the feed_reader task if it exists
        if hasattr(self, 'queue') and hasattr(self, 'feed_task'):
            # Signal the feed_reader task to stop by putting None in the queue
            # Use create_task to avoid blocking in on_finish
            # Store a reference to the task to prevent it from being garbage collected
            self._cleanup_task = asyncio.create_task(self._cleanup_feed_task())
            # Add a done callback to handle any exceptions
            self._cleanup_task.add_done_callback(
                lambda task: logger.error(f"Cleanup task error: {task.exception()}") if task.exception() else None
            )

        status, method = self.get_status(), self.request.method.upper()

        # If the response code is not within the 200-302 range, the request was a HEAD or OPTIONS,
        # the response code is 202, or the response was a 206 partial request, then no callbacks
        # should be sent and no metrics collected.  For 202s, celery will send its own callback.
        # Osfstorage and s3 can return 302s for file downloads, which should be tallied.
        if any({
            method in {'HEAD', 'OPTIONS'},
            status in {202, 206},
            status > 302,
            status < 200
        }):
            return

        # WB doesn't send along Range headers when requesting signed urls, expecting the client
        # to forward them along with the redirect request (as curl and Postman do).  Before
        # rejecting 302s with partials, start logging instances of them to see how common they
        # are.  That information will be used to decide if direct+Range requests should be logged
        # or not.  For now, continue to log.
        if status == 302 and 'Range' in self.request.headers:
            logger.info('Received a direct-to-provider download request (302 response) with '
                        'Range header: {}'.format(self.request.headers['Range']))

        # Don't log metadata requests, incl. anything with 'meta' or 'revisions' as a query arg.
        # Folder requests w/o 'zip' as a query arg are treated as metadata requests, and should
        # be ignored.
        if (method == 'GET' and ('meta' in self.request.query_arguments or
                                 'revisions' in self.request.query_arguments or
                                 (self.path.is_folder and
                                  'zip' not in self.request.query_arguments))):
            return

        # Done here just because method is defined
        action = {
            'GET': lambda: 'download_file' if self.path.is_file else 'download_zip',
            'PUT': lambda: ('create' if self.target_path.is_file else 'create_folder') if status == 201 else 'update',
            'POST': lambda: 'move' if self.json['action'] == 'rename' else self.json['action'],
            'DELETE': lambda: 'delete'
        }[method]()

        self._send_hook(action)

    def _send_hook(self, action):
        source = None
        destination = None

        if action in ('move', 'copy'):
            # if provider can't intra_move or copy, then the celery task will take care of logging
            if not getattr(self.provider, 'can_intra_' + action)(self.dest_provider, self.path):
                return

            source = LogPayload(self.resource, self.provider, path=self.path)
            destination = LogPayload(
                self.dest_resource,
                self.dest_provider,
                metadata=self.dest_meta,
            )
        elif action in ('create', 'create_folder', 'update'):
            source = LogPayload(self.resource, self.provider, metadata=self.metadata)
        elif action in ('delete', 'download_file', 'download_zip'):
            source = LogPayload(self.resource, self.provider, path=self.path)
        else:
            return

        remote_logging.log_file_action(action, source=source, destination=destination, api_version='v1',
                                       request=remote_logging._serialize_request(self.request),
                                       bytes_downloaded=self.bytes_downloaded,
                                       bytes_uploaded=self.bytes_uploaded,)
