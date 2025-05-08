import asyncio
import tornado.iostream

from waterbutler.auth.osf.handler import logger
from waterbutler.server import settings

CORS_ACCEPT_HEADERS = [
    'Range',
    'Content-Type',
    'Authorization',
    'Cache-Control',
    'X-Requested-With',
    'X-CSRFToken',
]

CORS_EXPOSE_HEADERS = [
    'Range',
    'Accept-Ranges',
    'Content-Range',
    'Content-Length',
    'Content-Encoding',
]

HTTP_REASONS = {
    422: 'Unprocessable Entity',
    461: 'Unavailable For Legal Reasons',
}


def parse_request_range(range_header):
    r"""WB uses tornado's ``httputil._parse_request_range`` function to parse the Range HTTP header
    and return a tuple representing the range.  Tornado's version returns a tuple suitable for
    slicing arrays, meaning that a range of 0-1 will be returned as ``(0, 2)``.  WB had been
    assuming that the tuple would represent the first and last byte positions and was consistently
    returning one more byte than requested. Since WB doesn't ever use ranges to do list slicing of
    byte streams, this function wraps tornado's version and returns the actual byte indices.

    Ex. ``Range: bytes=0-1`` will be returned as ``(0, 1)``.

    If the end byte is omitted, the second element of the tuple will be ``None``. This will be sent
    to the provider as an open ended range, e.g. (``Range: bytes=5-``).  Most providers interpret
    this to mean "send from the start byte to the end of the file".

    If this function receives an unsupported or unfamiliar Range header, it will return ``None``,
    indicating that the full file should be sent.  Some formats supported by other providers but
    unsupported by WB include:

    * ``Range: bytes=-5`` -- some providers interpret this as "send the last five bytes"

    * ``Range: bytes=0-5,10-12`` -- indicates a multi-range, "send the first six bytes, then the
      next three bytes starting from the eleventh".

    Unfamiliar byte ranges are anything not matching ``^bytes=[0-9]+\-[0-9]*$``, or ranges where
    the end byte position is less than the start byte.

    :param str range_header: a string containing the value of the Range header
    :rtype: `tuple` or `None`
    :return: a `tuple` representing the inclusive range of byte positions or `None`.
    """
    request_range = tornado.httputil._parse_request_range(range_header)

    if request_range is None:
        return request_range

    start, end = request_range
    if start is None or start < 0:
        return None

    if end is not None:
        end -= 1
        if end < start:
            return None

    return start, end


class CORsMixin:

    def _cross_origin_is_allowed(self):
        if self.request.method == 'OPTIONS':
            return True
        elif not self.request.cookies and self.request.headers.get('Authorization'):
            return True
        return False

    def set_default_headers(self):
        if not self.request.headers.get('Origin'):
            return

        allowed_origin = None
        if self._cross_origin_is_allowed():
            allowed_origin = self.request.headers['Origin']
        elif isinstance(settings.CORS_ALLOW_ORIGIN, str):
            if settings.CORS_ALLOW_ORIGIN == '*':
                # Wild cards cannot be used with allowCredentials.
                # Match Origin if its specified, makes pdfs and pdbs render properly
                allowed_origin = self.request.headers['Origin']
            else:
                allowed_origin = settings.CORS_ALLOW_ORIGIN
        else:
            if self.request.headers['Origin'] in settings.CORS_ALLOW_ORIGIN:
                allowed_origin = self.request.headers['Origin']

        if allowed_origin is not None:
            self.set_header('Access-Control-Allow-Origin', allowed_origin)

        self.set_header('Access-Control-Allow-Credentials', 'true')
        self.set_header('Access-Control-Allow-Headers', ', '.join(CORS_ACCEPT_HEADERS))
        self.set_header('Access-Control-Expose-Headers', ', '.join(CORS_EXPOSE_HEADERS))
        self.set_header('Cache-control', 'no-store, no-cache, must-revalidate, max-age=0')

    def options(self, *args, **kwargs):
        self.set_status(204)
        if self.request.headers.get('Origin'):
            self.set_header('Access-Control-Allow-Methods', 'GET, PUT, POST, DELETE'),


class UtilMixin:

    bytes_downloaded = 0
    bytes_uploaded = 0

    def set_status(self, code, reason=None):
        return super().set_status(code, reason or HTTP_REASONS.get(code))

    async def write_stream(self, stream):
        logger.error(f"scope start")
        try:
            from waterbutler.core.utils import ZipStreamGenerator
            # import pydevd_pycharm
            # pydevd_pycharm.settrace('host.docker.internal', port=1236, stdoutToServer=True, stderrToServer=True)
            is_s3_provider = self.path_kwargs['provider'] == 's3'
            if not is_s3_provider or (is_s3_provider and hasattr(stream, 'streams') and isinstance(stream.streams, ZipStreamGenerator)):
                try:
                    while True:
                        chunk = await stream.read(settings.CHUNK_SIZE)
                        if not chunk:
                            break
                        # Temp fix, write does not accept bytearrays currently
                        if isinstance(chunk, bytearray):
                            chunk = bytes(chunk)
                        self.write(chunk)
                        self.bytes_downloaded += len(chunk)
                        del chunk
                        await self.flush()
                except tornado.iostream.StreamClosedError:
                    # Client has disconnected early.
                    # No need for any exception to be raised
                    return
            else:

                import datetime
                logger.error("write_stream")
                logger.error(f"Response closed: {stream.response.closed}")

                async def async_chunk_gen():
                    iterator = stream.response.iter_chunks(settings.CHUNK_SIZE)
                    loop = asyncio.get_event_loop()

                    while True:
                        try:
                            chunk = await loop.run_in_executor(None, next, iterator)
                        except StopIteration:
                            break

                        if chunk:
                            if isinstance(chunk, bytearray):
                                chunk = bytes(chunk)
                            yield chunk

                async for chunk in async_chunk_gen():
                    logger.error(f"{len(chunk)} {self.bytes_downloaded} {datetime.datetime.now()}")
                    self.write(chunk)
                    self.bytes_downloaded += len(chunk)
                    del chunk
                    await self.flush()

        except (tornado.iostream.StreamClosedError, Exception) as exc:
            logger.error(f"exc {exc}")
        logger.error(f"scope end")
