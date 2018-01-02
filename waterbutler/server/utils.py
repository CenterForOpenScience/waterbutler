import tornado.iostream
from waterbutler.server import settings


CORS_ACCEPT_HEADERS = [
    'Range',
    'Content-Type',
    'Authorization',
    'Cache-Control',
    'X-Requested-With',
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


def make_disposition(filename):
    return 'attachment;filename="{}"'.format(filename.replace('"', '\\"'))


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

    async def write_stream(self, stream, request_range=(0, None)):

        # If the browser does not supply a request range our start is 0 and our end is a NoneType.
        # This is just to be consitent with other Tornado code.
        start, end = request_range

        while True:
            if end and end - start - self.bytes_downloaded < settings.CHUNK_SIZE:
                chunk = await stream.read(end - start - self.bytes_downloaded)
            else:
                chunk = await stream.read(settings.CHUNK_SIZE)

            if not chunk:
                break

            if isinstance(chunk, bytearray):
                chunk = bytes(chunk)

            self.write(chunk)
            self.bytes_downloaded += len(chunk)
            del chunk
            try:
                await self.flush()
            except tornado.iostream.StreamClosedError:
                # No need to panic the client just closed the stream
                return
