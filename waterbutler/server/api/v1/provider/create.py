import os
import asyncio

from tornado.web import HTTPError


class CreateMixin:

    def validate_put(self):
        self.kind = self.get_query_argument('kind', default='file')

        if self.kind not in ('file', 'folder'):
            raise HTTPError(400)

        if self.path.endswith('/'):
            name = self.get_query_argument('name')  # TODO What does this do?
            self.path = os.path.join(self.path, name)
            if self.kind == 'folder':
                self.path += '/'
        elif self.kind == 'folder':
            raise HTTPError(400)

        length = self.request.headers.get('Content-Length')

        if length is None and self.kind == 'file':
            raise HTTPError(400)

        try:
            if int(length) > 0 and self.kind == 'folder':
                raise HTTPError(400)
        except ValueError:
                raise HTTPError(400)

    @asyncio.coroutine
    def create_folder(self):
        metadata = yield from self.provider.create_folder(self.path)
        self.set_status(201)
        self.write(metadata.serialized())

    @asyncio.coroutine
    def upload_file(self):
        self.writer.write_eof()

        metadata, created = yield from self.uploader
        self.writer.close()
        self.wsock.close()
        if created:
            self.set_status(201)

        self.write(metadata.serialized())
