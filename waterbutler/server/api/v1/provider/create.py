import asyncio

from tornado.web import HTTPError


class CreateMixin:

    def validate_put(self):
        self.name = self.get_query_argument('name')  # TODO What does this do?
        self.kind = self.get_query_argument('kind', default='file')

        if self.kind not in ('file', 'folder'):
            raise Exception()

        if 'Content-Length' not in self.request.headers:
            raise HTTPError(404, 'foo', body='bar')

        if self.kind == 'folder' and self.request.headers.get('Content-Length') != 0:
            raise Exception()

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
