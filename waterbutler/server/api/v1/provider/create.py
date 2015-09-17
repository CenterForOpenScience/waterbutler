import os
import asyncio

from waterbutler.core import exceptions


class CreateMixin:

    def validate_put(self):
        """Prevalidation for creation requests. Run BEFORE
        the body of a request is accepted. Requests with bodies that are too large can be
        rejected if we have not began to accept the body.
        Validation is as follows:
        1. Pull kind from query params. It must be file, folder, or not included (which defaults to file)
        2. If path is a folder (ends with a slash) pull name from query parameters raise an exception if its not found
            * If kind is folder a / is append to path
        3. If path does not end with a / and kind is folder raise an exception
        4. Ensure that content length is present for file uploads
        5. Ensure that content length is either not present or 0 for folder creation requests
        """
        self.kind = self.get_query_argument('kind', default='file')

        if self.kind not in ('file', 'folder'):
            raise exceptions.InvalidParameters('Kind must be file, folder or unspecified (interpreted as file), not {}'.format(self.kind))

        if self.path.endswith('/'):
            name = self.get_query_argument('name')  # TODO What does this do?
            self.path = os.path.join(self.path, name)
            if self.kind == 'folder':
                self.path += '/'
        elif self.kind == 'folder':
            raise exceptions.InvalidParameters('Path must end with a / if kind is folder')

        length = self.request.headers.get('Content-Length')

        if length is None and self.kind == 'file':
            # Length Required
            raise exceptions.InvalidParameters('Content-Length is required for file uploads', code=411)

        try:
            if int(length) > 0 and self.kind == 'folder':
                # Payload Too Large
                raise exceptions.InvalidParameters('Folder creation requests may not have a body', code=413)
        except ValueError:
                raise exceptions.InvalidParameters('Invalid Content-Length')

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
