from waterbutler.core import exceptions


class CreateMixin:

    def prevalidate_put(self):
        """Prevalidation for creation requests. Runs BEFORE the body of a request is accepted and
        before the path given in the url has been validated.  An early rejection here will save us
        one or more API calls to the provider. Requests with bodies that are too large can be
        rejected if we have not began to accept the body. Validation is as follows:

        1. Pull kind from query params. It must be file, folder, or not included (which defaults to file)
        2. Ensure that content length is present for file uploads
        3. Ensure that content length is either not present or 0 for folder creation requests
        """
        self.kind = self.get_query_argument('kind', default='file')

        if self.kind not in ('file', 'folder'):
            raise exceptions.InvalidParameters('Kind must be file, folder or unspecified (interpreted as file), not {}'.format(self.kind))

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

    def postvalidate_put(self):
        """Postvalidation for creation requests. Runs BEFORE the body of a request is accepted, but
        after the path has been validated.  Invalid path+params combinations can be rejected here.
        Validation is as follows:

        1. If path is a folder, the name parameter must be present.
        2. If path is a file, the name parameter must be absent.
        3. If the entity being created is a folder, then path must be a folder as well.
        """

        self.childs_name = self.get_query_argument('name', default=None)

        if self.path.is_dir:
            if self.childs_name is None:
                raise exceptions.InvalidParameters('Missing required parameter \'name\'')
            self.target_path = self.path.child(self.childs_name, folder=(self.kind == 'folder'))
        else:
            if self.childs_name is not None:
                raise exceptions.InvalidParameters("'name' parameter doesn't apply to actions on files")
            if self.kind == 'folder':
                raise exceptions.InvalidParameters(
                    'Path must be a folder (and end with a "/") if trying to create a subfolder',
                    code=409
                )
            self.target_path = self.path

    async def create_folder(self):
        metadata = await self.provider.create_folder(self.target_path)
        self.set_status(201)
        self.write({'data': metadata.json_api_serialized(self.resource)})

    async def upload_file(self):
        self.writer.write_eof()

        metadata, created = await self.uploader
        self.writer.close()
        self.wsock.close()
        if created:
            self.set_status(201)

        self.write({'data': metadata.json_api_serialized(self.resource)})
