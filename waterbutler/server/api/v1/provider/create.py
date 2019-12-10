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
            if length is not None and int(length) > 0 and self.kind == 'folder':
                # Payload Too Large
                raise exceptions.InvalidParameters('Folder creation requests may not have a body', code=413)
        except ValueError:
            raise exceptions.InvalidParameters('Invalid Content-Length')

    async def postvalidate_put(self):
        """Postvalidation for creation requests. Runs BEFORE the body of a request is accepted, but
        after the path has been validated.  Invalid path+params combinations can be rejected here.
        Validation is as follows:

        1. If path is a folder, the name parameter must be present.
        2. If path is a file, the name parameter must be absent.
        3. If the entity being created is a folder, then path must be a folder as well.

        Also checks to make sure V1 semantics are being respected.  If a PUT request is issued
        against a folder then we check to make sure that an entity with the same name does not
        already exist there.  If it does, then we issue a 409 Conflict.  For providers that do not
        allow entites of different types to have the same name (e.g. Github), we also check to make
        sure that such an entity does not exist.
        """

        self.childs_name = self.get_query_argument('name', default=None)

        # handle newfile and newfolder naming conflicts
        if self.path.is_dir:
            if self.childs_name is None:
                raise exceptions.InvalidParameters('Missing required parameter \'name\'')
            self.target_path = self.path.child(self.childs_name, folder=(self.kind == 'folder'))

            # osfstorage, box, and googledrive need ids before calling exists()
            validated_target_path = await self.provider.revalidate_path(
                self.path, self.target_path.name, self.target_path.is_dir
            )

            my_type_exists = await self.provider.exists(validated_target_path)
            if not isinstance(my_type_exists, bool) or my_type_exists:
                raise exceptions.NamingConflict(self.target_path.name)

            if not self.provider.can_duplicate_names():
                target_flipped = self.path.child(self.childs_name, folder=(self.kind != 'folder'))

                # osfstorage, box, and googledrive need ids before calling exists(), but only box
                # disallows can_duplicate_names and needs this.
                validated_target_flipped = await self.provider.revalidate_path(
                    self.path, target_flipped.name, target_flipped.is_dir
                )

                other_exists = await self.provider.exists(validated_target_flipped)
                # the dropbox provider's metadata() method returns a [] here instead of True
                if not isinstance(other_exists, bool) or other_exists:
                    raise exceptions.NamingConflict(self.target_path.name)

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
        self.metadata = await self.provider.create_folder(self.target_path)
        self.set_status(201)
        self.write({'data': self.metadata.json_api_serialized(self.resource)})

    async def upload_file(self):
        self.writer.write_eof()

        self.metadata, created = await self.uploader
        self.writer.close()
        self.wsock.close()
        if created:
            self.set_status(201)

        self.write({'data': self.metadata.json_api_serialized(self.resource)})
