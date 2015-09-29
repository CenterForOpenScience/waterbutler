import json
import asyncio

from waterbutler import tasks
from waterbutler.sizes import MBs
from waterbutler.core import exceptions
from waterbutler.server import settings
from waterbutler.server.auth import AuthHandler
from waterbutler.core.utils import make_provider

auth_handler = AuthHandler(settings.AUTH_HANDLERS)


class MoveCopyMixin:

    @property
    def json(self):
        if not hasattr(self, '_json'):
            try:
                # self.body is defined by self.data_received
                self._json = json.loads(self.body.decode())
            except ValueError:
                raise exceptions.InvalidParameters('Invalid json body')
        return self._json

    def validate_post(self):
        try:
            if int(self.request.headers['Content-Length']) > 1 * MBs:
                # There should be no JSON body > 1 megs
                raise exceptions.InvalidParameters('Request body must be under 1Mb', code=413)
        except (KeyError, ValueError):
            raise exceptions.InvalidParameters('Content-Length is required', code=411)

    def build_args(self):
        return ({
            'nid': self.resource,  # TODO rename to anything but nid
            'path': self.path,
            'provider': self.provider.serialized()
        }, {
            'nid': self.dest_resource,
            'path': self.dest_path,
            'provider': self.dest_provider.serialized()
        })

    @asyncio.coroutine
    def move_or_copy(self):
        # Force the json body to load into memory
        yield self.request.body

        if self.json.get('action') not in ('copy', 'move', 'rename'):
            # Note: null is used as the default to avoid python specific error messages
            raise exceptions.InvalidParameters('Action must be copy, move or rename, not {}'.format(self.json.get('action', 'null')))

        if self.json['action'] == 'rename':
            if not self.json.get('rename'):
                raise exceptions.InvalidParameters('Rename is required for renaming')
            action = 'move'
            self.dest_auth = self.auth
            self.dest_provider = self.provider
            self.dest_path = self.path.parent
            self.dest_resource = self.resource
        else:
            if 'path' not in self.json:
                raise exceptions.InvalidParameters('Path is required for moves or copies')

            action = self.json['action']

            # Note: attached to self so that _send_hook has access to these
            self.dest_resource = self.json.get('resource', self.resource)

            # TODO optimize for same provider and resource
            self.dest_auth = yield from auth_handler.get(
                self.dest_resource,
                self.json.get('provider', self.provider.NAME),
                self.request
            )

            self.dest_provider = make_provider(
                self.json.get('provider', self.provider.NAME),
                self.dest_auth['auth'],
                self.dest_auth['credentials'],
                self.dest_auth['settings']
            )

            self.dest_path = yield from self.dest_provider.validate_path(self.json['path'])

        if not getattr(self.provider, 'can_intra_' + action)(self.dest_provider, self.path):
            result = yield from getattr(tasks, action).adelay(*self.build_args())
            metadata, created = yield from tasks.wait_on_celery(result)
        else:
            metadata, created = (
                yield from tasks.backgrounded(
                    getattr(self.provider, action),
                    self.dest_provider,
                    self.path,
                    self.dest_path,
                    rename=self.json.get('rename'),
                    conflict=self.json.get('conflict', 'replace'),
                )
            )

        metadata = metadata.serialized()

        if created:
            self.set_status(201)
        else:
            self.set_status(200)

        self.write(metadata)
