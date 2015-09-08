import json
import asyncio

from tornado.web import HTTPError

from waterbutler import tasks
from waterbutler.sizes import MBs
from waterbutler.server import settings
from waterbutler.server.auth import AuthHandler
from waterbutler.core.utils import make_provider

auth_handler = AuthHandler(settings.AUTH_HANDLERS)


class MoveCopyMixin:

    @property
    def json(self):
        if not hasattr(self, '_json'):
            # TODO catch exceptions
            try:
                # Defined by self.data_received
                self._json = json.loads(self.body.decode())
            except ValueError:
                raise HTTPError(400)
        return self._json

    def validate_post(self):
        try:
            if int(self.request.headers['Content-Length']) > 1 * MBs:
                # There should be no JSON body > 1 megs
                raise HTTPError(413)
        except (KeyError, ValueError):
            raise HTTPError(411)

    def build_args(self, dest_provider, dest_path):
        return ({
            'nid': self.resource,  # TODO rename to anything but nid
            'path': self.path,
            'provider': self.provider.serialized()
        }, {
            'nid': self.json['resource'],
            'path': dest_path,
            'provider': dest_provider.serialized()
        })

    @asyncio.coroutine
    def move_or_copy(self):
        # Force the json body to load into memory
        yield self.request.body

        if self.json.get('action') not in ('copy', 'move', 'rename'):
            raise Exception()

        if self.json['action'] == 'rename':
            action = 'move'
            dest_auth = self.auth
            dest_provider = self.provider
            dest_path = self.path.parent
        else:
            if 'path' not in self.json:
                raise Exception()

            action = self.json['action']

            dest_auth = yield from auth_handler.get(
                self.json.get('resource', self.resource),
                self.json.get('provider', self.provider.NAME),
                self.request
            )

            dest_provider = make_provider(
                self.json['provider'],
                dest_auth['auth'],
                dest_auth['credentials'],
                dest_auth['settings']
            )

            dest_path = yield from self.dest_provider(self.json['path'])

        if not getattr(self.provider, 'can_intra_' + action)(dest_provider, self.path):
            result = yield from getattr(tasks, action).adelay(*self.build_args(dest_provider, dest_path))
            metadata, created = yield from tasks.wait_on_celery(result)
        else:
            metadata, created = (
                yield from tasks.backgrounded(
                    getattr(self.provider, action),
                    dest_provider,
                    self.path,
                    dest_path,
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
