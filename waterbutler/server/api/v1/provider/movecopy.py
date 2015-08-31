import time
import asyncio

import tornado.gen

from waterbutler import tasks
from waterbutler.sizes import MBs
from waterbutler.server.api.v0 import core


class MoveCopyMixin:

    @property
    def json(self):
        if not hasattr(self, '_json'):
            # TODO catch exceptions
            self._json = json.loads(self.request.body)
        return self._json

    def validate_post(self):
        if 'Content-Length' not in self.request.headers or self.request.headers['Content-Length'] > 10 * MBs:
            # There should be no JSON body > 10 megs
            raise Exception()

    def build_args(self, dest_provider):
        return ({
            'nid': self.resource,  # TODO rename to anything but nid
            'path': self.path,
            'provider': self.provider.serialized()
        }, {
            'nid': self.json['resource'],
            'path': (yield from dest_provider.validate_path(self.json.get('path')))
            'provider': dest_provider.serialized()
        })

    def move_or_copy(self):
        if self.json.get('action') not in ('copy', 'move'):
            raise Exception()

        dest_auth = yield from auth_handler.get(
            self.json.get('resource'),
            self.json.get('provider'),
            self.request
        )

        dest_provider = make_provider(
            self.json['provider']
            dest_auth['auth'],
            dest_auth['credentials'],
            dest_auth['settings']
        )

        if not getattr(self.provider, 'can_intra_' + self.json['action'])(dest_provider, self.path):
            result = yield from getattr(tasks, self.json['action']).adelay(*(yield from self.build_args(dest_provider)))
            metadata, created = yield from tasks.wait_on_celery(result)
        else:
            metadata, created = (
                yield from tasks.backgrounded(
                    getattr(self.provider, self.json['action']),
                    dest_provider,
                    self.path,
                    (yield from dest_provider.validate_path(self.json.get('path')))
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
