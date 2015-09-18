import time

import tornado.gen

from waterbutler import tasks
from waterbutler.server.api.v0 import core


class CopyHandler(core.BaseCrossProviderHandler):
    JSON_REQUIRED = True
    ACTION_MAP = {
        'POST': 'copy'
    }

    @tornado.gen.coroutine
    def post(self):
        if not self.source_provider.can_intra_copy(self.destination_provider, self.json['source']['path']):
            result = yield from tasks.copy.adelay({
                'nid': self.json['source']['nid'],
                'path': self.json['source']['path'],
                'provider': self.source_provider.serialized()
            }, {
                'nid': self.json['destination']['nid'],
                'path': self.json['destination']['path'],
                'provider': self.destination_provider.serialized()
            },
                self.callback_url,
                self.auth,
                rename=self.json.get('rename'),
                conflict=self.json.get('conflict', 'replace'),
                start_time=time.time()
            )

            metadata, created = yield from tasks.wait_on_celery(result)
        else:
            metadata, created = (
                yield from tasks.backgrounded(
                    self.source_provider.copy,
                    self.destination_provider,
                    self.json['source']['path'],
                    self.json['destination']['path'],
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

        if self.source_provider.can_intra_move(self.destination_provider, self.json['source']['path']):
            self._send_hook('copy', metadata)
