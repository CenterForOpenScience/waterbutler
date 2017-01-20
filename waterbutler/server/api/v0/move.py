import time

from waterbutler import tasks
from waterbutler.server.api.v0 import core
from waterbutler.core import remote_logging


class MoveHandler(core.BaseCrossProviderHandler):
    JSON_REQUIRED = True
    ACTION_MAP = {
        'POST': 'move'
    }

    async def post(self):
        if not self.source_provider.can_intra_move(self.destination_provider, self.json['source']['path']):
            resp = await tasks.move.adelay({
                'nid': self.json['source']['nid'],
                'path': self.json['source']['path'],
                'provider': self.source_provider.serialized()
            }, {
                'nid': self.json['destination']['nid'],
                'path': self.json['destination']['path'],
                'provider': self.destination_provider.serialized()
            },
                rename=self.json.get('rename'),
                conflict=self.json.get('conflict', 'replace'),
                start_time=time.time(),
                request=remote_logging._serialize_request(self.request),
            )

            metadata, created = await tasks.wait_on_celery(resp)

        else:
            metadata, created = (
                await tasks.backgrounded(
                    self.source_provider.move,
                    self.destination_provider,
                    self.json['source']['path'],
                    self.json['destination']['path'],
                    rename=self.json.get('rename'),
                    conflict=self.json.get('conflict', 'replace'),
                )
            )

        if created:
            self.set_status(201)
        else:
            self.set_status(200)

        self.write(metadata.serialized())

        if self.source_provider.can_intra_move(self.destination_provider, self.json['source']['path']):
            self._send_hook('move', metadata)
