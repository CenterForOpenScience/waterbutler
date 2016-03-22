import asyncio

from waterbutler.server.api.v0 import core


class RevisionHandler(core.BaseProviderHandler):

    ACTION_MAP = {
        'GET': 'revisions',
    }

    async def get(self):
        """List revisions of a file"""
        result = self.provider.revisions(**self.arguments)

        if asyncio.iscoroutine(result):
            result = await result

        self.write({'data': [r.serialized() for r in result]})
