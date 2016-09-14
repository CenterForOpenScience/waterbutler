from waterbutler.constants import IDENTIFIER_PATHS
from waterbutler.core.path import WaterButlerPath

import logging

logger = logging.getLogger(__name__)


class LogPayload:
    """Simple wrapper class to abstract the object being logged.  Expects the parent resource,
    provider, and either a metadata or WaterButlerPath object.
    """

    def __init__(self, resource, provider, metadata=None, path=None):
        if path is None and metadata is None:
            raise Exception("Log payload needs either a path or metadata.")

        self.resource = resource
        self.provider = provider
        self.metadata = metadata
        self.path = path or WaterButlerPath.from_metadata(metadata)

    def serialize(self):
        """Turn this LogPayload into something decent for mixed company.

        A serialized LogPayload will always have the following keys::

            {
              'kind': 'file', // 'file' or 'folder'
              'materialized': '/foo/bar/baz', // 'full human-readable path'
              'name': 'baz', // just the file or folder basename
              'nid': 'mst3k',  // same thing as 'resource', kept for backcompat
              'path': '/foo/bar/baz', // path of the file, differs from materialized if provider uses IDs
              'provider': 'osfstorage', // the provider being connected to
              'resource': 'mst3k', // the parent entity controlling access to the provider
            }

        If a metadata object is available it will also contain the `etag` and `extra` properties.
        Metadata is `None` when the operation is a DELETE, or when constructing a source payload
        for a move/copy operation.
        """
        payload = {
            'nid': self.resource,
            'resource': self.resource,
        }
        if self.metadata is None:
            payload.update({
                'provider': self.provider.NAME,
                'kind': self.path.kind,
                'path': self.path.identifier_path if self.provider.NAME in IDENTIFIER_PATHS else '/' + self.path.raw_path,
                'name': self.path.name,
                'materialized': self.path.materialized_path,
                'extra': self.path.extra,
            })
        else:
            payload.update(self.metadata.serialized())

        return payload

    @property
    def auth(self):
        """The auth object for the entity.  Contains the callback_url."""
        return self.provider.auth
