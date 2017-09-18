import tempfile
import logging
from urllib.parse import urlparse, urlunparse

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.weko.metadata import WEKOItemMetadata
from waterbutler.providers.weko.metadata import WEKOIndexMetadata
from waterbutler.providers.weko import client

logger = logging.getLogger('waterbutler.providers.weko')


class WEKOProvider(provider.BaseProvider):
    """Provider for WEKO"""

    NAME = 'weko'
    connection = None

    def __init__(self, auth, credentials, settings):
        """
        :param dict auth: Not used
        :param dict credentials: Contains `token`
        :param dict settings: Contains `url`, `index_id` and `index_title` of a repository.
        """
        super().__init__(auth, credentials, settings)
        self.BASE_URL = self.settings['url']

        self.token = self.credentials['token']
        self.index_id = self.settings['index_id']
        self.index_title = self.settings['index_title']
        self.connection = client.connect_or_error(self.BASE_URL, self.token)

        self._metadata_cache = {}

    def build_url(self, path, *segments, **query):
        return super().build_url(*(tuple(path.split('/')) + segments), **query)

    def can_duplicate_names(self):
        return False

    async def validate_v1_path(self, path, **kwargs):
        return await self.validate_path(path, **kwargs)

    async def validate_path(self, path, revision=None, **kwargs):
        """Ensure path is in configured index

        :param str path: The path to a file
        :param list metadata: List of file metadata from _get_data
        """
        wbpath = WaterButlerPath(path)
        wbpath.revision = revision
        return wbpath

    async def revalidate_path(self, base, path, folder=False, revision=None):
        path = path.strip('/')
        wbpath = base.child(path, _id=None, folder=False)
        wbpath.revision = revision or base.revision
        return wbpath

    async def download(self, path, revision=None, range=None, **kwargs):
        # Dummy implementation for registration
        return streams.StringStream('')

    async def upload(self, stream, path, **kwargs):
        """uploads to WEKO.

        :param waterbutler.core.streams.RequestWrapper stream: The stream to put to WEKO
        :param str path: The filename prepended with '/'

        :rtype: dict, bool
        """

        # Write stream to disk (Necessary to find zip file size)
        f = tempfile.TemporaryFile()
        stream_size = 0
        chunk = await stream.read()
        while chunk:
            f.write(chunk)
            stream_size += len(chunk)
            chunk = await stream.read()
        f.seek(0)

        insert_index_id = self.index_id if '/' not in path.path else path.path.split('/')[-2]
        new_item_url = client.post(self.connection, insert_index_id, f, stream_size)

        indices = client.get_all_indices(self.connection)
        index = list(filter(lambda i: str(i.identifier) == insert_index_id,
                            indices))[0]
        nitems = [WEKOItemMetadata(item, index, indices)
                  for item in client.get_items(self.connection, index)
                  if client.itemId(item.about) == client.itemId(new_item_url)]

        return nitems[0], True

    async def delete(self, path, **kwargs):
        """Deletes the key at the specified path

        :param str path: The path of the key to delete
        """
        logger.info('Delete: {}'.format(path.path))
        assert path.path.split('/')[-1].startswith('item')
        parent = path.path.split('/')[-2]
        item_id = path.path.split('/')[-1][4:]

        indices = client.get_all_indices(self.connection)
        index = [index
                 for index in indices if str(index.identifier) == parent][0]
        delitem = [item
                   for item in client.get_items(self.connection, index)
                   if client.itemId(item.about) == item_id][0]

        scheme, netloc, path, params, oai_query, fragment = urlparse(delitem.about)
        sword_query = 'action=repository_uri&item_id={}'.format(item_id)
        sword_url = urlunparse((scheme, netloc, path, params, sword_query, fragment))
        logger.info('Delete target: {} - {}'.format(delitem.title, sword_url))

        client.delete(self.connection, sword_url)

    async def metadata(self, path, version=None, **kwargs):
        """
        :param str version:

            - 'latest' for draft files
            - 'latest-published' for published files
            - None for all data
        """
        version = version or path.revision
        indices = client.get_all_indices(self.connection)

        if path.is_root:
            parent = str(self.index_id)
        elif path.is_dir:
            parent = path.path.split('/')[-2]
        else:
            raise exceptions.MetadataError('unsupported', code=404)

        index = [index
                 for index in indices if str(index.identifier) == parent][0]

        index_urls = set([index.about for index in indices if str(index.parentIdentifier) == parent])
        ritems = [WEKOItemMetadata(item, index, indices)
                  for item in client.get_items(self.connection, index)
                  if item.about not in index_urls]

        rindices = [WEKOIndexMetadata(index, indices)
                    for index in indices if str(index.parentIdentifier) == parent]
        return rindices + ritems

    def can_intra_move(self, dest_provider, path=None):
        logger.debug('can_intra_move: dest_provider={} path={}'.format(dest_provider.NAME, path))
        return dest_provider.NAME == self.NAME and path.path.endswith('/')

    async def intra_move(self, dest_provider, src_path, dest_path):
        logger.debug('Moved: {}->{}'.format(src_path, dest_path))
        indices = client.get_all_indices(self.connection)

        if src_path.is_root:
            src_path_id = str(self.index_id)
        elif src_path.is_dir:
            src_path_id = src_path.path.split('/')[-2]
        else:
            raise exceptions.MetadataError('unsupported', code=404)
        if dest_path.is_root:
            dest_path_id = str(self.index_id)
        else:
            dest_path_id = dest_path.path.split('/')[-2]

        target_index = [index
                        for index in indices
                        if str(index.identifier) == src_path_id][0]
        parent_index = [index
                        for index in indices
                        if str(index.identifier) == dest_path_id][0]
        logger.info('Moving: Index {} to {}'.format(target_index.identifier,
                                                    parent_index.identifier))
        client.update_index(self.connection, target_index.identifier,
                            relation=parent_index.identifier)

        indices = client.get_all_indices(self.connection)
        target_index = [index
                        for index in indices
                        if str(index.identifier) == src_path_id][0]
        return WEKOIndexMetadata(target_index, indices), True

    async def revisions(self, path, **kwargs):
        """Get past versions of the request file.

        :param str path: The path to a key
        :rtype list:
        """

        return []
