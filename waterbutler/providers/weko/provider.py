import os
import tempfile
import logging
import hashlib
import zipfile
import shutil
from urllib.parse import urlparse, urlunparse
from lxml import etree

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.weko.metadata import (
    ITEM_PREFIX,
    get_files,
    split_path,
    WEKOItemMetadata,
    WEKOIndexMetadata,
    WEKODraftFileMetadata,
    WEKODraftFolderMetadata
)
from waterbutler.providers.weko import client
from waterbutler.providers.weko import settings

logger = logging.getLogger(__name__)
IMPORT_XML_SUFFIX = '-import.xml'
IMPORT_ZIP_SUFFIX = '-import.zipimport'


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
        self.nid = self.settings['nid']
        self.BASE_URL = self.settings['url']

        self.token = self.credentials['token']
        self.user_id = self.credentials['user_id']
        self.index_id = self.settings['index_id']
        self.index_title = self.settings['index_title']
        self.connection = client.connect_or_error(self.BASE_URL, self.token)

        self._metadata_cache = {}

    def _get_draft_dir(self):
        d = hashlib.sha256(self.user_id.encode('utf-8')).hexdigest()
        return os.path.join(settings.FILE_PATH_DRAFT, self.nid, d,
                            str(self.index_id))

    def _resolve_target_index(self, index_path):
        if index_path is None:
            return str(self.index_id)
        else:
            return index_path.split('/')[-2][len(ITEM_PREFIX):]

    def _get_draft_metadata(self, draft_path, file_path, index_path):
        fparent, fname = os.path.split(file_path)
        if os.path.isdir(file_path):
            return WEKODraftFolderMetadata({'path': draft_path + fname + '/',
                                            'filepath': file_path},
                                           index_path)
        else:
            stream_size = os.path.getsize(file_path)
            return WEKODraftFileMetadata({'path': draft_path + fname,
                                          'bytes': stream_size,
                                          'filepath': file_path},
                                         index_path)

    def _import_xml(self, target_index_id, import_xml_path):
        import_xml_dir, fname = os.path.split(import_xml_path)
        target_file = os.path.join(import_xml_dir, fname[:-len(IMPORT_XML_SUFFIX)])
        if not os.path.exists(target_file):
            logger.info('Skipped: target file {} does not exist'.format(target_file))
            return
        content_files = []
        with open(import_xml_path, 'rb') as f:
            export_xml = etree.parse(f)
            for repo_file in export_xml.xpath('//repository_file'):
                cname = repo_file.attrib['file_name']
                if os.path.isdir(target_file):
                    if not os.path.exists(os.path.join(target_file, cname)):
                        logger.info('Skipped: repo_file {} does not exist'.format(cname))
                        return
                    else:
                        content_files.append((cname, os.path.join(target_file, cname)))
                else:
                    if os.path.split(target_file)[1] != cname:
                        logger.info('Skipped: repo_file {} does not exist'.format(cname))
                        return
                    else:
                        content_files.append((cname, target_file))
        logger.info('Importing... {} to {}'.format(content_files, target_index_id))
        with tempfile.NamedTemporaryFile(delete=False) as ziptf:
            with zipfile.ZipFile(ziptf, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(import_xml_path, 'import.xml')
                for cname, cpath in content_files:
                    zipf.write(cpath, cname)
            archived_file = ziptf.name
        with open(archived_file, 'rb') as f:
            client.post(self.connection, target_index_id, f,
                        os.path.getsize(archived_file))
        for cname, cpath in content_files:
            os.remove(cpath)
        if os.path.isdir(target_file) and len(get_files(target_file)) == 0:
            shutil.rmtree(target_file)
        os.remove(import_xml_path)

    def _import_zip(self, target_index_id, import_xml_path):
        import_xml_dir, fname = os.path.split(import_xml_path)
        target_file = os.path.join(import_xml_dir, fname[:-len(IMPORT_ZIP_SUFFIX)])
        if not os.path.exists(target_file):
            logger.info('Skipped: target file {} does not exist'.format(target_file))
            return
        logger.info('Importing... {} to {}'.format(target_file, target_index_id))
        with open(target_file, 'rb') as f:
            client.post(self.connection, target_index_id, f,
                        os.path.getsize(target_file))
        os.remove(target_file)
        os.remove(import_xml_path)

    def path_from_metadata(self, parent_path, metadata):
        return parent_path.child(metadata.materialized_name,
                                 _id=metadata.path.strip('/'),
                                 folder=metadata.is_folder)

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
        return WaterButlerPath(path)

    async def download(self, path, revision=None, range=None, **kwargs):
        index_path, draft_path = split_path(path.path)
        if len(draft_path) > 0:
            parent = self._resolve_target_index(index_path)
            draft_root = os.path.join(self._get_draft_dir(), parent)
            assert len([d for d in draft_path if d == '..']) == 0
            file_path = os.path.join(draft_root, draft_path)
            return streams.FileStreamReader(open(file_path, 'rb'))
        else:
            # Dummy implementation for registration
            return streams.StringStream('')

    async def upload(self, stream, path, **kwargs):
        """uploads to WEKO.

        :param waterbutler.core.streams.RequestWrapper stream: The stream to put to WEKO
        :param str path: The filename prepended with '/'

        :rtype: dict, bool
        """
        target_path = path.path[:path.path.rindex('/') + 1] \
                      if '/' in path.path else ''
        index_path, draft_path = split_path(target_path)
        parent_index = self._resolve_target_index(index_path)
        draft_dir = self._get_draft_dir()
        fname = path.path.split('/')[-1]
        assert not fname.startswith(ITEM_PREFIX)
        dest_file = os.path.join(draft_dir, parent_index, draft_path, fname)
        if not os.path.exists(os.path.split(dest_file)[0]):
            os.makedirs(os.path.split(dest_file)[0])
        with open(dest_file, 'wb') as f:
            stream_size = 0
            chunk = await stream.read()
            while chunk:
                f.write(chunk)
                stream_size += len(chunk)
                chunk = await stream.read()

        mt = WEKODraftFileMetadata({'path': draft_path + fname,
                                    'bytes': stream_size,
                                    'filepath': dest_file},
                                    index_path), True
        if fname.endswith(IMPORT_XML_SUFFIX):
            self._import_xml(parent_index, dest_file)
        elif fname.endswith(IMPORT_ZIP_SUFFIX):
            self._import_zip(parent_index, dest_file)
        return mt

    async def create_folder(self, path, folder_precheck=True, **kwargs):
        """
        :param str path: The path to create a folder at
        """

        WaterButlerPath.validate_folder(path)
        target_path = path.path[:path.path.rstrip('/').rindex('/') + 1] \
                      if '/' in path.path.rstrip('/') else ''
        index_path, draft_path = split_path(target_path)
        parent_index = self._resolve_target_index(index_path)
        draft_dir = self._get_draft_dir()
        assert len([d for d in draft_path if d == '..']) == 0
        dname = path.path.split('/')[-2]
        dest_file = os.path.join(draft_dir, parent_index, draft_path, dname)
        if not os.path.exists(dest_file):
            os.makedirs(dest_file)

        return WEKODraftFolderMetadata({'path': path.path,
                                        'filepath': dest_file},
                                       index_path)

    async def delete(self, path, **kwargs):
        """Deletes the key at the specified path

        :param str path: The path of the key to delete
        """
        index_path, draft_path = split_path(path.path)
        if len(draft_path) > 0:
            parent = self._resolve_target_index(index_path)
            draft_root = os.path.join(self._get_draft_dir(), parent)
            if not os.path.exists(draft_root):
                raise exceptions.DeleteError('Draft not found', code=404)
            assert len([d for d in draft_path if d == '..']) == 0
            file_path = os.path.join(draft_root, draft_path)
            if not os.path.exists(file_path):
                raise exceptions.DeleteError('Draft not found', code=404)
            if os.path.isdir(file_path):
                shutil.rmtree(file_path)
            else:
                os.remove(file_path)
        else:
            assert index_path.split('/')[-1][len(ITEM_PREFIX):].startswith('item')
            parent = index_path.split('/')[-2][len(ITEM_PREFIX):]
            item_id = index_path.split('/')[-1][len(ITEM_PREFIX) + 4:]

            indices = client.get_all_indices(self.connection)
            index = [index
                     for index in indices if str(index.identifier) == parent][0]
            delitem = [item
                       for item in client.get_items(self.connection, index)
                       if client.itemId(item.about) == item_id][0]

            scheme, netloc, path, params, oai_query, fragment = urlparse(delitem.about)
            sword_query = 'action=repository_uri&item_id={}'.format(item_id)
            sword_url = urlunparse((scheme, netloc, path, params, sword_query, fragment))
            client.delete(self.connection, sword_url)

    async def metadata(self, path, version=None, **kwargs):
        """
        :param str version:

            - 'latest' for draft files
            - 'latest-published' for published files
            - None for all data
        """
        indices = client.get_all_indices(self.connection)

        index_path, draft_path = split_path(path.path)

        if path.is_root:
            parent = str(self.index_id)
        elif path.is_dir:
            parent = self._resolve_target_index(index_path)
        elif len(draft_path) > 0:
            parent = self._resolve_target_index(index_path)
        else:
            raise exceptions.MetadataError('unsupported', code=404)

        pindices = [index
                    for index in indices if str(index.identifier) == parent]
        if len(pindices) == 0:
            raise exceptions.MetadataError('Index not found', code=404)
        index = pindices[0]
        draft_root = os.path.join(self._get_draft_dir(), parent)

        if len(draft_path) > 0:
            if not os.path.exists(draft_root):
                raise exceptions.MetadataError('Draft not found', code=404)
            assert len([d for d in draft_path if d == '..']) == 0
            file_path = os.path.join(draft_root, draft_path)
            if not os.path.exists(file_path):
                raise exceptions.MetadataError('Draft not found', code=404)
            if os.path.isdir(file_path):
                drafts = [self._get_draft_metadata(draft_path,
                                                   os.path.join(file_path, d),
                                                   index_path)
                          for d in os.listdir(file_path)]
                return drafts
            else:
                return self._get_draft_metadata(os.path.split(draft_path)[0],
                                                file_path,
                                                index_path)
        else:
            # WEKO index
            index_urls = set([index.about for index in indices if str(index.parentIdentifier) == parent])
            ritems = [WEKOItemMetadata(item, index, indices)
                      for item in client.get_items(self.connection, index)
                      if item.about not in index_urls]
            rindices = [WEKOIndexMetadata(index, indices)
                        for index in indices if str(index.parentIdentifier) == parent]
            if os.path.exists(draft_root):
                drafts = [self._get_draft_metadata(draft_path,
                                                   os.path.join(draft_root, d),
                                                   index_path)
                          for d in os.listdir(draft_root)]
            else:
                drafts = []
            return rindices + ritems + drafts

    def can_intra_move(self, dest_provider, path=None):
        logger.debug('can_intra_move: dest_provider={} path={}'.format(dest_provider.NAME, path))
        index_path, draft_path = split_path(path.path)
        if len(draft_path) > 0:
            return False
        return dest_provider.NAME == self.NAME and path.path.endswith('/')

    async def intra_move(self, dest_provider, src_path, dest_path):
        logger.debug('Moved: {}->{}'.format(src_path, dest_path))
        indices = client.get_all_indices(self.connection)

        if src_path.is_root:
            src_path_id = str(self.index_id)
        elif src_path.is_dir:
            src_path_id = src_path.path.split('/')[-2][len(ITEM_PREFIX):]
        else:
            raise exceptions.MetadataError('unsupported', code=404)
        if dest_path.is_root:
            dest_path_id = str(self.index_id)
        else:
            dest_path_id = dest_path.path.split('/')[-2][len(ITEM_PREFIX):]

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
