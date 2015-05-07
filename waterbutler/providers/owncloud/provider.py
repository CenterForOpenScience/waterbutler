import os
import asyncio
import hashlib
from urllib import parse
from base64 import b64encode
from xml.etree import ElementTree

from waterbutler.core import utils
from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions

from waterbutler.providers.owncloud import settings
from waterbutler.providers.owncloud.metadata import OwnCloudFileMetadata
from waterbutler.providers.owncloud.metadata import OwnCloudFolderMetadata


class OwnCloudPath(utils.WaterButlerPath):

    def __init__(self, path, prefix=False, suffix=True):
        super().__init__(path, prefix=prefix, suffix=suffix)


class OwnCloudProvider(provider.BaseProvider):
    """Provider for OwnCloud
    """

    _DAV_URL = 'remote.php/webdav/'

    def __init__(self, auth, credentials, settings):
        super().__init__(auth, credentials, settings)
        self.auth = b64encode((credentials['username'] + ':' 
            + credentials['password']).encode('ascii')).decode('ascii')
        self.webdav_url = settings['base_url'] + self._DAV_URL
 
    @property
    def default_headers(self):
        return { 'Authorization' : 'Basic {0}'.format(self.auth) }

    @asyncio.coroutine
    def metadata(self, path, **kwargs):
        """Get Metadata about the requested file or folder

        :param str path: The path to a key or folder
        :rtype: dict or list
        """
        path = OwnCloudPath(path)

        url = self.webdav_url + path.path

        resp = yield from self.make_request(
            'PROPFIND',
            url,
            expects=(207, ),
            throws=exceptions.MetadataError,
        )

        data = yield from resp.read()
        items = self._parse_dav_tree(ElementTree.fromstring(data))

        if path.is_dir:
            return items[1:]

        return items[0]

    def _parse_dav_tree(self, dav_tree):
        items = []
        for child in dav_tree:
            items.append(self._parse_dav_element(child))
        return items

    def _parse_dav_element(self, dav_node):

        href = parse.unquote(dav_node.find('{DAV:}href').text)

        path = OwnCloudPath(href)

        node_attrs = {}
        attrs = dav_node.find('{DAV:}propstat')
        attrs = attrs.find('{DAV:}prop')
        for attr in attrs:
            node_attrs[attr.tag] = attr.text

        if path.is_file:
            return OwnCloudFileMetadata(path.path, node_attrs).serialized()

        return OwnCloudFolderMetadata(path.path, node_attrs).serialized()
 
    @asyncio.coroutine
    def download(self, path, **kwargs):

        path = OwnCloudPath(path)
        
        if not path.is_file:
            raise exceptions.DownloadError('No file specified for download', code=400)

        url = self.webdav_url + path.path

        resp = yield from self.make_request(
            'GET',
            url,
            expects=(200, ),
            throws=exceptions.DownloadError,
        )
        return streams.ResponseStreamReader(resp)

    @asyncio.coroutine
    def upload(self, stream, path, **kwargs):

        url = self.webdav_url + path

        resp = yield from self.make_request(
            'PUT',
            url,
            headers={'Content-Length': str(stream.size)},
            data=stream,
            expects=(201, ),
            throws=exceptions.UploadError,
        )

        return True
        

    @asyncio.coroutine
    def delete(self, path, **kwargs):

        url = self.webdav_url + path

        yield from self.make_request(
            'DELETE',
            url,
            expects=(204, ),
            throws=exceptions.DeleteError,
        )
        
        return True