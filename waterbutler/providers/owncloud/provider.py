import os
import asyncio
import hashlib
from urllib import parse
from base64 import b64encode
from xml.etree import ElementTree

import xmltodict

from waterbutler.core import utils
from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions

from waterbutler.providers.owncloud import settings
from waterbutler.providers.owncloud.metadata import OwnCloudFileMetadata

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
        return { 'Authorization' : 'Basic %s' %  self.auth }

    @asyncio.coroutine
    def metadata(self, path, **kwargs):
        """Get Metadata about the requested file or folder

        :param str path: The path to a key or folder
        :rtype: dict or list
        """
        path = OwnCloudPath(path)

        if path.is_dir:
            return (yield from self._metadata_folder(path))

        return (yield from self._metadata_file(path))

    @asyncio.coroutine
    def _metadata_file(self, path):
        url = self.webdav_url + path.path

        resp = yield from self.make_request(
            'PROPFIND',
            url,
            expects=(207, ),
            throws=exceptions.MetadataError,
        )

        data = yield from resp.read()
        tree = ElementTree.fromstring(data)
        
        file_attrs = {}
        attrs = (tree[0]).find('{DAV:}propstat')
        attrs = attrs.find('{DAV:}prop')
        for attr in attrs:
            file_attrs[attr.tag] = attr.text

        return OwnCloudFileMetadata(path.path, file_attrs).serialized()

    @asyncio.coroutine
    def _metadata_folder(self, path):
        pass
 
    @asyncio.coroutine
    def download(self, **kwargs):
        pass

    @asyncio.coroutine
    def upload(self, stream, **kwargs):
        pass

    @asyncio.coroutine
    def delete(self, **kwargs):
        pass