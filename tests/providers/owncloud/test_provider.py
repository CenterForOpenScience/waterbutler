import pytest

import io
from http import client

import aiohttpretty

from waterbutler.core import streams
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.owncloud import OwnCloudProvider
from waterbutler.providers.owncloud.metadata import OwnCloudFileMetadata
from waterbutler.providers.owncloud.metadata import OwnCloudFolderMetadata

@pytest.fixture
def auth():
    return {
        'name': 'cat',
        'email': 'cat@cat.com',
    }

@pytest.fixture
def credentials():
    return {'username':'cat',
            'password':'cat',
            'host':'https://cat/owncloud'}

@pytest.fixture
def settings():
    return {'folder': '/my_folder', 'verify_ssl':False}

@pytest.fixture
def provider(auth, credentials, settings):
    return OwnCloudProvider(auth, credentials, settings)

@pytest.fixture
def folder_list():
    return b'''<?xml version="1.0" ?>
    <d:multistatus xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns" xmlns:s="http://sabredav.org/ns">
        <d:response>
            <d:href>/owncloud/remote.php/webdav/</d:href>
            <d:propstat>
                <d:prop>
                    <d:getlastmodified>Tue, 21 Jun 2016 00:44:03 GMT</d:getlastmodified>
                    <d:resourcetype>
                         <d:collection/>
                    </d:resourcetype>
                    <d:quota-used-bytes>714783</d:quota-used-bytes>
                    <d:quota-available-bytes>-3</d:quota-available-bytes>
                    <d:getetag>&quot;57688dd358fb7&quot;</d:getetag>
                </d:prop>
                <d:status>HTTP/1.1 200 OK</d:status>
            </d:propstat>
        </d:response>
        <d:response>
            <d:href>/owncloud/remote.php/webdav/Documents/</d:href>
            <d:propstat>
                <d:prop>
                    <d:getlastmodified>Tue, 21 Jun 2016 00:44:03 GMT</d:getlastmodified>
                    <d:resourcetype>
                        <d:collection/>
                    </d:resourcetype>
                    <d:quota-used-bytes>36227</d:quota-used-bytes>
                    <d:quota-available-bytes>-3</d:quota-available-bytes>
                    <d:getetag>&quot;57688dd3584b0&quot;</d:getetag>
                </d:prop>
                <d:status>HTTP/1.1 200 OK</d:status>
            </d:propstat>
        </d:response>
        <d:response>
            <d:href>/owncloud/remote.php/webdav/Photos/</d:href>
            <d:propstat>
                <d:prop>
                    <d:getlastmodified>Wed, 15 Jun 2016 22:49:40 GMT</d:getlastmodified>
                    <d:resourcetype>
                        <d:collection/>
                    </d:resourcetype>
                    <d:quota-used-bytes>678556</d:quota-used-bytes>
                    <d:quota-available-bytes>-3</d:quota-available-bytes>
                    <d:getetag>&quot;5761db8485325&quot;</d:getetag>
                </d:prop>
                <d:status>HTTP/1.1 200 OK</d:status>
            </d:propstat>
        </d:response>
     </d:multistatus>'''


@pytest.fixture
def folder_metadata():
    return b'''<?xml version="1.0" ?>
    <d:multistatus xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns" xmlns:s="http://sabredav.org/ns">
        <d:response>
            <d:href>/owncloud/remote.php/webdav/Documents/</d:href>
            <d:propstat>
                <d:prop>
                    <d:getlastmodified>Tue, 21 Jun 2016 00:44:03 GMT</d:getlastmodified>
                    <d:resourcetype>
                        <d:collection/>
                    </d:resourcetype>
                    <d:quota-used-bytes>36227</d:quota-used-bytes>
                    <d:quota-available-bytes>-3</d:quota-available-bytes>
                    <d:getetag>&quot;57688dd3584b0&quot;</d:getetag>
                </d:prop>
                <d:status>HTTP/1.1 200 OK</d:status>
            </d:propstat>
        </d:response>
     </d:multistatus>'''


@pytest.fixture
def folder_contents_metadata():
    return b'''<?xml version="1.0"?>
      <d:multistatus xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns" xmlns:s="http://sabredav.org/ns">
        <d:response>
          <d:href>/remote.php/webdav/Documents/</d:href>
          <d:propstat>
            <d:prop>
              <d:getlastmodified>Thu, 01 Jun 2017 15:53:13 GMT</d:getlastmodified>
              <d:resourcetype>
                <d:collection/>
              </d:resourcetype>
              <d:quota-used-bytes>36227</d:quota-used-bytes>
              <d:quota-available-bytes>-3</d:quota-available-bytes>
              <d:getetag>&quot;5930386978ae6&quot;</d:getetag>
            </d:prop>
            <d:status>HTTP/1.1 200 OK</d:status>
          </d:propstat>
        </d:response>
        <d:response>
          <d:href>/remote.php/webdav/Documents/Example.odt</d:href>
          <d:propstat>
            <d:prop>
              <d:getlastmodified>Fri, 12 May 2017 20:37:35 GMT</d:getlastmodified>
              <d:getcontentlength>36227</d:getcontentlength>
              <d:resourcetype/>
              <d:getetag>&quot;95db455e6e33d57d521c0d4e93496747&quot;</d:getetag>
              <d:getcontenttype>application/vnd.oasis.opendocument.text</d:getcontenttype>
            </d:prop>
            <d:status>HTTP/1.1 200 OK</d:status>
          </d:propstat>
        </d:response>
        <d:response>
          <d:href>/remote.php/webdav/Documents/pumpkin/</d:href>
          <d:propstat>
            <d:prop>
              <d:getlastmodified>Thu, 01 Jun 2017 15:28:10 GMT</d:getlastmodified>
              <d:resourcetype>
                <d:collection/>
              </d:resourcetype>
              <d:quota-used-bytes>0</d:quota-used-bytes>
              <d:quota-available-bytes>-3</d:quota-available-bytes>
              <d:getetag>&quot;5930328a6f9da&quot;</d:getetag>
            </d:prop>
            <d:status>HTTP/1.1 200 OK</d:status>
          </d:propstat>
        </d:response>
        <d:response>
          <d:href>/remote.php/webdav/Documents/squash/</d:href>
          <d:propstat>
            <d:prop>
              <d:getlastmodified>Thu, 01 Jun 2017 15:53:13 GMT</d:getlastmodified>
              <d:resourcetype>
                <d:collection/>
              </d:resourcetype>
              <d:quota-used-bytes>0</d:quota-used-bytes>
              <d:quota-available-bytes>-3</d:quota-available-bytes>
              <d:getetag>&quot;59303869582b4&quot;</d:getetag>
            </d:prop>
            <d:status>HTTP/1.1 200 OK</d:status>
          </d:propstat>
        </d:response>
      </d:multistatus>'''


@pytest.fixture
def file_metadata():
    return b'''<?xml version="1.0"?>
            <d:multistatus xmlns:d="DAV:" xmlns:s="http://sabredav.org/ns" xmlns:oc="http://owncloud.org/ns">
                <d:response>
                    <d:href>/owncloud/remote.php/webdav/Documents/dissertation.aux</d:href>
                    <d:propstat>
                        <d:prop>
                            <d:getlastmodified>Sun, 10 Jul 2016 23:28:31 GMT</d:getlastmodified>
                            <d:getcontentlength>3011</d:getcontentlength>
                            <d:resourcetype/>
                            <d:getetag>&quot;a3c411808d58977a9ecd7485b5b7958e&quot;</d:getetag>
                            <d:getcontenttype>application/octet-stream</d:getcontenttype>
                        </d:prop>
                        <d:status>HTTP/1.1 200 OK</d:status>
                    </d:propstat>
                </d:response>
            </d:multistatus>'''

@pytest.fixture
def file_content():
    return b'SLEEP IS FOR THE WEAK GO SERVE STREAMS'

@pytest.fixture
def file_like(file_content):
    return io.BytesIO(file_content)

@pytest.fixture
def file_stream(file_like):
    return streams.FileStreamReader(file_like)


class TestProviderConstruction:

    def test_base_folder_no_slash(self, auth, credentials):
        provider = OwnCloudProvider(auth, credentials, {'folder': '/foo', 'verify_ssl': False})
        assert provider.folder == '/foo/'

    def test_base_folder_with_slash(self, auth, credentials):
        provider = OwnCloudProvider(auth, credentials, {'folder': '/foo/', 'verify_ssl': False})
        assert provider.folder == '/foo/'


class TestValidatePath:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_file(self, provider, file_metadata):
        path = WaterButlerPath('/triangles.txt', prepend=provider.folder)
        url = provider._webdav_url_ + path.full_path
        aiohttpretty.register_uri('PROPFIND', url, body=file_metadata, auto_length=True, status=207)
        try:
            wb_path_v1 = await provider.validate_v1_path('/triangles.txt')
        except Exception as exc:
            pytest.fail(str(exc))

        wb_path_v0 = await provider.validate_path('/triangles.txt')

        assert wb_path_v1 == wb_path_v0

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_folder(self, provider, folder_metadata):
        path = WaterButlerPath('/myfolder/', prepend=provider.folder)
        url = provider._webdav_url_ + path.full_path
        aiohttpretty.register_uri('PROPFIND', url, body=folder_metadata, auto_length=True, status=207)
        try:
            wb_path_v1 = await provider.validate_v1_path('/myfolder/')
        except Exception as exc:
            pytest.fail(str(exc))

        wb_path_v0 = await provider.validate_path('/myfolder/')

        assert wb_path_v1 == wb_path_v0

class TestCRUD:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download(self, provider, file_metadata, file_like):
        path = WaterButlerPath('/triangles.txt', prepend=provider.folder)
        url = provider._webdav_url_ + path.full_path
        aiohttpretty.register_uri('PROPFIND', url, body=file_metadata, auto_length=True, status=207)
        aiohttpretty.register_uri('GET', url, body=b'squares', auto_length=True, status=200)
        result = await provider.download(path)
        content = await result.response.read()
        assert content == b'squares'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload(self, provider, file_stream, file_metadata):
        path = WaterButlerPath('/phile', prepend=provider.folder)
        url = provider._webdav_url_ + path.full_path
        aiohttpretty.register_uri('PROPFIND', url, body=file_metadata, auto_length=True, status=207)
        aiohttpretty.register_uri('PUT', url, body=b'squares', auto_length=True, status=201)
        metadata, created = await provider.upload(file_stream, path)
        expected = OwnCloudFileMetadata('dissertation.aux','/owncloud/remote.php/webdav/Documents/phile',
        {'{DAV:}getetag':'&quot;a3c411808d58977a9ecd7485b5b7958e&quot;',
        '{DAV:}getlastmodified':'Sun, 10 Jul 2016 23:28:31 GMT',
        '{DAV:}getcontentlength':3011})
        assert created is True
        assert metadata.name == expected.name
        assert metadata.size == expected.size
        assert aiohttpretty.has_call(method='PUT', uri=url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete(self, provider, file_metadata):
        path = WaterButlerPath('/phile', prepend=provider.folder)
        url = provider._webdav_url_ + path.full_path
        aiohttpretty.register_uri('PROPFIND', url, body=file_metadata, auto_length=True, status=207)
        path = await provider.validate_path('/phile')
        url = provider._webdav_url_ + path.full_path
        aiohttpretty.register_uri('DELETE', url, status=204)
        await provider.delete(path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_create_folder(self, provider, folder_contents_metadata):
        path = WaterButlerPath('/pumpkin/', prepend=provider.folder)
        folder_url = provider._webdav_url_ + path.full_path
        aiohttpretty.register_uri('MKCOL', folder_url, status=201)

        parent_url = provider._webdav_url_ + path.parent.full_path
        aiohttpretty.register_uri('PROPFIND', parent_url, body=folder_contents_metadata,
                                  auto_length=True, status=207)

        folder_metadata = await provider.create_folder(path)
        assert folder_metadata.name == 'pumpkin'
        assert folder_metadata.path == '/pumpkin/'


class TestMetadata:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata(self, provider, folder_list):
        path = WaterButlerPath('/', prepend=provider.folder)
        url = provider._webdav_url_ + path.full_path
        aiohttpretty.register_uri('PROPFIND', url, body=folder_list, auto_length=True, status=207)
        path = await provider.validate_path('/')
        url = provider._webdav_url_ + path.full_path
        aiohttpretty.register_uri('PROPFIND', url, body=folder_list, status=207)
        result = await provider.metadata(path)

        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0].kind == 'folder'
        assert result[0].name == 'Documents'

class TestOperations:

    async def test_can_intra_copy(self, provider):
        assert provider.can_intra_copy(provider)

    async def test_can_intra_move(self, provider):
        assert provider.can_intra_move(provider)
