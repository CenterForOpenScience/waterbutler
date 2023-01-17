import io
import asyncio
from http import client

import pytest
import aiohttpretty

from waterbutler.core import streams
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath
from waterbutler.providers.nextcloud import NextcloudProvider
from waterbutler.providers.nextcloud.metadata import (NextcloudFileMetadata,
                                                     NextcloudFileRevisionMetadata)

from unittest import mock
from tests import utils
from tests.providers.nextcloud.fixtures import (
    provider,
    provider_different_credentials,
    auth,
    settings,
    credentials,
    credentials_2,
    credentials_host_with_trailing_slash,
    file_content,
    file_metadata,
    file_metadata_2,
    file_revision_metadata,
    folder_contents_metadata,
    file_metadata_object,
    file_metadata_object_2,
    folder_list,
    folder_metadata,
    file_metadata_unparsable_response,
    file_revision_metadata_error_response,
    moved_folder_metadata,
    moved_parent_folder_metadata,
    file_checksum,
    file_checksum_2,
    file_checksum_3,
    file_checksum_4
)

@pytest.fixture
def file_like(file_content):
    return io.BytesIO(file_content)

@pytest.fixture
def file_stream(file_like):
    return streams.FileStreamReader(file_like)


class TestProviderConstruction:

    def test_base_folder_no_slash(self, auth, credentials):
        provider = NextcloudProvider(auth, credentials, {'folder': '/foo', 'verify_ssl': False})
        assert provider.folder == '/foo/'

    def test_base_folder_with_slash(self, auth, credentials):
        provider = NextcloudProvider(auth, credentials, {'folder': '/foo/', 'verify_ssl': False})
        assert provider.folder == '/foo/'

    def test_webdav_url_trailing_slash(self, auth, credentials_host_with_trailing_slash, provider):

        provider_host_with_trailing_slash = NextcloudProvider(auth,
                                                             credentials_host_with_trailing_slash,
                                                             {'folder': '/foo/',
                                                              'verify_ssl': False})

        expected = 'https://cat/nextcloud/remote.php/webdav/'
        assert expected == provider._webdav_url_
        assert expected == provider_host_with_trailing_slash._webdav_url_


class TestValidatePath:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_root(self, provider):
        assert WaterButlerPath('/', prepend=provider.folder) == await provider.validate_v1_path('/')

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
        aiohttpretty.register_uri('PROPFIND',
                                  url,
                                  body=folder_metadata,
                                  auto_length=True,
                                  status=207)
        try:
            wb_path_v1 = await provider.validate_v1_path('/myfolder/')
        except Exception as exc:
            pytest.fail(str(exc))

        wb_path_v0 = await provider.validate_path('/myfolder/')

        assert wb_path_v1 == wb_path_v0

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_unparsable_dav_response(self, provider, file_metadata_unparsable_response):
        path = WaterButlerPath('/triangles.txt', prepend=provider.folder)
        url = provider._webdav_url_ + path.full_path
        aiohttpretty.register_uri('PROPFIND',
                                  url,
                                  body=file_metadata_unparsable_response,
                                  auto_length=True,
                                  status=207)

        with pytest.raises(exceptions.NotFoundError):
            await provider.validate_v1_path('/triangles.txt')

        try:
            await provider.validate_path('/triangles.txt')
        except Exception as exc:
            pytest.fail(str(exc))

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_v1_own_cloud_404(self, provider, file_metadata_unparsable_response):
        path = WaterButlerPath('/triangles.txt', prepend=provider.folder)
        url = provider._webdav_url_ + path.full_path
        aiohttpretty.register_uri('PROPFIND',
                                  url,
                                  body=file_metadata_unparsable_response,
                                  auto_length=True,
                                  status=404)

        with pytest.raises(exceptions.NotFoundError):
            await provider.validate_v1_path('/triangles.txt')

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_response_different_of_kind_than_path(self, provider, folder_metadata):
        path = WaterButlerPath('/triangles.txt', prepend=provider.folder)
        url = provider._webdav_url_ + path.full_path
        aiohttpretty.register_uri('PROPFIND',
                                  url,
                                  body=folder_metadata,
                                  auto_length=True,
                                  status=207)

        with pytest.raises(exceptions.NotFoundError):
            await provider.validate_v1_path('/triangles.txt')


class TestCRUD:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download(self, provider, file_metadata):
        path = WaterButlerPath('/triangles.txt', prepend=provider.folder)
        url = provider._webdav_url_ + path.full_path
        aiohttpretty.register_uri('PROPFIND', url, body=file_metadata, auto_length=True, status=207)
        aiohttpretty.register_uri('GET', url, body=b'squares', auto_length=True, status=200)
        result = await provider.download(path)
        content = await result.response.read()
        assert content == b'squares'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload(self, provider, file_stream, file_metadata, file_metadata_object, file_checksum):
        path = WaterButlerPath('/phile', prepend=provider.folder)
        url = provider._webdav_url_ + path.full_path
        aiohttpretty.register_uri('PROPFIND', url, body=file_metadata, auto_length=True, status=207)
        aiohttpretty.register_uri('PUT', url, body=b'squares', auto_length=True, status=201)
        checksum_url = provider._ocs_url + 'apps/checksum_api/api/checksum?path=' + file_metadata_object.path + '&hash=md5,sha256,sha512'
        aiohttpretty.register_uri('GET', checksum_url, body=file_checksum, auto_length=True, status=200)
        metadata, created = await provider.upload(file_stream, path)

        assert created is True
        assert metadata.name == file_metadata_object.name
        assert metadata.size == file_metadata_object.size

        if provider.NAME == 'nextcloudinstitutions':
            extra = {
                'hashes': {
                    provider.NAME: {
                        'md5': 'b204384c399505d2b82b7172f3494358',
                        'sha256': '2ddc9cedb34b4e7fb056b7868b1af29af1fe9ee025b8a67c8c63607587baa657',
                        'sha512': '9157b6864199953e1f06d32325f0789ffc0e3cefbf93bf4fd95fa1b15948d6dfdef2d2ca1836ea43ce8271a1f3ae0a589dd2e259435da822c994d3609bd277ae'
                    }
                }
            }
            assert metadata.extra == extra
        assert aiohttpretty.has_call(method='PUT', uri=url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_keep(self, provider, file_stream, file_metadata, file_metadata_object, file_checksum):
        path = WaterButlerPath('/phile', prepend=provider.folder)
        renamed_path = WaterButlerPath('/phile (1)', prepend=provider.folder)
        path._parts[-1]._id = 'fake_id'

        provider.handle_name_conflict = utils.MockCoroutine(return_value=(renamed_path, True))
        url = provider._webdav_url_ + renamed_path.full_path

        aiohttpretty.register_uri('PROPFIND', url, body=file_metadata, auto_length=True, status=207)
        aiohttpretty.register_uri('PUT',
                                  provider._webdav_url_ + '/my_folder/phile (1)',
                                  body=b'squares',
                                  auto_length=True,
                                  status=201)
        checksum_url = provider._ocs_url + 'apps/checksum_api/api/checksum?path=' + file_metadata_object.path + '&hash=md5,sha256,sha512'
        aiohttpretty.register_uri('GET', checksum_url, body=file_checksum, auto_length=True, status=200)
        metadata, created = await provider.upload(file_stream, path, 'keep')

        assert created is True
        assert metadata.name == file_metadata_object.name
        assert metadata.size == file_metadata_object.size

        if provider.NAME == 'nextcloudinstitutions':
            extra = {
                'hashes': {
                    provider.NAME: {
                        'md5': 'b204384c399505d2b82b7172f3494358',
                        'sha256': '2ddc9cedb34b4e7fb056b7868b1af29af1fe9ee025b8a67c8c63607587baa657',
                        'sha512': '9157b6864199953e1f06d32325f0789ffc0e3cefbf93bf4fd95fa1b15948d6dfdef2d2ca1836ea43ce8271a1f3ae0a589dd2e259435da822c994d3609bd277ae'
                    }
                }
            }
            assert metadata.extra == extra
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
    async def test_create_folder(self, provider, folder_contents_metadata, file_checksum):
        path = WaterButlerPath('/pumpkin/', prepend=provider.folder)
        folder_url = provider._webdav_url_ + path.full_path
        aiohttpretty.register_uri('MKCOL', folder_url, status=201)

        parent_url = provider._webdav_url_ + path.parent.full_path
        aiohttpretty.register_uri('PROPFIND', parent_url, body=folder_contents_metadata,
                                  auto_length=True, status=207)

        checksum_url = provider._ocs_url + 'apps/checksum_api/api/checksum?path=/Documents/Example.odt&hash=md5,sha256,sha512'
        aiohttpretty.register_uri('GET', checksum_url, body=file_checksum, auto_length=True, status=200)

        folder_metadata = await provider.create_folder(path)
        assert folder_metadata.name == 'pumpkin'
        assert folder_metadata.path == '/pumpkin/'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_create_folder_naming_conflict(self, provider, folder_contents_metadata):
        path = WaterButlerPath('/pumpkin/', prepend=provider.folder)
        folder_url = provider._webdav_url_ + path.full_path
        aiohttpretty.register_uri('MKCOL', folder_url, status=405)

        with pytest.raises(exceptions.FolderNamingConflict):
            await provider.create_folder(path)


class TestIntraMoveCopy:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_copy_folder(self,
                                     provider,
                                     moved_folder_metadata,
                                     moved_parent_folder_metadata,
                                     file_checksum):
        provider.folder = '/'
        src_path = WaterButlerPath('/moved_folder/', prepend=provider.folder, folder=True)
        dest_path = WaterButlerPath('/parent_folder/moved_folder/',
                                    prepend=provider.folder,
                                    folder=True)

        url = provider._webdav_url_ + src_path.full_path
        metadata_parent_url = provider._webdav_url_ + dest_path.parent.full_path
        metadata_url = provider._webdav_url_ + dest_path.full_path
        aiohttpretty.register_uri('COPY', url, auto_length=True, status=201)
        aiohttpretty.register_uri('PROPFIND', metadata_url, body=moved_folder_metadata, status=207)
        aiohttpretty.register_uri('PROPFIND',
                                  metadata_parent_url,
                                  body=moved_parent_folder_metadata,
                                  status=207)

        checksum_url = provider._ocs_url + 'apps/checksum_api/api/checksum?path=/moved_folder/child_file&hash=md5,sha256,sha512'
        aiohttpretty.register_uri('GET', checksum_url, body=file_checksum, auto_length=True, status=200)

        metadata, exists = await provider.intra_copy(None, src_path, dest_path)

        assert exists
        assert metadata.name == 'moved_folder'
        assert metadata.kind == 'folder'
        assert len(metadata.children) == 1
        assert metadata.children[0].name == 'child_file'
        assert metadata.children[0].kind == 'file'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_move_folder(self,
                                     provider,
                                     moved_folder_metadata,
                                     moved_parent_folder_metadata,
                                     file_checksum):
        provider.folder = '/'
        src_path = WaterButlerPath('/moved_folder/', prepend=provider.folder, folder=True)
        dest_path = WaterButlerPath('/parent_folder/moved_folder/',
                                    prepend=provider.folder,
                                    folder=True)

        url = provider._webdav_url_ + src_path.full_path
        metadata_parent_url = provider._webdav_url_ + dest_path.parent.full_path
        metadata_url = provider._webdav_url_ + dest_path.full_path
        aiohttpretty.register_uri('MOVE', url, auto_length=True, status=201)
        aiohttpretty.register_uri('PROPFIND', metadata_url, body=moved_folder_metadata, status=207)
        aiohttpretty.register_uri('PROPFIND',
                                  metadata_parent_url,
                                  body=moved_parent_folder_metadata,
                                  status=207)

        checksum_url = provider._ocs_url + 'apps/checksum_api/api/checksum?path=/moved_folder/child_file&hash=md5,sha256,sha512'
        aiohttpretty.register_uri('GET', checksum_url, body=file_checksum, auto_length=True, status=200)

        metadata, exists = await provider.intra_move(None, src_path, dest_path)

        assert exists
        assert metadata.name == 'moved_folder'
        assert metadata.kind == 'folder'
        assert len(metadata.children) == 1
        assert metadata.children[0].name == 'child_file'
        assert metadata.children[0].kind == 'file'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_copy_file(self, provider, file_metadata, file_checksum):
        provider.folder = '/'
        src_path = WaterButlerPath('/dissertation.aux', prepend=provider.folder, folder=True)
        dest_path = WaterButlerPath('/parent_folder/', prepend=provider.folder, folder=False)

        url = provider._webdav_url_ + src_path.full_path
        metadata_url = provider._webdav_url_ + dest_path.full_path
        aiohttpretty.register_uri('COPY', url, auto_length=True, status=201)
        aiohttpretty.register_uri('PROPFIND', metadata_url, body=file_metadata, status=207)
        checksum_url = provider._ocs_url + 'apps/checksum_api/api/checksum?path=/Documents/dissertation.aux&hash=md5,sha256,sha512'
        aiohttpretty.register_uri('GET', checksum_url, body=file_checksum, auto_length=True, status=200)

        metadata, exists = await provider.intra_copy(None, src_path, dest_path)

        assert exists
        assert metadata.name == 'dissertation.aux'
        assert metadata.kind == 'file'

        if provider.NAME == 'nextcloudinstitutions':
            extra = {
                'hashes': {
                    provider.NAME: {
                        'md5': 'b204384c399505d2b82b7172f3494358',
                        'sha256': '2ddc9cedb34b4e7fb056b7868b1af29af1fe9ee025b8a67c8c63607587baa657',
                        'sha512': '9157b6864199953e1f06d32325f0789ffc0e3cefbf93bf4fd95fa1b15948d6dfdef2d2ca1836ea43ce8271a1f3ae0a589dd2e259435da822c994d3609bd277ae'
                    }
                }
            }
            assert metadata.extra == extra

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

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_revision(self, provider, file_metadata, file_revision_metadata, file_metadata_object,
                                     file_metadata_2, file_revision_metadata_error_response, file_metadata_object_2,
                                     file_checksum, file_checksum_2, file_checksum_3, file_checksum_4):
        path = WaterButlerPath('/dissertation.aux', prepend=provider.folder)
        url = provider._webdav_url_ + path.full_path
        aiohttpretty.register_uri('PROPFIND', url, body=file_metadata, auto_length=True, status=207)
        url = provider._dav_url_ + 'versions/' + provider.credentials['username'] + '/versions/' + file_metadata_object.fileid
        aiohttpretty.register_uri('PROPFIND', url, body=file_revision_metadata, auto_length=True, status=207)
        checksum_url = provider._ocs_url + 'apps/checksum_api/api/checksum?path=/my_folder/dissertation.aux&hash=md5,sha256,sha512'
        aiohttpretty.register_uri('GET', checksum_url, body=file_checksum, auto_length=True, status=200)
        checksum_url = provider._ocs_url + 'apps/checksum_api/api/checksum?path=/my_folder/dissertation.aux&hash=md5,sha256,sha512&revision=1591876099'
        aiohttpretty.register_uri('GET', checksum_url, body=file_checksum_2, auto_length=True, status=200)
        checksum_url = provider._ocs_url + 'apps/checksum_api/api/checksum?path=/my_folder/dissertation.aux&hash=md5,sha256,sha512&revision=1591864889'
        aiohttpretty.register_uri('GET', checksum_url, body=file_checksum_3, auto_length=True, status=200)
        result = await provider._metadata_revision(path)

        assert isinstance(result, list)
        assert len(result) == 3
        assert isinstance(result[0], NextcloudFileMetadata)
        assert isinstance(result[1], NextcloudFileMetadata)
        assert isinstance(result[2], NextcloudFileMetadata)

        assert result[0].size == '3011'
        assert result[0].etag == '"a3c411808d58977a9ecd7485b5b7958e"'
        assert result[0].modified == 'Sun, 10 Jul 2016 23:28:31 GMT'
        assert result[0].created_utc is None
        assert result[0].content_type == 'application/octet-stream'
        assert result[0].fileid == '7923'

        if provider.NAME == 'nextcloudinstitutions':
            extra = {
                'hashes': {
                    provider.NAME: {
                        'md5': 'b204384c399505d2b82b7172f3494358',
                        'sha256': '2ddc9cedb34b4e7fb056b7868b1af29af1fe9ee025b8a67c8c63607587baa657',
                        'sha512': '9157b6864199953e1f06d32325f0789ffc0e3cefbf93bf4fd95fa1b15948d6dfdef2d2ca1836ea43ce8271a1f3ae0a589dd2e259435da822c994d3609bd277ae'
                    }
                }
            }
            assert result[0].extra == extra

        assert result[1].size == '2983'
        assert result[1].etag == '1591876099'
        assert result[1].modified == 'Sat, 9 Jul 2016 11:48:19 GMT'
        assert result[1].created_utc is None
        assert result[1].content_type == 'application/octet-stream'
        assert result[1].fileid is None

        if provider.NAME == 'nextcloudinstitutions':
            extra2 = {
                'hashes': {
                    provider.NAME: {
                        'md5': 'f1d2177eda4cc227a230d7e39e3e8d5f',
                        'sha256': '276d30cd92b0e86ad51614220ab7f1b74fb4e0dfe9ceeabb4935bcc4693ea1cf',
                        'sha512': '624d95af88516b1c5eb1fe0fd2bbfbcb369ab73057671253233e799cb6666633bf8efb6e2171138ee2566d29365138ff69c6a6681f8563084f0942088d3f78e1'
                    }
                }
            }
            assert result[1].extra == extra2

        assert result[2].size == '2514'
        assert result[2].etag == '1591864889'
        assert result[2].modified == 'Wed, 6 Jul 2016 08:41:29 GMT'
        assert result[2].created_utc is None
        assert result[2].content_type == 'application/octet-stream'
        assert result[2].fileid is None

        if provider.NAME == 'nextcloudinstitutions':
            extra3 = {
                'hashes': {
                    provider.NAME: {
                        'md5': 'ee0558f500468642243e29dc914832e9',
                        'sha256': 'c9b2543ae9c0a94579fa899dde770af9538d93ce6c58948c86c0a6d8f5d1b014',
                        'sha512': '45e0920b6d7850fbaf028a1ee1241154a7641f3ee325efb3fe483d86dba5c170a4b1075d7e7fd2ae0c321def6022f3aa2b59e0c1dc5213bf1c50690f5cf0b688'
                    }
                }
            }
            assert result[2].extra == extra3


        path = WaterButlerPath('/meeting_memo.txt', prepend=provider.folder)
        url = provider._webdav_url_ + path.full_path
        aiohttpretty.register_uri('PROPFIND', url, body=file_metadata_2, auto_length=True, status=207)
        url = provider._dav_url_ + 'versions/' + provider.credentials['username'] + '/versions/' + file_metadata_object_2.fileid
        aiohttpretty.register_uri('PROPFIND', url, body=file_revision_metadata_error_response, auto_length=True, status=404)
        checksum_url = provider._ocs_url + 'apps/checksum_api/api/checksum?path=/my_folder/meeting_memo.txt&hash=md5,sha256,sha512'
        aiohttpretty.register_uri('GET', checksum_url, body=file_checksum_4, auto_length=True, status=200)
        result = await provider._metadata_revision(path)

        assert isinstance(result, list)
        assert len(result) == 1
        assert isinstance(result[0], NextcloudFileMetadata)

        assert result[0].size == '1820'
        assert result[0].etag == '"8acd67d989953d6a02c9e496bb2fe9ff"'
        assert result[0].modified == 'Thu, 11 Jun 2020 08:41:29 GMT'
        assert result[0].created_utc is None
        assert result[0].content_type == 'text/plain'
        assert result[0].fileid == '8512'

        if provider.NAME == 'nextcloudinstitutions':
            extra4 = {
                'hashes': {
                    provider.NAME: {
                        'md5': 'aaef77b9010107820b58385de45c4a98',
                        'sha256': 'd85218389da0e5f5b2f7bfce7306dcb3efde2ceb321aafd68266b11fe7162f84',
                        'sha512': '2f70dbc489cb3bfc29a50798e2301406a7722f696c8ade7411309f7430d690d13fa75a9a3ee4ea194cf351e1bb3738915f97fa76816e3db95b9868b6073a5128'
                    }
                }
            }
            assert result[0].extra == extra4


class TestRevisions:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_revisions(self, provider, file_metadata, file_revision_metadata, file_metadata_object,
                             file_checksum, file_checksum_2, file_checksum_3):
        path = WaterButlerPath('/dissertation.aux', prepend=provider.folder)
        url = provider._webdav_url_ + path.full_path
        aiohttpretty.register_uri('PROPFIND', url, body=file_metadata, auto_length=True, status=207)
        url = provider._dav_url_ + 'versions/' + provider.credentials['username'] + '/versions/' + file_metadata_object.fileid
        aiohttpretty.register_uri('PROPFIND', url, body=file_revision_metadata, auto_length=True, status=207)
        checksum_url = provider._ocs_url + 'apps/checksum_api/api/checksum?path=/my_folder/dissertation.aux&hash=md5,sha256,sha512'
        aiohttpretty.register_uri('GET', checksum_url, body=file_checksum, auto_length=True, status=200)
        checksum_url = provider._ocs_url + 'apps/checksum_api/api/checksum?path=/my_folder/dissertation.aux&hash=md5,sha256,sha512&revision=1591876099'
        aiohttpretty.register_uri('GET', checksum_url, body=file_checksum_2, auto_length=True, status=200)
        checksum_url = provider._ocs_url + 'apps/checksum_api/api/checksum?path=/my_folder/dissertation.aux&hash=md5,sha256,sha512&revision=1591864889'
        aiohttpretty.register_uri('GET', checksum_url, body=file_checksum_3, auto_length=True, status=200)

        result = await provider.revisions(path)

        assert isinstance(result, list)
        assert len(result) == 3
        assert isinstance(result[0], NextcloudFileRevisionMetadata)
        assert isinstance(result[1], NextcloudFileRevisionMetadata)
        assert isinstance(result[2], NextcloudFileRevisionMetadata)

        assert result[0].modified == 'Sun, 10 Jul 2016 23:28:31 GMT'
        assert result[0].version == 'a3c411808d58977a9ecd7485b5b7958e'
        assert result[0].version_identifier == 'revision'

        if provider.NAME == 'nextcloudinstitutions':
            extra = {
                'hashes': {
                    'md5': 'b204384c399505d2b82b7172f3494358',
                    'sha256': '2ddc9cedb34b4e7fb056b7868b1af29af1fe9ee025b8a67c8c63607587baa657'
                }
            }

            assert result[0].extra == extra

        assert result[1].modified == 'Sat, 9 Jul 2016 11:48:19 GMT'
        assert result[1].version == '1591876099'
        assert result[1].version_identifier == 'revision'

        if provider.NAME == 'nextcloudinstitutions':
            extra2 = {
                'hashes': {
                    'md5': 'f1d2177eda4cc227a230d7e39e3e8d5f',
                    'sha256': '276d30cd92b0e86ad51614220ab7f1b74fb4e0dfe9ceeabb4935bcc4693ea1cf'
                }
            }

            assert result[1].extra == extra2

        assert result[2].modified == 'Wed, 6 Jul 2016 08:41:29 GMT'
        assert result[2].version == '1591864889'
        assert result[2].version_identifier == 'revision'

        if provider.NAME == 'nextcloudinstitutions':
            extra3 = {
                'hashes': {
                    'md5': 'ee0558f500468642243e29dc914832e9',
                    'sha256': 'c9b2543ae9c0a94579fa899dde770af9538d93ce6c58948c86c0a6d8f5d1b014'
                }
            }

            assert result[2].extra == extra3


class TestOperations:

    def test_can_intra_copy(self, provider, provider_different_credentials):
        assert provider.can_intra_copy(provider)
        assert not provider.can_intra_copy(provider_different_credentials)

    def test_can_intra_move(self, provider, provider_different_credentials):
        assert provider.can_intra_move(provider)
        assert not provider.can_intra_move(provider_different_credentials)

    def test_shares_storage_root(self, provider, provider_different_credentials):
        assert provider.shares_storage_root(provider)
        assert not provider.shares_storage_root(provider_different_credentials)

    def test_can_duplicate_names(self, provider):
        assert provider.can_duplicate_names()


class FilePathFactory:
    def __init__(self, _href):
        self._href = _href
        self.is_file = True


class TestMetadataFolder:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder_path_is_dir(self, provider, file_metadata, file_revision_metadata, file_metadata_object,
                                     file_metadata_2, file_checksum, file_checksum_2, file_checksum_3):

        path = WaterButlerPath('/dissertation.aux', prepend=provider.folder)
        url = provider._webdav_url_ + path.full_path
        aiohttpretty.register_uri('PROPFIND', url, body=file_metadata, auto_length=True, status=207)
        url = provider._dav_url_ + 'versions/' + provider.credentials['username'] + '/versions/' + file_metadata_object.fileid
        aiohttpretty.register_uri('PROPFIND', url, body=file_revision_metadata, auto_length=True, status=207)
        checksum_url = provider._ocs_url + 'apps/checksum_api/api/checksum?path=/my_folder/dissertation.aux&hash=md5,sha256,sha512'
        aiohttpretty.register_uri('GET', checksum_url, body=file_checksum, auto_length=True, status=200)
        checksum_url = provider._ocs_url + 'apps/checksum_api/api/checksum?path=/my_folder/dissertation.aux&hash=md5,sha256,sha512&revision=1591876099'
        aiohttpretty.register_uri('GET', checksum_url, body=file_checksum_2, auto_length=True, status=200)
        checksum_url = provider._ocs_url + 'apps/checksum_api/api/checksum?path=/my_folder/dissertation.aux&hash=md5,sha256,sha512&revision=1591864889'
        aiohttpretty.register_uri('GET', checksum_url, body=file_checksum_3, auto_length=True, status=200)
        future = asyncio.Future()
        future.set_result([FilePathFactory('/my_folder/dissertation.aux')])
        with mock.patch('waterbutler.providers.nextcloud.utils.parse_dav_response', return_value=future):
            result = await provider._metadata_folder(path)
            assert isinstance(result, list)
            assert len(result) > 0
