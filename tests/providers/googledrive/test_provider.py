import copy
import pytest

import io
from http import client

import aiohttpretty
from json import dumps

from waterbutler.core import streams
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.googledrive import settings as ds
from waterbutler.providers.googledrive import GoogleDriveProvider
from waterbutler.providers.googledrive.provider import GoogleDrivePath
from waterbutler.providers.googledrive.metadata import GoogleDriveRevision
from waterbutler.providers.googledrive.metadata import GoogleDriveFileMetadata
from waterbutler.providers.googledrive.metadata import GoogleDriveFolderMetadata
from waterbutler.providers.googledrive.metadata import GoogleDriveFileRevisionMetadata

from tests.providers.googledrive import fixtures


@pytest.fixture
def file_content():
    return b'SLEEP IS FOR THE WEAK GO SERVE STREAMS'


@pytest.fixture
def file_like(file_content):
    return io.BytesIO(file_content)


@pytest.fixture
def file_stream(file_like):
    return streams.FileStreamReader(file_like)


@pytest.fixture
def auth():
    return {
        'name': 'cat',
        'email': 'cat@cat.com',
    }


@pytest.fixture
def credentials():
    return {'token': 'hugoandkim'}


@pytest.fixture
def settings():
    return {
        'folder': {
            'id': '19003e',
            'name': '/conrad/birdie',
        },
    }


@pytest.fixture
def provider(auth, credentials, settings):
    return GoogleDriveProvider(auth, credentials, settings)


@pytest.fixture
def search_for_file_response():
    return {
        'items': [
            {'id': '1234ideclarethumbwar'}
        ]
    }

@pytest.fixture
def no_file_response():
    return {
        'items': []
    }

@pytest.fixture
def actual_file_response():
    return {
        'id': '1234ideclarethumbwar',
        'mimeType': 'text/plain',
        'title': 'B.txt',
    }

@pytest.fixture
def search_for_folder_response():
    return {
        'items': [
            {'id': 'whyis6afraidof7'}
        ]
    }

@pytest.fixture
def no_folder_response():
    return {
        'items': []
    }

@pytest.fixture
def actual_folder_response():
    return {
        'id': 'whyis6afraidof7',
        'mimeType': 'application/vnd.google-apps.folder',
        'title': 'A',
    }

def _build_title_search_query(provider, entity_name, is_folder=True):
    return "title = '{}' " \
            "and trashed = false " \
            "and mimeType != 'application/vnd.google-apps.form' " \
            "and mimeType != 'application/vnd.google-apps.map' " \
            "and mimeType != 'application/vnd.google-apps.document' " \
            "and mimeType != 'application/vnd.google-apps.drawing' " \
            "and mimeType != 'application/vnd.google-apps.presentation' " \
            "and mimeType != 'application/vnd.google-apps.spreadsheet' " \
            "and mimeType {} '{}'".format(
                entity_name,
                '=' if is_folder else '!=',
                provider.FOLDER_MIME_TYPE
            )

class TestValidatePath:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_file(self, provider, search_for_file_response,
                                         actual_file_response, no_folder_response):
        file_name = 'file.txt'
        file_id = '1234ideclarethumbwar'

        query_url = provider.build_url(
            'files', provider.folder['id'], 'children',
            q=_build_title_search_query(provider, file_name, False),
            fields='items(id)'
        )
        wrong_query_url = provider.build_url(
            'files', provider.folder['id'], 'children',
            q=_build_title_search_query(provider, file_name, True),
            fields='items(id)'
        )
        specific_url = provider.build_url('files', file_id, fields='id,title,mimeType')

        aiohttpretty.register_json_uri('GET', query_url, body=search_for_file_response)
        aiohttpretty.register_json_uri('GET', wrong_query_url, body=no_folder_response)
        aiohttpretty.register_json_uri('GET', specific_url, body=actual_file_response)

        try:
            wb_path_v1 = await provider.validate_v1_path('/' + file_name)
        except Exception as exc:
            pytest.fail(str(exc))

        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider.validate_v1_path('/' + file_name + '/')

        assert exc.value.code == client.NOT_FOUND

        wb_path_v0 = await provider.validate_path('/' + file_name)

        assert wb_path_v1 == wb_path_v0

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_folder(self, provider, search_for_folder_response,
                                           actual_folder_response, no_file_response):
        folder_name = 'foofolder'
        folder_id = 'whyis6afraidof7'

        query_url = provider.build_url(
            'files', provider.folder['id'], 'children',
            q=_build_title_search_query(provider, folder_name, True),
            fields='items(id)'
        )
        wrong_query_url = provider.build_url(
            'files', provider.folder['id'], 'children',
            q=_build_title_search_query(provider, folder_name, False),
            fields='items(id)'
        )
        specific_url = provider.build_url('files', folder_id, fields='id,title,mimeType')

        aiohttpretty.register_json_uri('GET', query_url, body=search_for_folder_response)
        aiohttpretty.register_json_uri('GET', wrong_query_url, body=no_file_response)
        aiohttpretty.register_json_uri('GET', specific_url, body=actual_folder_response)

        try:
            wb_path_v1 = await provider.validate_v1_path('/' + folder_name + '/')
        except Exception as exc:
            pytest.fail(str(exc))

        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider.validate_v1_path('/' + folder_name)

        assert exc.value.code == client.NOT_FOUND

        wb_path_v0 = await provider.validate_path('/' + folder_name + '/')

        assert wb_path_v1 == wb_path_v0


class TestUpload:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_create(self, provider, file_stream):
        upload_id = '7'
        item = fixtures.list_file['items'][0]
        path = WaterButlerPath('/birdie.jpg', _ids=(provider.folder['id'], None))

        start_upload_url = provider._build_upload_url('files', uploadType='resumable')
        finish_upload_url = provider._build_upload_url('files', uploadType='resumable', upload_id=upload_id)

        aiohttpretty.register_json_uri('PUT', finish_upload_url, body=item)
        aiohttpretty.register_uri('POST', start_upload_url, headers={'LOCATION': 'http://waterbutler.io?upload_id={}'.format(upload_id)})

        result, created = await provider.upload(file_stream, path)

        expected = GoogleDriveFileMetadata(item, path)

        assert created is True
        assert result == expected
        assert aiohttpretty.has_call(method='PUT', uri=finish_upload_url)
        assert aiohttpretty.has_call(method='POST', uri=start_upload_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_doesnt_unquote(self, provider, file_stream):
        upload_id = '7'
        item = fixtures.list_file['items'][0]
        path = GoogleDrivePath('/birdie%2F %20".jpg', _ids=(provider.folder['id'], None))

        start_upload_url = provider._build_upload_url('files', uploadType='resumable')
        finish_upload_url = provider._build_upload_url('files', uploadType='resumable', upload_id=upload_id)

        aiohttpretty.register_json_uri('PUT', finish_upload_url, body=item)
        aiohttpretty.register_uri('POST', start_upload_url, headers={'LOCATION': 'http://waterbutler.io?upload_id={}'.format(upload_id)})

        result, created = await provider.upload(file_stream, path)

        expected = GoogleDriveFileMetadata(item, path)

        assert created is True
        assert result == expected
        assert aiohttpretty.has_call(method='POST', uri=start_upload_url)
        assert aiohttpretty.has_call(method='PUT', uri=finish_upload_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_update(self, provider, file_stream):
        upload_id = '7'
        item = fixtures.list_file['items'][0]
        path = WaterButlerPath('/birdie.jpg', _ids=(provider.folder['id'], item['id']))

        start_upload_url = provider._build_upload_url('files', path.identifier, uploadType='resumable')
        finish_upload_url = provider._build_upload_url('files', path.identifier, uploadType='resumable', upload_id=upload_id)

        aiohttpretty.register_json_uri('PUT', finish_upload_url, body=item)
        aiohttpretty.register_uri('PUT', start_upload_url, headers={'LOCATION': 'http://waterbutler.io?upload_id={}'.format(upload_id)})
        result, created = await provider.upload(file_stream, path)

        assert aiohttpretty.has_call(method='PUT', uri=start_upload_url)
        assert aiohttpretty.has_call(method='PUT', uri=finish_upload_url)
        assert created is False
        expected = GoogleDriveFileMetadata(item, path)
        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_create_nested(self, provider, file_stream):
        upload_id = '7'
        item = fixtures.list_file['items'][0]
        path = WaterButlerPath(
            '/ed/sullivan/show.mp3',
            _ids=[str(x) for x in range(3)]
        )

        start_upload_url = provider._build_upload_url('files', uploadType='resumable')
        finish_upload_url = provider._build_upload_url('files', uploadType='resumable', upload_id=upload_id)
        aiohttpretty.register_uri('POST', start_upload_url, headers={'LOCATION': 'http://waterbutler.io?upload_id={}'.format(upload_id)})
        aiohttpretty.register_json_uri('PUT', finish_upload_url, body=item)
        result, created = await provider.upload(file_stream, path)

        assert aiohttpretty.has_call(method='POST', uri=start_upload_url)
        assert aiohttpretty.has_call(method='PUT', uri=finish_upload_url)
        assert created is True
        expected = GoogleDriveFileMetadata(item, path)
        assert result == expected


class TestDelete:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete(self, provider):
        item = fixtures.list_file['items'][0]
        path = WaterButlerPath('/birdie.jpg', _ids=(None, item['id']))
        delete_url = provider.build_url('files', item['id'])
        del_url_body = dumps({'labels': {'trashed': 'true'}})
        aiohttpretty.register_uri('PUT',
                                  delete_url,
                                  body=del_url_body,
                                  status=200)

        result = await provider.delete(path)

        assert result is None
        assert aiohttpretty.has_call(method='PUT', uri=delete_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_folder(self, provider):
        item = fixtures.folder_metadata
        del_url = provider.build_url('files', item['id'])
        del_url_body = dumps({'labels': {'trashed': 'true'}})

        path = WaterButlerPath('/foobar/', _ids=('doesntmatter', item['id']))

        aiohttpretty.register_uri('PUT',
                                  del_url,
                                  body=del_url_body,
                                  status=200)

        result = await provider.delete(path)

        assert aiohttpretty.has_call(method='PUT', uri=del_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_not_existing(self, provider):
        with pytest.raises(exceptions.NotFoundError):
            await provider.delete(WaterButlerPath('/foobar/'))


class TestDownload:
    """Google Docs (incl. Google Sheets, Google Slides, etc.) require extra API calls and use a
    different branch for downloading/exporting files than non-GDoc files.  For brevity's sake
    our non-gdoc test files are called jpegs, though it could stand for any type of file.

    We want to test all the permutations of:

    * editability: editable vs. viewable files
    * file type: Google doc vs. non-Google Doc (e.g. jpeg)
    * revision parameter: non, valid, invalid, and magic

    Non-editable (viewable) GDocs do not support revisions, so the good and bad revisions tests
    are the same.  Both should 404.

    The notion of a GDOC_GOOD_REVISION being the same as a JPEG_BAD_REVISION and vice-versa is an
    unnecessary flourish for testing purposes.  I'm only including it to remind developers that
    GDoc revisions look very different from non-GDoc revisions in production.
    """

    GDOC_GOOD_REVISION = '1'
    GDOC_BAD_REVISION = '0B74RCNS4TbRVTitFais4VzVmQlQ4S0docGlhelk5MXE3OFJnPQ'
    JPEG_GOOD_REVISION = GDOC_BAD_REVISION
    JPEG_BAD_REVISION = GDOC_GOOD_REVISION
    MAGIC_REVISION = '"LUxk1DXE_0fd4yeJDIgpecr5uPA/MTQ5NTExOTgxMzgzOQ"{}'.format(
        ds.DRIVE_IGNORE_VERSION)

    GDOC_EXPORT_MIME_TYPE = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_editable_gdoc_no_revision(self, provider):
        metadata_body = fixtures.sharing['editable_gdoc']['metadata']
        path = GoogleDrivePath(
            '/sharing/editable_gdoc',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_query = provider._build_query(path.identifier)
        metadata_url = provider.build_url('files', path.identifier)
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_body)

        revisions_body = fixtures.sharing['editable_gdoc']['revisions']
        revisions_url = provider.build_url('files', metadata_body['id'], 'revisions')
        aiohttpretty.register_json_uri('GET', revisions_url, body=revisions_body)

        file_content = b'we love you conrad'
        download_file_url = metadata_body['exportLinks'][self.GDOC_EXPORT_MIME_TYPE]
        aiohttpretty.register_uri('GET', download_file_url, body=file_content, auto_length=True)

        result = await provider.download(path)
        assert result.name == 'editable_gdoc.docx'

        content = await result.read()
        assert content == file_content
        assert aiohttpretty.has_call(method='GET', uri=metadata_url)
        assert aiohttpretty.has_call(method='GET', uri=revisions_url)
        assert aiohttpretty.has_call(method='GET', uri=download_file_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_editable_gdoc_good_revision(self, provider):
        metadata_body = fixtures.sharing['editable_gdoc']['metadata']
        path = GoogleDrivePath(
            '/sharing/editable_gdoc',
            _ids=['1', '2', metadata_body['id']]
        )

        revision_body = fixtures.sharing['editable_gdoc']['revision']
        revision_url = provider.build_url('files', metadata_body['id'],
                                          'revisions', self.GDOC_GOOD_REVISION)
        aiohttpretty.register_json_uri('GET', revision_url, body=revision_body)

        file_content = b'we love you conrad'
        download_file_url = revision_body['exportLinks'][self.GDOC_EXPORT_MIME_TYPE]
        aiohttpretty.register_uri('GET', download_file_url, body=file_content, auto_length=True)

        result = await provider.download(path, revision=self.GDOC_GOOD_REVISION)
        assert result.name == 'editable_gdoc.docx'

        content = await result.read()
        assert content == file_content
        assert aiohttpretty.has_call(method='GET', uri=revision_url)
        assert aiohttpretty.has_call(method='GET', uri=download_file_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_editable_gdoc_bad_revision(self, provider):
        metadata_body = fixtures.sharing['editable_gdoc']['metadata']
        path = GoogleDrivePath(
            '/sharing/editable_gdoc',
            _ids=['1', '2', metadata_body['id']]
        )

        no_such_revision_error = fixtures.make_no_such_revision_error(self.GDOC_BAD_REVISION)
        revision_url = provider.build_url('files', metadata_body['id'],
                                          'revisions', self.GDOC_BAD_REVISION)
        aiohttpretty.register_json_uri('GET', revision_url, status=404, body=no_such_revision_error)

        with pytest.raises(exceptions.NotFoundError) as e:
            await provider.download(path, revision=self.GDOC_BAD_REVISION)

        assert e.value.code == 404

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_editable_gdoc_magic_revision(self, provider):
        metadata_body = fixtures.sharing['editable_gdoc']['metadata']
        path = GoogleDrivePath(
            '/sharing/editable_gdoc',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_query = provider._build_query(path.identifier)
        metadata_url = provider.build_url('files', path.identifier)
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_body)

        revisions_body = fixtures.sharing['editable_gdoc']['revisions']
        revisions_url = provider.build_url('files', metadata_body['id'], 'revisions')
        aiohttpretty.register_json_uri('GET', revisions_url, body=revisions_body)

        file_content = b'we love you conrad'
        download_file_url = metadata_body['exportLinks'][self.GDOC_EXPORT_MIME_TYPE]
        aiohttpretty.register_uri('GET', download_file_url, body=file_content, auto_length=True)

        result = await provider.download(path, revision=self.MAGIC_REVISION)
        assert result.name == 'editable_gdoc.docx'

        content = await result.read()
        assert content == file_content
        assert aiohttpretty.has_call(method='GET', uri=metadata_url)
        assert aiohttpretty.has_call(method='GET', uri=revisions_url)
        assert aiohttpretty.has_call(method='GET', uri=download_file_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_viewable_gdoc_no_revision(self, provider):
        metadata_body = fixtures.sharing['viewable_gdoc']['metadata']
        path = GoogleDrivePath(
            '/sharing/viewaable_gdoc',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_query = provider._build_query(path.identifier)
        metadata_url = provider.build_url('files', path.identifier)
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_body)

        file_content = b'we love you conrad'
        download_file_url = metadata_body['exportLinks'][self.GDOC_EXPORT_MIME_TYPE]
        aiohttpretty.register_uri('GET', download_file_url, body=file_content, auto_length=True)

        result = await provider.download(path)
        assert result.name == 'viewable_gdoc.docx'

        content = await result.read()
        assert content == file_content
        assert aiohttpretty.has_call(method='GET', uri=metadata_url)
        assert aiohttpretty.has_call(method='GET', uri=download_file_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_viewable_gdoc_bad_revision(self, provider):
        metadata_body = fixtures.sharing['viewable_gdoc']['metadata']
        path = GoogleDrivePath(
            '/sharing/viewable_gdoc',
            _ids=['1', '2', metadata_body['id']]
        )

        unauthorized_error = fixtures.make_unauthorized_file_access_error(metadata_body['id'])
        revision_url = provider.build_url('files', metadata_body['id'],
                                          'revisions', self.GDOC_BAD_REVISION)
        aiohttpretty.register_json_uri('GET', revision_url, status=404, body=unauthorized_error)

        with pytest.raises(exceptions.NotFoundError) as e:
            await provider.download(path, revision=self.GDOC_BAD_REVISION)

        assert e.value.code == 404

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_viewable_gdoc_magic_revision(self, provider):
        metadata_body = fixtures.sharing['viewable_gdoc']['metadata']
        path = GoogleDrivePath(
            '/sharing/viewable_gdoc',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_query = provider._build_query(path.identifier)
        metadata_url = provider.build_url('files', path.identifier)
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_body)

        file_content = b'we love you conrad'
        download_file_url = metadata_body['exportLinks'][self.GDOC_EXPORT_MIME_TYPE]
        aiohttpretty.register_uri('GET', download_file_url, body=file_content, auto_length=True)

        result = await provider.download(path, revision=self.MAGIC_REVISION)
        assert result.name == 'viewable_gdoc.docx'

        content = await result.read()
        assert content == file_content
        assert aiohttpretty.has_call(method='GET', uri=metadata_url)
        assert aiohttpretty.has_call(method='GET', uri=download_file_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_editable_jpeg_no_revision(self, provider):
        metadata_body = fixtures.sharing['editable_jpeg']['metadata']
        path = GoogleDrivePath(
            '/sharing/editable_jpeg.jpeg',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_query = provider._build_query(path.identifier)
        metadata_url = provider.build_url('files', path.identifier)
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_body)

        file_content = b'we love you conrad'
        download_file_url = metadata_body['downloadUrl']
        aiohttpretty.register_uri('GET', download_file_url, body=file_content, auto_length=True)

        result = await provider.download(path)

        content = await result.read()
        assert content == file_content
        assert aiohttpretty.has_call(method='GET', uri=metadata_url)
        assert aiohttpretty.has_call(method='GET', uri=download_file_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_editable_jpeg_good_revision(self, provider):
        metadata_body = fixtures.sharing['editable_jpeg']['metadata']
        path = GoogleDrivePath(
            '/sharing/editable_jpeg.jpeg',
            _ids=['1', '2', metadata_body['id']]
        )

        revision_body = fixtures.sharing['editable_jpeg']['revision']
        revision_url = provider.build_url('files', metadata_body['id'],
                                          'revisions', self.JPEG_GOOD_REVISION)
        aiohttpretty.register_json_uri('GET', revision_url, body=revision_body)

        file_content = b'we love you conrad'
        download_file_url = revision_body['downloadUrl']
        aiohttpretty.register_uri('GET', download_file_url, body=file_content, auto_length=True)

        result = await provider.download(path, revision=self.JPEG_GOOD_REVISION)

        content = await result.read()
        assert content == file_content
        assert aiohttpretty.has_call(method='GET', uri=revision_url)
        assert aiohttpretty.has_call(method='GET', uri=download_file_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_editable_jpeg_bad_revision(self, provider):
        metadata_body = fixtures.sharing['editable_jpeg']['metadata']
        path = GoogleDrivePath(
            '/sharing/editable_jpeg.jpeg',
            _ids=['1', '2', metadata_body['id']]
        )

        no_such_revision_error = fixtures.make_no_such_revision_error(self.JPEG_BAD_REVISION)
        revision_url = provider.build_url('files', metadata_body['id'],
                                          'revisions', self.JPEG_BAD_REVISION)
        aiohttpretty.register_json_uri('GET', revision_url, status=404, body=no_such_revision_error)

        with pytest.raises(exceptions.NotFoundError) as e:
            await provider.download(path, revision=self.JPEG_BAD_REVISION)

        assert e.value.code == 404

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_editable_jpeg_magic_revision(self, provider):
        metadata_body = fixtures.sharing['editable_jpeg']['metadata']
        path = GoogleDrivePath(
            '/sharing/editable_jpeg.jpeg',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_query = provider._build_query(path.identifier)
        metadata_url = provider.build_url('files', path.identifier)
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_body)

        file_content = b'we love you conrad'
        download_file_url = metadata_body['downloadUrl']
        aiohttpretty.register_uri('GET', download_file_url, body=file_content, auto_length=True)

        result = await provider.download(path, revision=self.MAGIC_REVISION)

        content = await result.read()
        assert content == file_content
        assert aiohttpretty.has_call(method='GET', uri=metadata_url)
        assert aiohttpretty.has_call(method='GET', uri=download_file_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_viewable_jpeg_no_revision(self, provider):
        metadata_body = fixtures.sharing['viewable_jpeg']['metadata']
        path = GoogleDrivePath(
            '/sharing/viewaable_jpeg.jpeg',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_query = provider._build_query(path.identifier)
        metadata_url = provider.build_url('files', path.identifier)
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_body)

        file_content = b'we love you conrad'
        download_file_url = metadata_body['downloadUrl']
        aiohttpretty.register_uri('GET', download_file_url, body=file_content, auto_length=True)

        result = await provider.download(path)

        content = await result.read()
        assert content == file_content
        assert aiohttpretty.has_call(method='GET', uri=metadata_url)
        assert aiohttpretty.has_call(method='GET', uri=download_file_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_viewable_jpeg_bad_revision(self, provider):
        metadata_body = fixtures.sharing['viewable_jpeg']['metadata']
        path = GoogleDrivePath(
            '/sharing/viewable_jpeg.jpeg',
            _ids=['1', '2', metadata_body['id']]
        )

        unauthorized_error = fixtures.make_unauthorized_file_access_error(metadata_body['id'])
        revision_url = provider.build_url('files', metadata_body['id'],
                                          'revisions', self.JPEG_BAD_REVISION)
        aiohttpretty.register_json_uri('GET', revision_url, status=404, body=unauthorized_error)

        with pytest.raises(exceptions.NotFoundError) as e:
            await provider.download(path, revision=self.JPEG_BAD_REVISION)

        assert e.value.code == 404

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_viewable_jpeg_magic_revision(self, provider):
        metadata_body = fixtures.sharing['viewable_jpeg']['metadata']
        path = GoogleDrivePath(
            '/sharing/viewable_jpeg.jpeg',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_query = provider._build_query(path.identifier)
        metadata_url = provider.build_url('files', path.identifier)
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_body)

        file_content = b'we love you conrad'
        download_file_url = metadata_body['downloadUrl']
        aiohttpretty.register_uri('GET', download_file_url, body=file_content, auto_length=True)

        result = await provider.download(path, revision=self.MAGIC_REVISION)

        content = await result.read()
        assert content == file_content
        assert aiohttpretty.has_call(method='GET', uri=metadata_url)
        assert aiohttpretty.has_call(method='GET', uri=download_file_url)


class TestMetadata:
    """Google Docs (incl. Google Sheets, Google Slides, etc.) require extra API calls and use a
    different branch for fetching metadata about files than non-GDoc files.  For brevity's sake
    our non-gdoc test files are called jpegs, though it could stand for any type of file.

    We want to test all the permutations of:

    * editability: editable vs. viewable files
    * file type: Google doc vs. non-Google Doc (e.g. jpeg)
    * revision parameter: non, valid, invalid, and magic

    Non-editable (viewable) GDocs do not support revisions, so the good and bad revisions tests
    are the same.  Both should 404.

    The notion of a GDOC_GOOD_REVISION being the same as a JPEG_BAD_REVISION and vice-versa is an
    unnecessary flourish for testing purposes.  I'm only including it to remind developers that
    GDoc revisions look very different from non-GDoc revisions in production.
    """

    GDOC_GOOD_REVISION = '1'
    GDOC_BAD_REVISION = '0B74RCNS4TbRVTitFais4VzVmQlQ4S0docGlhelk5MXE3OFJnPQ'
    JPEG_GOOD_REVISION = GDOC_BAD_REVISION
    JPEG_BAD_REVISION = GDOC_GOOD_REVISION
    MAGIC_REVISION = '"LUxk1DXE_0fd4yeJDIgpecr5uPA/MTQ5NTExOTgxMzgzOQ"{}'.format(
        ds.DRIVE_IGNORE_VERSION)

    GDOC_EXPORT_MIME_TYPE = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file_root(self, provider):
        path = WaterButlerPath('/birdie.jpg', _ids=(provider.folder['id'], fixtures.list_file['items'][0]['id']))

        list_file_url = provider.build_url('files', path.identifier)
        aiohttpretty.register_json_uri('GET', list_file_url, body=fixtures.list_file['items'][0])

        result = await provider.metadata(path)

        expected = GoogleDriveFileMetadata(fixtures.list_file['items'][0], path)
        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file_root_not_found(self, provider):
        path = WaterButlerPath('/birdie.jpg', _ids=(provider.folder['id'], None))

        with pytest.raises(exceptions.MetadataError) as exc_info:
            await provider.metadata(path)

        assert exc_info.value.code == 404

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file_nested(self, provider):
        path = GoogleDrivePath(
            '/hugo/kim/pins',
            _ids=[str(x) for x in range(4)]
        )

        item = fixtures.generate_list(3)['items'][0]
        url = provider.build_url('files', path.identifier)

        aiohttpretty.register_json_uri('GET', url, body=item)

        result = await provider.metadata(path)

        expected = GoogleDriveFileMetadata(item, path)
        assert result == expected
        assert aiohttpretty.has_call(method='GET', uri=url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_root_folder(self, provider):
        path = await provider.validate_path('/')
        query = provider._build_query(provider.folder['id'])
        list_file_url = provider.build_url('files', q=query, alt='json', maxResults=1000)
        aiohttpretty.register_json_uri('GET', list_file_url, body=fixtures.list_file)

        result = await provider.metadata(path)

        expected = GoogleDriveFileMetadata(
            fixtures.list_file['items'][0],
            path.child(fixtures.list_file['items'][0]['title'])
        )
        assert result == [expected]

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder_nested(self, provider):
        path = GoogleDrivePath(
            '/hugo/kim/pins/',
            _ids=[str(x) for x in range(4)]
        )

        body = fixtures.generate_list(3)
        item = body['items'][0]

        query = provider._build_query(path.identifier)
        url = provider.build_url('files', q=query, alt='json', maxResults=1000)

        aiohttpretty.register_json_uri('GET', url, body=body)

        result = await provider.metadata(path)

        expected = GoogleDriveFileMetadata(item, path.child(item['title']))

        assert result == [expected]
        assert aiohttpretty.has_call(method='GET', uri=url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_folder_metadata(self, provider):
        path = GoogleDrivePath(
            '/hugo/kim/pins/',
            _ids=[str(x) for x in range(4)]
        )

        body = fixtures.generate_list(3, **fixtures.folder_metadata)
        item = body['items'][0]

        query = provider._build_query(path.identifier)
        url = provider.build_url('files', q=query, alt='json', maxResults=1000)

        aiohttpretty.register_json_uri('GET', url, body=body)

        result = await provider.metadata(path)

        expected = GoogleDriveFolderMetadata(item, path.child(item['title'], folder=True))

        assert result == [expected]
        assert aiohttpretty.has_call(method='GET', uri=url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_editable_gdoc_no_revision(self, provider):
        metadata_body = fixtures.sharing['editable_gdoc']['metadata']
        path = GoogleDrivePath(
            '/sharing/editable_gdoc',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_query = provider._build_query(path.identifier)
        metadata_url = provider.build_url('files', path.identifier)
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_body)

        revisions_body = fixtures.sharing['editable_gdoc']['revisions']
        revisions_url = provider.build_url('files', metadata_body['id'], 'revisions')
        aiohttpretty.register_json_uri('GET', revisions_url, body=revisions_body)

        result = await provider.metadata(path)

        local_metadata = copy.deepcopy(metadata_body)
        local_metadata['version'] = revisions_body['items'][-1]['id']
        expected = GoogleDriveFileMetadata(local_metadata, path)
        assert result == expected
        assert aiohttpretty.has_call(method='GET', uri=metadata_url)
        assert aiohttpretty.has_call(method='GET', uri=revisions_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_editable_gdoc_good_revision(self, provider):
        metadata_body = fixtures.sharing['editable_gdoc']['metadata']
        path = GoogleDrivePath(
            '/sharing/editable_gdoc',
            _ids=['1', '2', metadata_body['id']]
        )

        revision_body = fixtures.sharing['editable_gdoc']['revision']
        revision_url = provider.build_url('files', metadata_body['id'],
                                          'revisions', self.GDOC_GOOD_REVISION)
        aiohttpretty.register_json_uri('GET', revision_url, body=revision_body)

        result = await provider.metadata(path, revision=self.GDOC_GOOD_REVISION)

        expected = GoogleDriveFileRevisionMetadata(revision_body, path)
        assert result == expected
        assert aiohttpretty.has_call(method='GET', uri=revision_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_editable_gdoc_bad_revision(self, provider):
        metadata_body = fixtures.sharing['editable_gdoc']['metadata']
        path = GoogleDrivePath(
            '/sharing/editable_gdoc',
            _ids=['1', '2', metadata_body['id']]
        )

        no_such_revision_error = fixtures.make_no_such_revision_error(self.GDOC_BAD_REVISION)
        revision_url = provider.build_url('files', metadata_body['id'],
                                          'revisions', self.GDOC_BAD_REVISION)
        aiohttpretty.register_json_uri('GET', revision_url, status=404, body=no_such_revision_error)

        with pytest.raises(exceptions.NotFoundError) as e:
            await provider.metadata(path, revision=self.GDOC_BAD_REVISION)

        assert e.value.code == 404

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_editable_gdoc_magic_revision(self, provider):
        metadata_body = fixtures.sharing['editable_gdoc']['metadata']
        path = GoogleDrivePath(
            '/sharing/editable_gdoc',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_query = provider._build_query(path.identifier)
        metadata_url = provider.build_url('files', path.identifier)
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_body)

        revisions_body = fixtures.sharing['editable_gdoc']['revisions']
        revisions_url = provider.build_url('files', metadata_body['id'], 'revisions')
        aiohttpretty.register_json_uri('GET', revisions_url, body=revisions_body)

        result = await provider.metadata(path, revision=self.MAGIC_REVISION)

        local_metadata = copy.deepcopy(metadata_body)
        local_metadata['version'] = revisions_body['items'][-1]['id']
        expected = GoogleDriveFileMetadata(local_metadata, path)
        assert result == expected
        assert aiohttpretty.has_call(method='GET', uri=metadata_url)
        assert aiohttpretty.has_call(method='GET', uri=revisions_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_viewable_gdoc_no_revision(self, provider):
        metadata_body = fixtures.sharing['viewable_gdoc']['metadata']
        path = GoogleDrivePath(
            '/sharing/viewable_gdoc',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_query = provider._build_query(path.identifier)
        metadata_url = provider.build_url('files', path.identifier)
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_body)

        result = await provider.metadata(path)

        local_metadata = copy.deepcopy(metadata_body)
        local_metadata['version'] = local_metadata['etag'] + ds.DRIVE_IGNORE_VERSION
        expected = GoogleDriveFileMetadata(local_metadata, path)
        assert result == expected
        assert aiohttpretty.has_call(method='GET', uri=metadata_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_viewable_gdoc_bad_revision(self, provider):
        metadata_body = fixtures.sharing['viewable_gdoc']['metadata']
        path = GoogleDrivePath(
            '/sharing/viewable_gdoc',
            _ids=['1', '2', metadata_body['id']]
        )

        unauthorized_error = fixtures.make_unauthorized_file_access_error(metadata_body['id'])
        revision_url = provider.build_url('files', metadata_body['id'],
                                          'revisions', self.GDOC_BAD_REVISION)
        aiohttpretty.register_json_uri('GET', revision_url, status=404, body=unauthorized_error)

        with pytest.raises(exceptions.NotFoundError) as e:
            await provider.metadata(path, revision=self.GDOC_BAD_REVISION)

        assert e.value.code == 404

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_viewable_gdoc_magic_revision(self, provider):
        metadata_body = fixtures.sharing['viewable_gdoc']['metadata']
        path = GoogleDrivePath(
            '/sharing/viewable_gdoc',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_query = provider._build_query(path.identifier)
        metadata_url = provider.build_url('files', path.identifier)
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_body)

        result = await provider.metadata(path, revision=self.MAGIC_REVISION)

        local_metadata = copy.deepcopy(metadata_body)
        local_metadata['version'] = local_metadata['etag'] + ds.DRIVE_IGNORE_VERSION
        expected = GoogleDriveFileMetadata(local_metadata, path)
        assert result == expected
        assert aiohttpretty.has_call(method='GET', uri=metadata_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_editable_jpeg_no_revision(self, provider):
        metadata_body = fixtures.sharing['editable_jpeg']['metadata']
        path = GoogleDrivePath(
            '/sharing/editable_jpeg.jpeg',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_query = provider._build_query(path.identifier)
        metadata_url = provider.build_url('files', path.identifier)
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_body)

        result = await provider.metadata(path)

        expected = GoogleDriveFileMetadata(metadata_body, path)
        assert result == expected
        assert aiohttpretty.has_call(method='GET', uri=metadata_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_editable_jpeg_good_revision(self, provider):
        metadata_body = fixtures.sharing['editable_jpeg']['metadata']
        path = GoogleDrivePath(
            '/sharing/editable_jpeg.jpeg',
            _ids=['1', '2', metadata_body['id']]
        )

        revision_body = fixtures.sharing['editable_jpeg']['revision']
        revision_url = provider.build_url('files', metadata_body['id'],
                                          'revisions', self.JPEG_GOOD_REVISION)
        aiohttpretty.register_json_uri('GET', revision_url, body=revision_body)

        result = await provider.metadata(path, revision=self.JPEG_GOOD_REVISION)

        expected = GoogleDriveFileRevisionMetadata(revision_body, path)
        assert result == expected
        assert aiohttpretty.has_call(method='GET', uri=revision_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_editable_jpeg_bad_revision(self, provider):
        metadata_body = fixtures.sharing['editable_jpeg']['metadata']
        path = GoogleDrivePath(
            '/sharing/editable_jpeg.jpeg',
            _ids=['1', '2', metadata_body['id']]
        )

        no_such_revision_error = fixtures.make_no_such_revision_error(self.JPEG_BAD_REVISION)
        revision_url = provider.build_url('files', metadata_body['id'],
                                          'revisions', self.JPEG_BAD_REVISION)
        aiohttpretty.register_json_uri('GET', revision_url, status=404, body=no_such_revision_error)

        with pytest.raises(exceptions.NotFoundError) as e:
            await provider.metadata(path, revision=self.JPEG_BAD_REVISION)

        assert e.value.code == 404

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_editable_jpeg_magic_revision(self, provider):
        metadata_body = fixtures.sharing['editable_jpeg']['metadata']
        path = GoogleDrivePath(
            '/sharing/editable_jpeg.jpeg',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_query = provider._build_query(path.identifier)
        metadata_url = provider.build_url('files', path.identifier)
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_body)

        result = await provider.metadata(path, revision=self.MAGIC_REVISION)

        expected = GoogleDriveFileMetadata(metadata_body, path)
        assert result == expected
        assert aiohttpretty.has_call(method='GET', uri=metadata_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_viewable_jpeg_no_revision(self, provider):
        metadata_body = fixtures.sharing['viewable_jpeg']['metadata']
        path = GoogleDrivePath(
            '/sharing/viewaable_jpeg.jpeg',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_query = provider._build_query(path.identifier)
        metadata_url = provider.build_url('files', path.identifier)
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_body)

        result = await provider.metadata(path)

        expected = GoogleDriveFileMetadata(metadata_body, path)
        assert result == expected
        assert aiohttpretty.has_call(method='GET', uri=metadata_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_viewable_jpeg_bad_revision(self, provider):
        metadata_body = fixtures.sharing['viewable_jpeg']['metadata']
        path = GoogleDrivePath(
            '/sharing/viewable_jpeg.jpeg',
            _ids=['1', '2', metadata_body['id']]
        )

        unauthorized_error = fixtures.make_unauthorized_file_access_error(metadata_body['id'])
        revision_url = provider.build_url('files', metadata_body['id'],
                                          'revisions', self.JPEG_BAD_REVISION)
        aiohttpretty.register_json_uri('GET', revision_url, status=404, body=unauthorized_error)

        with pytest.raises(exceptions.NotFoundError) as e:
            await provider.metadata(path, revision=self.JPEG_BAD_REVISION)

        assert e.value.code == 404

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_viewable_jpeg_magic_revision(self, provider):
        metadata_body = fixtures.sharing['viewable_jpeg']['metadata']
        path = GoogleDrivePath(
            '/sharing/viewable_jpeg.jpeg',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_query = provider._build_query(path.identifier)
        metadata_url = provider.build_url('files', path.identifier)
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_body)

        result = await provider.metadata(path, revision=self.MAGIC_REVISION)

        expected = GoogleDriveFileMetadata(metadata_body, path)
        assert result == expected
        assert aiohttpretty.has_call(method='GET', uri=metadata_url)


class TestRevisions:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_get_revisions(self, provider):
        item = fixtures.list_file['items'][0]
        path = WaterButlerPath('/birdie.jpg', _ids=('doesntmatter', item['id']))

        revisions_url = provider.build_url('files', item['id'], 'revisions')
        aiohttpretty.register_json_uri('GET', revisions_url, body=fixtures.revisions_list)

        result = await provider.revisions(path)
        expected = [
            GoogleDriveRevision(each)
            for each in fixtures.revisions_list['items']
        ]
        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_get_revisions_no_revisions(self, provider):
        item = fixtures.list_file['items'][0]
        metadata_url = provider.build_url('files', item['id'])
        revisions_url = provider.build_url('files', item['id'], 'revisions')
        path = WaterButlerPath('/birdie.jpg', _ids=('doesntmatter', item['id']))

        aiohttpretty.register_json_uri('GET', metadata_url, body=item)
        aiohttpretty.register_json_uri('GET', revisions_url, body=fixtures.revisions_list_empty)

        result = await provider.revisions(path)
        expected = [
            GoogleDriveRevision({
                'modifiedDate': item['modifiedDate'],
                'id': item['etag'] + ds.DRIVE_IGNORE_VERSION,
            })
        ]
        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_get_revisions_for_uneditable(self, provider):
        file_fixtures = fixtures.sharing['viewable_gdoc']
        item = file_fixtures['metadata']
        metadata_url = provider.build_url('files', item['id'])
        revisions_url = provider.build_url('files', item['id'], 'revisions')
        path = WaterButlerPath('/birdie.jpg', _ids=('doesntmatter', item['id']))

        aiohttpretty.register_json_uri('GET', metadata_url, body=item)
        aiohttpretty.register_json_uri(
            'GET', revisions_url, body=file_fixtures['revisions_error'], status=403)

        result = await provider.revisions(path)
        expected = [
            GoogleDriveRevision({
                'modifiedDate': item['modifiedDate'],
                'id': item['etag'] + ds.DRIVE_IGNORE_VERSION,
            })
        ]
        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_get_revisions_doesnt_exist(self, provider):
        with pytest.raises(exceptions.NotFoundError):
            await provider.revisions(WaterButlerPath('/birdie.jpg'))


class TestCreateFolder:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_already_exists(self, provider):
        path = WaterButlerPath('/hugo/', _ids=('doesnt', 'matter'))

        with pytest.raises(exceptions.FolderNamingConflict) as e:
            await provider.create_folder(path)

        assert e.value.code == 409
        assert e.value.message == 'Cannot create folder "{}" because a file or folder already exists at path "{}"'.format(path.name, str(path))

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_returns_metadata(self, provider):
        path = WaterButlerPath('/osf%20test/', _ids=(provider.folder['id'], None))

        aiohttpretty.register_json_uri('POST', provider.build_url('files'), body=fixtures.folder_metadata)

        resp = await provider.create_folder(path)

        assert resp.kind == 'folder'
        assert resp.name == 'osf test'
        assert resp.path == '/osf%20test/'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_raises_non_404(self, provider):
        path = WaterButlerPath('/hugo/kim/pins/', _ids=(provider.folder['id'], 'something', 'something', None))

        url = provider.build_url('files')
        aiohttpretty.register_json_uri('POST', url, status=418)

        with pytest.raises(exceptions.CreateFolderError) as e:
            await provider.create_folder(path)

        assert e.value.code == 418

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_must_be_folder(self, provider, monkeypatch):
        with pytest.raises(exceptions.CreateFolderError) as e:
            await provider.create_folder(WaterButlerPath('/carp.fish', _ids=('doesnt', 'matter')))
