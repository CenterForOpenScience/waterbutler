import io
import os
import copy
import json
from http import client
from urllib import parse

import furl
import pytest
import aiohttpretty

from waterbutler.core import streams
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.googledrive import settings as ds
from waterbutler.providers.googledrive import GoogleDriveProvider
from waterbutler.providers.googledrive import utils as drive_utils
from waterbutler.providers.googledrive.provider import GoogleDrivePath
from waterbutler.providers.googledrive.metadata import (GoogleDriveRevision,
                                                        GoogleDriveFileMetadata,
                                                        GoogleDriveFolderMetadata,
                                                        GoogleDriveFileRevisionMetadata)

from tests.providers.googledrive.fixtures import(error_fixtures,
                                                 sharing_fixtures,
                                                 revision_fixtures,
                                                 root_provider_fixtures)


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
def other_credentials():
    return {'token': 'hugoandprobablynotkim'}


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
def other_provider(auth, other_credentials, settings):
    return GoogleDriveProvider(auth, other_credentials, settings)


@pytest.fixture
def search_for_file_response():
    return {
        'files': [
            {'id': '1234ideclarethumbwar'}
        ]
    }


@pytest.fixture
def no_file_response():
    return {
        'files': []
    }


@pytest.fixture
def actual_file_response():
    return {
        'id': '1234ideclarethumbwar',
        'mimeType': 'text/plain',
        'name': 'B.txt',
    }


@pytest.fixture
def search_for_folder_response():
    return {
        'files': [
            {'id': 'whyis6afraidof7'}
        ]
    }


@pytest.fixture
def no_folder_response():
    return {
        'files': []
    }


@pytest.fixture
def actual_folder_response():
    return {
        'id': 'whyis6afraidof7',
        'mimeType': 'application/vnd.google-apps.folder',
        'name': 'A',
    }


def make_unauthorized_file_access_error(file_id):
    message = ('The authenticated user does not have the required access '
               'to the file {}'.format(file_id))
    return json.dumps({
        "error": {
            "errors": [
                {
                    "reason": "userAccess",
                    "locationType": "header",
                    "message": message,
                    "location": "Authorization",
                    "domain": "global"
                }
            ],
            "message": message,
            "code": 403
        }
    })


def make_no_such_revision_error(revision_id):
    message = 'Revision not found: {}'.format(revision_id)
    return json.dumps({
        "error": {
            "errors": [
                {
                    "reason": "notFound",
                    "locationType": "other",
                    "message": message,
                    "locationType": "parameter",
                    "location": "revisionId",
                    "domain": "global"
                }
            ],
            "message": message,
            "code": 404
        }
    })


def clean_query(query: str):
    # Replace \ with \\ and ' with \'
    # Note only single quotes need to be escaped
    return query.replace('\\', r'\\').replace("'", r"\'")


def _build_name_search_query(provider, entity_name, base_id, is_folder=True):
    return "name = '{}' " \
        "and trashed = false " \
        "and mimeType != 'application/vnd.google-apps.form' " \
        "and mimeType != 'application/vnd.google-apps.map' " \
        "and mimeType != 'application/vnd.google-apps.document' " \
        "and mimeType != 'application/vnd.google-apps.drawing' " \
        "and mimeType != 'application/vnd.google-apps.presentation' " \
        "and mimeType != 'application/vnd.google-apps.spreadsheet' " \
        "and '{}' in parents " \
        "and mimeType {} '{}'".format(
            entity_name,
            base_id,
            '=' if is_folder else '!=',
            provider.FOLDER_MIME_TYPE
        )


def generate_list(child_id, **kwargs):
    file = {}
    file.update(root_provider_fixtures()['list_file']['files'][0])
    file.update(kwargs)
    file['id'] = str(child_id)
    return {'files': [file]}


class TestValidatePath:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_file(self, provider, search_for_file_response,
                                         actual_file_response, no_folder_response):
        file_name = 'file.txt'
        file_id = '1234ideclarethumbwar'

        query_url = provider.build_url(
            'files',
            q=_build_name_search_query(provider, file_name, provider.folder['id'], False),
            fields='files(id)'
        )
        wrong_query_url = provider.build_url(
            'files',
            q=_build_name_search_query(provider, file_name, provider.folder['id'], True),
            fields='files(id)'
        )
        specific_url = provider.build_url('files', file_id, fields='id, name, mimeType')

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
            'files',
            q=_build_name_search_query(provider, folder_name, provider.folder['id'], True),
            fields='files(id)'
        )
        wrong_query_url = provider.build_url(
            'files',
            q=_build_name_search_query(provider, folder_name, provider.folder['id'], False),
            fields='files(id)'
        )
        specific_url = provider.build_url('files', folder_id, fields='id, name, mimeType')

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

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_root(self, provider):
        path = '/'

        result = await provider.validate_v1_path(path)
        expected = GoogleDrivePath('/', _ids=[provider.folder['id']], folder=True)

        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_revalidate_path_file(self, provider, root_provider_fixtures):
        file_name = '/Gear1.stl'
        revalidate_path_metadata = root_provider_fixtures['revalidate_path_file_metadata_1']
        file_id = revalidate_path_metadata['files'][0]['id']
        path = GoogleDrivePath(file_name, _ids=['0', file_id])

        parts = [[parse.unquote(x), True] for x in file_name.strip('/').split('/')]
        parts[-1][1] = False

        current_part = parts.pop(0)
        part_name, part_is_folder = current_part[0], current_part[1]
        name, ext = os.path.splitext(part_name)
        query = _build_name_search_query(provider, file_name.strip('/'), file_id, False)

        url = provider.build_url('files', q=query, fields='files(id)')
        aiohttpretty.register_json_uri('GET', url, body=revalidate_path_metadata)

        url = provider.build_url('files', file_id, fields='id, name, mimeType')
        aiohttpretty.register_json_uri('GET', url,
                                       body=root_provider_fixtures['revalidate_path_file_metadata_2'])

        result = await provider.revalidate_path(path, file_name)

        assert result.name in path.name

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_revalidate_path_file_gdoc(self, provider, root_provider_fixtures):
        file_name = '/Gear1.gdoc'
        file_id = root_provider_fixtures['revalidate_path_file_metadata_1']['files'][0]['id']
        path = GoogleDrivePath(file_name, _ids=['0', file_id])

        parts = [[parse.unquote(x), True] for x in file_name.strip('/').split('/')]
        parts[-1][1] = False

        current_part = parts.pop(0)
        part_name, part_is_folder = current_part[0], current_part[1]
        name, ext = os.path.splitext(part_name)
        gd_ext = drive_utils.get_mimetype_from_ext(ext)
        query = "name = '{}' " \
                "and trashed = false " \
                "and '{}' in parents " \
                "and mimeType = '{}'".format(clean_query(name), file_id, gd_ext)

        url = provider.build_url('files', q=query, fields='files(id)')
        aiohttpretty.register_json_uri('GET', url,
                                       body=root_provider_fixtures['revalidate_path_file_metadata_1'])

        url = provider.build_url('files', file_id, fields='id, name, mimeType')
        aiohttpretty.register_json_uri('GET', url,
                                       body=root_provider_fixtures['revalidate_path_gdoc_file_metadata'])

        result = await provider.revalidate_path(path, file_name)

        assert result.name in path.name

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_revalidate_path_folder(self, provider, root_provider_fixtures):
        file_name = "/inception folder yo/"
        file_id = root_provider_fixtures['revalidate_path_folder_metadata_1']['files'][0]['id']
        path = GoogleDrivePath(file_name, _ids=['0', file_id])

        parts = [[parse.unquote(x), True] for x in file_name.strip('/').split('/')]
        parts[-1][1] = False

        current_part = parts.pop(0)
        part_name, part_is_folder = current_part[0], current_part[1]
        name, ext = os.path.splitext(part_name)
        query = _build_name_search_query(provider, file_name.strip('/') + '/', file_id, True)

        folder_one_url = provider.build_url('files', q=query, fields='files(id)')
        aiohttpretty.register_json_uri('GET', folder_one_url,
                                       body=root_provider_fixtures['revalidate_path_folder_metadata_1'])

        folder_two_url = provider.build_url('files', file_id, fields='id, name, mimeType')
        aiohttpretty.register_json_uri('GET', folder_two_url,
                                       body=root_provider_fixtures['revalidate_path_folder_metadata_2'])

        result = await provider.revalidate_path(path, file_name, True)
        assert result.name in path.name


class TestUpload:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_create(self, provider, file_stream, root_provider_fixtures):
        upload_id = '7'
        file = root_provider_fixtures['list_file']['files'][0]
        path = WaterButlerPath('/birdie.jpg', _ids=(provider.folder['id'], None))

        start_upload_furl = furl.furl(provider._build_upload_url('files', '',
                                                        uploadType='resumable'))
        start_upload_url = start_upload_furl.add(provider.FILE_FIELDS).url
        finish_upload_url = provider._build_upload_url('files', uploadType='resumable',
                                                       upload_id=upload_id)

        aiohttpretty.register_uri('POST', start_upload_url,
                                  headers={'LOCATION': 'http://waterbutler.io?upload_id={}'.format(upload_id)})
        aiohttpretty.register_json_uri('PUT', finish_upload_url, body=file)

        result, created = await provider.upload(file_stream, path)

        expected = GoogleDriveFileMetadata(file, path)

        assert created is True
        assert result == expected
        assert aiohttpretty.has_call(method='POST', uri=start_upload_url)
        assert aiohttpretty.has_call(method='PUT', uri=finish_upload_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_doesnt_unquote(self, provider, file_stream, root_provider_fixtures):
        upload_id = '7'
        file = root_provider_fixtures['list_file']['files'][0]
        path = GoogleDrivePath('/birdie%2F %20".jpg', _ids=(provider.folder['id'], None))

        start_upload_furl = furl.furl(provider._build_upload_url('files', '',
                                                        uploadType='resumable'))
        start_upload_url = start_upload_furl.add(provider.FILE_FIELDS).url
        finish_upload_url = provider._build_upload_url('files', uploadType='resumable',
                                                       upload_id=upload_id)

        aiohttpretty.register_uri('POST', start_upload_url,
                                  headers={'LOCATION': 'http://waterbutler.io?upload_id={}'.format(upload_id)})
        aiohttpretty.register_json_uri('PUT', finish_upload_url, body=file)

        result, created = await provider.upload(file_stream, path)

        expected = GoogleDriveFileMetadata(file, path)

        assert created is True
        assert result == expected
        assert aiohttpretty.has_call(method='POST', uri=start_upload_url)
        assert aiohttpretty.has_call(method='PUT', uri=finish_upload_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_update(self, provider, file_stream, root_provider_fixtures):
        upload_id = '7'
        file = root_provider_fixtures['list_file']['files'][0]
        path = WaterButlerPath('/birdie.jpg', _ids=(provider.folder['id'], file['id']))

        start_upload_furl = furl.furl(provider._build_upload_url('files', file['id'],
                                                        uploadType='resumable'))
        start_upload_url = start_upload_furl.add(provider.FILE_FIELDS).url
        finish_upload_url = provider._build_upload_url('files', path.identifier,
                                                       uploadType='resumable', upload_id=upload_id)

        aiohttpretty.register_uri('PATCH', start_upload_url,
                                  headers={'LOCATION': 'http://waterbutler.io?upload_id={}'.format(upload_id)})
        aiohttpretty.register_json_uri('PUT', finish_upload_url, body=file)
        result, created = await provider.upload(file_stream, path)

        assert aiohttpretty.has_call(method='PATCH', uri=start_upload_url)
        assert aiohttpretty.has_call(method='PUT', uri=finish_upload_url)
        assert created is False
        expected = GoogleDriveFileMetadata(file, path)
        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_create_nested(self, provider, file_stream, root_provider_fixtures):
        upload_id = '7'
        file = root_provider_fixtures['list_file']['files'][0]
        path = WaterButlerPath(
            '/ed/sullivan/show.mp3',
            _ids=[str(x) for x in range(3)]
        )

        start_upload_furl = furl.furl(provider._build_upload_url('files', '',
                                                        uploadType='resumable'))
        start_upload_url = start_upload_furl.add(provider.FILE_FIELDS).url
        finish_upload_url = provider._build_upload_url('files', uploadType='resumable',
                                                       upload_id=upload_id)
        aiohttpretty.register_uri('POST', start_upload_url,
                                  headers={'LOCATION': 'http://waterbutler.io?upload_id={}'.format(upload_id)})
        aiohttpretty.register_json_uri('PUT', finish_upload_url, body=file)
        result, created = await provider.upload(file_stream, path)

        assert aiohttpretty.has_call(method='POST', uri=start_upload_url)
        assert aiohttpretty.has_call(method='PUT', uri=finish_upload_url)
        assert created is True
        expected = GoogleDriveFileMetadata(file, path)
        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_checksum_mismatch(self, provider, file_stream, root_provider_fixtures):
        upload_id = '7'
        path = WaterButlerPath('/birdie.jpg', _ids=(provider.folder['id'], None))

        start_upload_furl = furl.furl(provider._build_upload_url('files', '',
                                                        uploadType='resumable'))
        start_upload_url = start_upload_furl.add(provider.FILE_FIELDS).url
        finish_upload_url = provider._build_upload_url('files', uploadType='resumable',
                                                       upload_id=upload_id)

        aiohttpretty.register_json_uri('PUT', finish_upload_url,
                                       body=root_provider_fixtures['checksum_mismatch_metadata'])
        aiohttpretty.register_uri('POST', start_upload_url,
                                  headers={'LOCATION': 'http://waterbutler.io?upload_id={}'.format(upload_id)})

        with pytest.raises(exceptions.UploadChecksumMismatchError) as exc:
            await provider.upload(file_stream, path)

        assert aiohttpretty.has_call(method='PUT', uri=finish_upload_url)
        assert aiohttpretty.has_call(method='POST', uri=start_upload_url)


class TestDelete:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete(self, provider, root_provider_fixtures):
        file = root_provider_fixtures['list_file']['files'][0]
        path = WaterButlerPath('/birdie.jpg', _ids=(None, file['id']))
        delete_url = provider.build_url('files', file['id'])
        del_url_body = json.dumps({'labels': {'trashed': 'true'}})
        aiohttpretty.register_uri('PATCH',
                                  delete_url,
                                  body=del_url_body,
                                  status=200)

        result = await provider.delete(path)

        assert result is None
        assert aiohttpretty.has_call(method='PATCH', uri=delete_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_folder(self, provider, root_provider_fixtures):
        item = root_provider_fixtures['folder_metadata']
        del_url = provider.build_url('files', item['id'])
        del_url_body = json.dumps({'labels': {'trashed': 'true'}})

        path = WaterButlerPath('/foobar/', _ids=('doesntmatter', item['id']))

        aiohttpretty.register_uri('PATCH',
                                  del_url,
                                  body=del_url_body,
                                  status=200)

        result = await provider.delete(path)

        assert aiohttpretty.has_call(method='PATCH', uri=del_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_not_existing(self, provider):
        with pytest.raises(exceptions.NotFoundError):
            await provider.delete(WaterButlerPath('/foobar/'))

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_root_no_confirm(self, provider):
        path = WaterButlerPath('/', _ids=('0'))

        with pytest.raises(exceptions.DeleteError) as e:
            await provider.delete(path)

        assert e.value.message == 'confirm_delete=1 is required for deleting root provider folder'
        assert e.value.code == 400

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_root(self, provider, root_provider_fixtures):
        file = root_provider_fixtures['delete_contents_metadata']['files'][0]
        root_path = WaterButlerPath('/', _ids=('0'))

        url = provider.build_url('files', q="'{}' in parents and trashed = false".format('0'),
                                 fields='files(id)')
        aiohttpretty.register_json_uri('GET', url,
                                       body=root_provider_fixtures['delete_contents_metadata'])

        delete_url = provider.build_url('files', file['id'])
        data = json.dumps({'labels': {'trashed': 'true'}}),
        aiohttpretty.register_json_uri('PATCH', delete_url, data=data, status=200)

        await provider.delete(root_path, 1)

        assert aiohttpretty.has_call(method='PATCH', uri=delete_url)


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
    GDOC_BAD_REVISION = '0BxdmnuT5XpqcdFdzM2dtTnB4bnNuUXpmYlZkOUZDV1AvcWFRPQ'
    JPEG_GOOD_REVISION = GDOC_BAD_REVISION
    JPEG_BAD_REVISION = GDOC_GOOD_REVISION
    MAGIC_REVISION = '"LUxk1DXE_0fd4yeJDIgpecr5uPA/MTQ5NTExOTgxMzgzOQ"{}'.format(
        ds.DRIVE_IGNORE_VERSION)

    GDOC_EXPORT_MIME_TYPE = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_editable_gdoc_no_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['editable_gdoc']['metadata']
        path = GoogleDrivePath(
            '/sharing/editable_gdoc',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_furl = furl.furl(provider.build_url('files', path.identifier or ''))
        metadata_url = metadata_furl.add(provider.FILE_FIELDS).url
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_body)

        revisions_body = sharing_fixtures['editable_gdoc']['revisions']
        revisions_url = provider.build_url('files', metadata_body['id'], 'revisions')
        aiohttpretty.register_json_uri('GET', revisions_url, body=revisions_body)

        file_content = b'we love you conrad'
        download_file_url = provider.build_url('files', metadata_body['id'], 'export',
                mimeType=self.GDOC_EXPORT_MIME_TYPE)
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
    async def test_download_editable_gdoc_good_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['editable_gdoc']['metadata']
        path = GoogleDrivePath(
            '/sharing/editable_gdoc',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_furl = furl.furl(provider.build_url('files', path.identifier or ''))
        metadata_url = metadata_furl.add(provider.FILE_FIELDS).url
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_body)

        revisions_body = sharing_fixtures['editable_gdoc']['revisions']
        revisions_url = provider.build_url('files', metadata_body['id'], 'revisions')
        aiohttpretty.register_json_uri('GET', revisions_url, body=revisions_body)

        revision_body = sharing_fixtures['editable_gdoc']['v2_revision']
        revision_url = provider.build_url('files', metadata_body['id'],
                                          'revisions', self.GDOC_GOOD_REVISION)
        revision_url = revision_url.replace('/v3/', '/v2/', 1)
        aiohttpretty.register_json_uri('GET', revision_url, body=revision_body)

        file_content = b'we love you conrad'
        download_file_url = revision_body['exportLinks'][self.GDOC_EXPORT_MIME_TYPE]
        aiohttpretty.register_uri('GET', download_file_url, body=file_content, auto_length=True)

        result = await provider.download(path, revision=self.GDOC_GOOD_REVISION)
        assert result.name == 'editable_gdoc.docx'

        content = await result.read()
        assert content == file_content
        assert aiohttpretty.has_call(method='GET', uri=metadata_url)
        assert aiohttpretty.has_call(method='GET', uri=revisions_url)
        assert aiohttpretty.has_call(method='GET', uri=revision_url)
        assert aiohttpretty.has_call(method='GET', uri=download_file_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_editable_gdoc_bad_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['editable_gdoc']['metadata']
        path = GoogleDrivePath(
            '/sharing/editable_gdoc',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_furl = furl.furl(provider.build_url('files', path.identifier or ''))
        metadata_url = metadata_furl.add(provider.FILE_FIELDS).url
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_body)

        revisions_body = sharing_fixtures['editable_gdoc']['revisions']
        revisions_url = provider.build_url('files', metadata_body['id'], 'revisions')
        aiohttpretty.register_json_uri('GET', revisions_url, body=revisions_body)

        no_such_revision_error = make_no_such_revision_error(self.GDOC_BAD_REVISION)
        revision_url = provider.build_url('files', metadata_body['id'],
                                          'revisions', self.GDOC_BAD_REVISION)
        revision_url = revision_url.replace('/v3/', '/v2/', 1)
        aiohttpretty.register_json_uri('GET', revision_url, status=404, body=no_such_revision_error)

        with pytest.raises(exceptions.NotFoundError) as e:
            await provider.download(path, revision=self.GDOC_BAD_REVISION)

        assert e.value.code == 404
        assert aiohttpretty.has_call(method='GET', uri=metadata_url)
        assert aiohttpretty.has_call(method='GET', uri=revisions_url)
        assert aiohttpretty.has_call(method='GET', uri=revision_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_editable_gdoc_magic_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['editable_gdoc']['metadata']
        path = GoogleDrivePath(
            '/sharing/editable_gdoc',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_furl = furl.furl(provider.build_url('files', path.identifier or ''))
        metadata_url = metadata_furl.add(provider.FILE_FIELDS).url
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_body)

        revisions_body = sharing_fixtures['editable_gdoc']['revisions']
        revisions_url = provider.build_url('files', metadata_body['id'], 'revisions')
        aiohttpretty.register_json_uri('GET', revisions_url, body=revisions_body)

        file_content = b'we love you conrad'
        mime_type = self.GDOC_EXPORT_MIME_TYPE
        download_url = provider.build_url('files', path.identifier, 'export',
                                          mimeType=mime_type)
        aiohttpretty.register_uri('GET', download_url, body=file_content, auto_length=True)

        result = await provider.download(path, revision=self.MAGIC_REVISION)
        assert result.name == 'editable_gdoc.docx'

        content = await result.read()
        assert content == file_content
        assert aiohttpretty.has_call(method='GET', uri=metadata_url)
        assert aiohttpretty.has_call(method='GET', uri=revisions_url)
        assert aiohttpretty.has_call(method='GET', uri=download_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_viewable_gdoc_no_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['viewable_gdoc']['metadata']
        path = GoogleDrivePath(
            '/sharing/viewable_gdoc',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_furl = furl.furl(provider.build_url('files', path.identifier or ''))
        metadata_url = metadata_furl.add(provider.FILE_FIELDS).url
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_body)

        file_content = b'we love you conrad'
        download_file_url = provider.build_url('files', metadata_body.get('id'), 'export',
                                               mimeType=self.GDOC_EXPORT_MIME_TYPE)
        aiohttpretty.register_uri('GET', download_file_url, body=file_content, auto_length=True)

        result = await provider.download(path)
        assert result.name == 'viewable_gdoc.docx'

        content = await result.read()
        assert content == file_content
        assert aiohttpretty.has_call(method='GET', uri=metadata_url)
        assert aiohttpretty.has_call(method='GET', uri=download_file_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_viewable_gdoc_bad_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['viewable_gdoc']['metadata']
        path = GoogleDrivePath(
            '/sharing/viewable_gdoc',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_furl = furl.furl(provider.build_url('files', path.identifier or ''))
        metadata_url = metadata_furl.add(provider.FILE_FIELDS).url
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_body)

        revisions_body = sharing_fixtures['editable_gdoc']['revisions']
        revisions_url = provider.build_url('files', metadata_body['id'], 'revisions')
        aiohttpretty.register_json_uri('GET', revisions_url, body=revisions_body)

        unauthorized_error = make_unauthorized_file_access_error(metadata_body['id'])
        revision_url = provider.build_url('files', metadata_body['id'],
                                          'revisions', self.GDOC_BAD_REVISION)
        revision_url = revision_url.replace('/v3/', '/v2/', 1)
        aiohttpretty.register_json_uri('GET', revision_url, status=404, body=unauthorized_error)

        with pytest.raises(exceptions.NotFoundError) as e:
            await provider.download(path, revision=self.GDOC_BAD_REVISION)

        assert e.value.code == 404

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_viewable_gdoc_magic_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['viewable_gdoc']['metadata']
        path = GoogleDrivePath(
            '/sharing/viewable_gdoc',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_furl = furl.furl(provider.build_url('files', path.identifier or ''))
        metadata_url = metadata_furl.add(provider.FILE_FIELDS).url
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_body)

        file_content = b'we love you conrad'
        mime_type = self.GDOC_EXPORT_MIME_TYPE
        download_url = provider.build_url('files', metadata_body.get('id'), 'export',
                                          mimeType=mime_type)
        aiohttpretty.register_uri('GET', download_url, body=file_content, auto_length=True)

        result = await provider.download(path, revision=self.MAGIC_REVISION)
        assert result.name == 'viewable_gdoc.docx'

        content = await result.read()
        assert content == file_content
        assert aiohttpretty.has_call(method='GET', uri=metadata_url)
        assert aiohttpretty.has_call(method='GET', uri=download_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_editable_jpeg_no_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['editable_jpeg']['metadata']
        path = GoogleDrivePath(
            '/sharing/editable_jpeg.jpeg',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_furl = furl.furl(provider.build_url('files', path.identifier or ''))
        metadata_url = metadata_furl.add(provider.FILE_FIELDS).url
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_body)

        file_content = b'we love you conrad'
        download_file_url = metadata_body['webContentLink']
        aiohttpretty.register_uri('GET', download_file_url, body=file_content, auto_length=True)

        result = await provider.download(path)

        content = await result.read()
        assert content == file_content
        assert aiohttpretty.has_call(method='GET', uri=metadata_url)
        assert aiohttpretty.has_call(method='GET', uri=download_file_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_editable_jpeg_good_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['editable_jpeg']['metadata']
        path = GoogleDrivePath(
            '/sharing/editable_jpeg.jpeg',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_furl = furl.furl(provider.build_url('files', path.identifier or ''))
        metadata_url = metadata_furl.add(provider.FILE_FIELDS).url
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_body)

        file_content = b'we love you conrad'
        download_url = provider.build_url('files', metadata_body.get('id'),
                                          'revisions', self.JPEG_GOOD_REVISION, alt='media')
        aiohttpretty.register_uri('GET', download_url, body=file_content, auto_length=True)

        result = await provider.download(path, revision=self.JPEG_GOOD_REVISION)

        content = await result.read()
        assert content == file_content
        assert aiohttpretty.has_call(method='GET', uri=metadata_url)
        assert aiohttpretty.has_call(method='GET', uri=download_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_editable_jpeg_bad_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['editable_jpeg']['metadata']
        path = GoogleDrivePath(
            '/sharing/editable_jpeg.jpeg',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_furl = furl.furl(provider.build_url('files', path.identifier or ''))
        metadata_url = metadata_furl.add(provider.FILE_FIELDS).url
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_body)

        no_such_revision_error = make_no_such_revision_error(self.JPEG_BAD_REVISION)
        download_url = provider.build_url('files', metadata_body.get('id'),
                                          'revisions', self.JPEG_BAD_REVISION, alt='media')
        aiohttpretty.register_uri('GET', download_url, status=404, body=no_such_revision_error,
                                  auto_length=True)

        with pytest.raises(exceptions.DownloadError) as e:
            await provider.download(path, revision=self.JPEG_BAD_REVISION)

        assert e.value.code == 404
        assert aiohttpretty.has_call(method='GET', uri=metadata_url)
        assert aiohttpretty.has_call(method='GET', uri=download_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_editable_jpeg_magic_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['editable_jpeg']['metadata']
        path = GoogleDrivePath(
            '/sharing/editable_jpeg.jpeg',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_furl = furl.furl(provider.build_url('files', path.identifier or ''))
        metadata_url = metadata_furl.add(provider.FILE_FIELDS).url
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_body)

        file_content = b'we love you conrad'
        download_file_url = metadata_body['webContentLink']
        aiohttpretty.register_uri('GET', download_file_url, body=file_content, auto_length=True)

        result = await provider.download(path, revision=self.MAGIC_REVISION)

        content = await result.read()
        assert content == file_content
        assert aiohttpretty.has_call(method='GET', uri=metadata_url)
        assert aiohttpretty.has_call(method='GET', uri=download_file_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_viewable_jpeg_no_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['viewable_jpeg']['metadata']
        path = GoogleDrivePath(
            '/sharing/viewaable_jpeg.jpeg',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_furl = furl.furl(provider.build_url('files', path.identifier or ''))
        metadata_url = metadata_furl.add(provider.FILE_FIELDS).url
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_body)

        file_content = b'we love you conrad'
        download_file_url = metadata_body['webContentLink']
        aiohttpretty.register_uri('GET', download_file_url, body=file_content, auto_length=True)

        result = await provider.download(path)

        content = await result.read()
        assert content == file_content
        assert aiohttpretty.has_call(method='GET', uri=metadata_url)
        assert aiohttpretty.has_call(method='GET', uri=download_file_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_viewable_jpeg_bad_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['viewable_jpeg']['metadata']
        path = GoogleDrivePath(
            '/sharing/viewable_jpeg.jpeg',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_furl = furl.furl(provider.build_url('files', path.identifier or ''))
        metadata_url = metadata_furl.add(provider.FILE_FIELDS).url
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_body)

        no_such_revision_error = make_no_such_revision_error(metadata_body['id'])
        download_url = provider.build_url('files', metadata_body.get('id'),
                                          'revisions', self.JPEG_BAD_REVISION, alt='media')
        aiohttpretty.register_uri('GET', download_url, status=404, body=no_such_revision_error,
                                  auto_length=True)

        with pytest.raises(exceptions.DownloadError) as e:
            await provider.download(path, revision=self.JPEG_BAD_REVISION)

        assert e.value.code == 404
        assert aiohttpretty.has_call(method='GET', uri=metadata_url)
        assert aiohttpretty.has_call(method='GET', uri=download_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_viewable_jpeg_magic_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['viewable_jpeg']['metadata']
        path = GoogleDrivePath(
            '/sharing/viewable_jpeg.jpeg',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_furl = furl.furl(provider.build_url('files', path.identifier or ''))
        metadata_url = metadata_furl.add(provider.FILE_FIELDS).url
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_body)

        file_content = b'we love you conrad'
        download_file_url = metadata_body['webContentLink']
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
    async def test_metadata_file_root(self, provider, root_provider_fixtures):
        file_metadata = root_provider_fixtures['list_file']['files'][0]
        path = WaterButlerPath('/birdie.jpg', _ids=(provider.folder['id'], file_metadata['id']))

        metadata_furl = furl.furl(provider.build_url('files', path.identifier or ''))
        metadata_url = metadata_furl.add(provider.FILE_FIELDS).url
        aiohttpretty.register_json_uri('GET', metadata_url, body=file_metadata)

        result = await provider.metadata(path)

        expected = GoogleDriveFileMetadata(file_metadata, path)
        assert result == expected
        assert aiohttpretty.has_call(method='GET', uri=metadata_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_string_error_response(self, provider, root_provider_fixtures):
        path = WaterButlerPath('/birdie.jpg',
                               _ids=(provider.folder['id'],
                                     root_provider_fixtures['list_file']['files'][0]['id']))

        metadata_furl = furl.furl(provider.build_url('files', path.identifier or ''))
        metadata_url = metadata_furl.add(provider.FILE_FIELDS).url
        aiohttpretty.register_uri('GET', metadata_url, headers={'Content-Type': 'text/html'},
            body='this is an error message string with a 404... or is it?', status=404)

        with pytest.raises(exceptions.NotFoundError) as e:
            await provider.metadata(path)

        assert e.value.code == 404
        assert e.value.message == 'Could not retrieve file or directory {}'.format('/' + path.path)
        assert aiohttpretty.has_call(method='GET', uri=metadata_url)

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

        file = generate_list(3)['files'][0]

        metadata_furl = furl.furl(provider.build_url('files', path.identifier or ''))
        metadata_url = metadata_furl.add(provider.FILE_FIELDS).url
        aiohttpretty.register_json_uri('GET', metadata_url, body=file)

        result = await provider.metadata(path)

        expected = GoogleDriveFileMetadata(file, path)
        assert result == expected
        assert aiohttpretty.has_call(method='GET', uri=metadata_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_root_folder(self, provider, root_provider_fixtures):
        path = await provider.validate_path('/')
        query = provider._build_query(path.identifier)
        folder_furl = furl.furl(provider.build_url('files', q=query, alt='json', pageSize=1000))
        folder_url = folder_furl.add(provider.FOLDER_FIELDS).url
        aiohttpretty.register_json_uri('GET', folder_url, body=root_provider_fixtures['list_file'])

        result = await provider.metadata(path)

        expected = GoogleDriveFileMetadata(
            root_provider_fixtures['list_file']['files'][0],
            path.child(root_provider_fixtures['list_file']['files'][0]['name'])
        )
        assert result == [expected]
        assert aiohttpretty.has_call(method='GET', uri=folder_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder_nested(self, provider):
        path = GoogleDrivePath(
            '/hugo/kim/pins/',
            _ids=[str(x) for x in range(4)]
        )

        body = generate_list(3)
        file = body['files'][0]

        query = provider._build_query(path.identifier)
        folder_furl = furl.furl(provider.build_url('files', q=query, alt='json', pageSize=1000))
        folder_url = folder_furl.add(provider.FOLDER_FIELDS).url
        aiohttpretty.register_json_uri('GET', folder_url, body=body)

        result = await provider.metadata(path)

        expected = GoogleDriveFileMetadata(file, path.child(file['name']))
        assert result == [expected]
        assert aiohttpretty.has_call(method='GET', uri=folder_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_folder_metadata(self, provider, root_provider_fixtures):
        path = GoogleDrivePath(
            '/hugo/kim/pins/',
            _ids=[str(x) for x in range(4)]
        )

        body = generate_list(3, **root_provider_fixtures['folder_metadata'])
        file = body['files'][0]

        query = provider._build_query(path.identifier)
        folder_furl = furl.furl(provider.build_url('files', q=query, alt='json', pageSize=1000))
        folder_url = folder_furl.add(provider.FOLDER_FIELDS).url
        aiohttpretty.register_json_uri('GET', folder_url, body=body)

        result = await provider.metadata(path)

        expected = GoogleDriveFolderMetadata(file, path.child(file['name'], folder=True))

        assert result == [expected]
        assert aiohttpretty.has_call(method='GET', uri=folder_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_editable_gdoc_no_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['editable_gdoc']['metadata']
        path = GoogleDrivePath(
            '/sharing/editable_gdoc',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_furl = furl.furl(provider.build_url('files', path.identifier or ''))
        metadata_url = metadata_furl.add(provider.FILE_FIELDS).url
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_body)

        revisions_body = sharing_fixtures['editable_gdoc']['revisions']
        revisions_url = provider.build_url('files', metadata_body['id'], 'revisions')
        aiohttpretty.register_json_uri('GET', revisions_url, body=revisions_body)

        result = await provider.metadata(path)

        local_metadata = copy.deepcopy(metadata_body)
        local_metadata['version'] = revisions_body['revisions'][-1]['id']
        expected = GoogleDriveFileMetadata(local_metadata, path)
        assert result == expected
        assert aiohttpretty.has_call(method='GET', uri=metadata_url)
        assert aiohttpretty.has_call(method='GET', uri=revisions_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_editable_gdoc_good_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['editable_gdoc']['metadata']
        path = GoogleDrivePath(
            '/sharing/editable_gdoc',
            _ids=['1', '2', metadata_body['id']]
        )

        revision_body = sharing_fixtures['editable_gdoc']['revision']
        revision_furl = furl.furl(provider.build_url('files', path.identifier,
                                                     'revisions', self.GDOC_GOOD_REVISION))
        revision_url = revision_furl.add(provider.REVISION_FIELDS).url
        aiohttpretty.register_json_uri('GET', revision_url, body=revision_body)

        result = await provider.metadata(path, revision=self.GDOC_GOOD_REVISION)

        expected = GoogleDriveFileRevisionMetadata(revision_body, path)
        assert result == expected
        assert aiohttpretty.has_call(method='GET', uri=revision_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_editable_gdoc_bad_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['editable_gdoc']['metadata']
        path = GoogleDrivePath(
            '/sharing/editable_gdoc',
            _ids=['1', '2', metadata_body['id']]
        )

        no_such_revision_error = make_no_such_revision_error(self.GDOC_BAD_REVISION)
        revision_furl = furl.furl(provider.build_url('files', path.identifier,
                                                     'revisions', self.GDOC_BAD_REVISION))
        revision_url = revision_furl.add(provider.REVISION_FIELDS).url
        aiohttpretty.register_json_uri('GET', revision_url, status=404, body=no_such_revision_error)

        with pytest.raises(exceptions.NotFoundError) as e:
            await provider.metadata(path, revision=self.GDOC_BAD_REVISION)

        assert e.value.code == 404
        assert aiohttpretty.has_call(method='GET', uri=revision_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_editable_gdoc_magic_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['editable_gdoc']['metadata']
        path = GoogleDrivePath(
            '/sharing/editable_gdoc',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_furl = furl.furl(provider.build_url('files', path.identifier or ''))
        metadata_url = metadata_furl.add(provider.FILE_FIELDS).url
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_body)

        revisions_body = sharing_fixtures['editable_gdoc']['revisions']
        revisions_url = provider.build_url('files', metadata_body['id'], 'revisions')
        aiohttpretty.register_json_uri('GET', revisions_url, body=revisions_body)

        result = await provider.metadata(path, revision=self.MAGIC_REVISION)

        local_metadata = copy.deepcopy(metadata_body)
        local_metadata['version'] = revisions_body['revisions'][-1]['id']

        expected = GoogleDriveFileMetadata(local_metadata, path)
        assert result == expected
        assert aiohttpretty.has_call(method='GET', uri=metadata_url)
        assert aiohttpretty.has_call(method='GET', uri=revisions_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_viewable_gdoc_no_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['viewable_gdoc']['metadata']
        path = GoogleDrivePath(
            '/sharing/viewable_gdoc',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_furl = furl.furl(provider.build_url('files', path.identifier or ''))
        metadata_url = metadata_furl.add(provider.FILE_FIELDS).url
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_body)

        result = await provider.metadata(path)

        local_metadata = copy.deepcopy(metadata_body)
        etag = '{}::{}'.format(local_metadata['id'], local_metadata['modifiedTime'])
        local_metadata['version'] = etag + ds.DRIVE_IGNORE_VERSION
        expected = GoogleDriveFileMetadata(local_metadata, path)

        assert result == expected
        assert aiohttpretty.has_call(method='GET', uri=metadata_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_viewable_gdoc_bad_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['viewable_gdoc']['metadata']
        path = GoogleDrivePath(
            '/sharing/viewable_gdoc',
            _ids=['1', '2', metadata_body['id']]
        )

        unauthorized_error = make_unauthorized_file_access_error(metadata_body['id'])
        revision_furl = furl.furl(provider.build_url('files', path.identifier,
                                                     'revisions', self.GDOC_BAD_REVISION))
        revision_url = revision_furl.add(provider.REVISION_FIELDS).url
        aiohttpretty.register_json_uri('GET', revision_url, status=404, body=unauthorized_error)

        with pytest.raises(exceptions.NotFoundError) as e:
            await provider.metadata(path, revision=self.GDOC_BAD_REVISION)

        assert e.value.code == 404

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_viewable_gdoc_magic_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['viewable_gdoc']['metadata']
        path = GoogleDrivePath(
            '/sharing/viewable_gdoc',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_furl = furl.furl(provider.build_url('files', path.identifier or ''))
        metadata_url = metadata_furl.add(provider.FILE_FIELDS).url
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_body)

        result = await provider.metadata(path, revision=self.MAGIC_REVISION)

        local_metadata = copy.deepcopy(metadata_body)
        etag = '{}::{}'.format(local_metadata['id'], local_metadata['modifiedTime'])
        local_metadata['version'] = etag + ds.DRIVE_IGNORE_VERSION
        expected = GoogleDriveFileMetadata(local_metadata, path)
        assert result == expected
        assert aiohttpretty.has_call(method='GET', uri=metadata_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_editable_jpeg_no_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['editable_jpeg']['metadata']
        path = GoogleDrivePath(
            '/sharing/editable_jpeg.jpeg',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_furl = furl.furl(provider.build_url('files', path.identifier or ''))
        metadata_url = metadata_furl.add(provider.FILE_FIELDS).url
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_body)

        result = await provider.metadata(path)

        expected = GoogleDriveFileMetadata(metadata_body, path)
        assert result == expected
        assert aiohttpretty.has_call(method='GET', uri=metadata_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_editable_jpeg_good_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['editable_jpeg']['metadata']
        path = GoogleDrivePath(
            '/sharing/editable_jpeg.jpeg',
            _ids=['1', '2', metadata_body['id']]
        )

        revision_body = sharing_fixtures['editable_jpeg']['revision']
        revision_furl = furl.furl(provider.build_url('files', path.identifier,
                                                     'revisions', self.JPEG_GOOD_REVISION))
        revision_url = revision_furl.add(provider.REVISION_FIELDS).url
        aiohttpretty.register_json_uri('GET', revision_url, body=revision_body)

        result = await provider.metadata(path, revision=self.JPEG_GOOD_REVISION)

        expected = GoogleDriveFileRevisionMetadata(revision_body, path)
        assert result == expected
        assert aiohttpretty.has_call(method='GET', uri=revision_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_editable_jpeg_bad_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['editable_jpeg']['metadata']
        path = GoogleDrivePath(
            '/sharing/editable_jpeg.jpeg',
            _ids=['1', '2', metadata_body['id']]
        )

        no_such_revision_error = make_no_such_revision_error(self.JPEG_BAD_REVISION)
        revision_furl = furl.furl(provider.build_url('files', path.identifier,
                                                     'revisions', self.JPEG_BAD_REVISION))
        revision_url = revision_furl.add(provider.REVISION_FIELDS).url
        aiohttpretty.register_json_uri('GET', revision_url, status=404, body=no_such_revision_error)

        with pytest.raises(exceptions.NotFoundError) as e:
            await provider.metadata(path, revision=self.JPEG_BAD_REVISION)

        assert e.value.code == 404

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_editable_jpeg_magic_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['editable_jpeg']['metadata']
        path = GoogleDrivePath(
            '/sharing/editable_jpeg.jpeg',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_furl = furl.furl(provider.build_url('files', path.identifier or ''))
        metadata_url = metadata_furl.add(provider.FILE_FIELDS).url
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_body)

        result = await provider.metadata(path, revision=self.MAGIC_REVISION)

        expected = GoogleDriveFileMetadata(metadata_body, path)
        assert result == expected
        assert aiohttpretty.has_call(method='GET', uri=metadata_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_viewable_jpeg_no_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['viewable_jpeg']['metadata']
        path = GoogleDrivePath(
            '/sharing/viewaable_jpeg.jpeg',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_furl = furl.furl(provider.build_url('files', path.identifier or ''))
        metadata_url = metadata_furl.add(provider.FILE_FIELDS).url
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_body)

        result = await provider.metadata(path)

        expected = GoogleDriveFileMetadata(metadata_body, path)
        assert result == expected
        assert aiohttpretty.has_call(method='GET', uri=metadata_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_viewable_jpeg_bad_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['viewable_jpeg']['metadata']
        path = GoogleDrivePath(
            '/sharing/viewable_jpeg.jpeg',
            _ids=['1', '2', metadata_body['id']]
        )

        unauthorized_error = make_unauthorized_file_access_error(metadata_body['id'])
        revision_furl = furl.furl(provider.build_url('files', path.identifier,
                                                     'revisions', self.JPEG_BAD_REVISION))
        revision_url = revision_furl.add(provider.REVISION_FIELDS).url
        aiohttpretty.register_json_uri('GET', revision_url, status=404, body=unauthorized_error)

        with pytest.raises(exceptions.NotFoundError) as e:
            await provider.metadata(path, revision=self.JPEG_BAD_REVISION)

        assert e.value.code == 404

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_viewable_jpeg_magic_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['viewable_jpeg']['metadata']
        path = GoogleDrivePath(
            '/sharing/viewable_jpeg.jpeg',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_furl = furl.furl(provider.build_url('files', path.identifier or ''))
        metadata_url = metadata_furl.add(provider.FILE_FIELDS).url
        aiohttpretty.register_json_uri('GET', metadata_url, body=metadata_body)

        result = await provider.metadata(path, revision=self.MAGIC_REVISION)

        expected = GoogleDriveFileMetadata(metadata_body, path)
        assert result == expected
        assert aiohttpretty.has_call(method='GET', uri=metadata_url)


class TestRevisions:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_get_revisions(self, provider, revision_fixtures, root_provider_fixtures):
        file = root_provider_fixtures['list_file']['files'][0]
        path = WaterButlerPath('/birdie.jpg', _ids=('doesntmatter', file['id']))

        revisions_url = provider.build_url('files', file['id'], 'revisions')
        aiohttpretty.register_json_uri('GET', revisions_url,
                                       body=revision_fixtures['revisions_list'])

        result = await provider.revisions(path)
        expected = [
            GoogleDriveRevision(each)
            for each in revision_fixtures['revisions_list']['revisions']
        ]
        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_get_revisions_no_revisions(self, provider, revision_fixtures,
                                              root_provider_fixtures):
        file = root_provider_fixtures['list_file']['files'][0]
        path = WaterButlerPath('/birdie.jpg', _ids=('doesntmatter', file['id']))

        metadata_furl = furl.furl(provider.build_url('files', path.identifier or ''))
        metadata_url = metadata_furl.add(provider.FILE_FIELDS).url
        aiohttpretty.register_json_uri('GET', metadata_url, body=file)

        revisions_url = provider.build_url('files', file['id'], 'revisions')

        aiohttpretty.register_json_uri('GET', revisions_url,
                                       body=revision_fixtures['revisions_list_empty'])

        result = await provider.revisions(path)
        etag = '{}::{}'.format(file['id'], file['modifiedTime'])
        expected = [
            GoogleDriveRevision({
                'modifiedTime': file['modifiedTime'],
                'id': etag + ds.DRIVE_IGNORE_VERSION,
            })
        ]
        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_get_revisions_for_uneditable(self, provider, sharing_fixtures):
        file_fixtures = sharing_fixtures['viewable_gdoc']
        file = file_fixtures['metadata']
        path = WaterButlerPath('/birdie.jpg', _ids=('doesntmatter', file['id']))

        metadata_furl = furl.furl(provider.build_url('files', path.identifier or ''))
        metadata_url = metadata_furl.add(provider.FILE_FIELDS).url
        aiohttpretty.register_json_uri('GET', metadata_url, body=file)

        revisions_url = provider.build_url('files', file['id'], 'revisions')
        aiohttpretty.register_json_uri(
            'GET', revisions_url, body=file_fixtures['revisions_error'], status=403)

        result = await provider.revisions(path)
        etag = '{}::{}'.format(file['id'], file['modifiedTime'])
        expected = [
            GoogleDriveRevision({
                'modifiedTime': file['modifiedTime'],
                'id': etag + ds.DRIVE_IGNORE_VERSION,
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
        assert e.value.message == ('Cannot create folder "hugo", because a file or folder '
                                   'already exists with that name')

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_returns_metadata(self, provider, root_provider_fixtures):
        path = WaterButlerPath('/osf%20test/', _ids=(provider.folder['id'], None))

        metadata_furl = furl.furl(provider.build_url('files', path.identifier or ''))
        metadata_url = metadata_furl.add(provider.FILE_FIELDS).url
        aiohttpretty.register_json_uri('POST', metadata_url,
                                       body=root_provider_fixtures['folder_metadata'])

        resp = await provider.create_folder(path)

        assert aiohttpretty.has_call(method='POST', uri=metadata_url)
        assert resp.kind == 'folder'
        assert resp.name == 'osf test'
        assert resp.path == '/osf%20test/'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_raises_non_404(self, provider):
        path = WaterButlerPath('/hugo/kim/pins/', _ids=(provider.folder['id'],
                                                        'something', 'something', None))

        metadata_furl = furl.furl(provider.build_url('files', path.identifier or ''))
        metadata_url = metadata_furl.add(provider.FILE_FIELDS).url
        aiohttpretty.register_json_uri('POST', metadata_url, status=418)

        with pytest.raises(exceptions.CreateFolderError) as e:
            await provider.create_folder(path)

        assert e.value.code == 418

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_must_be_folder(self, provider, monkeypatch):
        with pytest.raises(exceptions.CreateFolderError) as e:
            await provider.create_folder(WaterButlerPath('/carp.fish', _ids=('doesnt', 'matter')))


class TestIntraFunctions:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_move_file(self, provider, root_provider_fixtures):
        file = root_provider_fixtures['docs_file_metadata']
        src_path = WaterButlerPath('/unsure.txt', _ids=(provider.folder['id'], file['id']))
        dest_path = WaterButlerPath('/really/unsure.txt', _ids=(provider.folder['id'],
                                                                file['id'], file['id']))

        move_furl = furl.furl(provider.build_url('files', src_path.identifier,
                                                 removeParents=src_path.parent.identifier,
                                                 addParents=dest_path.parent.identifier))
        move_url = move_furl.add(provider.FILE_FIELDS).url
        data = json.dumps({
            'name': dest_path.name
        }),
        aiohttpretty.register_json_uri('PATCH', move_url, data=data, body=file)

        delete_url = provider.build_url('files', file['id'])
        del_url_body = json.dumps({'trashed': 'true'})
        aiohttpretty.register_uri('PATCH', delete_url, body=del_url_body, status=200)

        result, created = await provider.intra_move(provider, src_path, dest_path)
        expected = GoogleDriveFileMetadata(file, dest_path)

        assert result == expected
        assert aiohttpretty.has_call(method='PATCH', uri=move_url)
        assert aiohttpretty.has_call(method='PATCH', uri=delete_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_move_folder(self, provider, root_provider_fixtures):
        folder = root_provider_fixtures['folder_metadata']
        folder2 = root_provider_fixtures['folder2_metadata']
        src_path = WaterButlerPath('/unsure/', _ids=(provider.folder['id'], folder['id']))
        dest_path = WaterButlerPath('/really/unsure/', _ids=(provider.folder['id'],
                                                             folder2['id'], folder2['id']))

        move_furl = furl.furl(provider.build_url('files', src_path.identifier,
                                                 removeParents=src_path.parent.identifier,
                                                 addParents=dest_path.parent.identifier))
        move_url = move_furl.add(provider.FILE_FIELDS).url
        data = json.dumps({
            'name': dest_path.name
        }),
        aiohttpretty.register_json_uri('PATCH', move_url, data=data, body=folder)

        delete_url = provider.build_url('files', folder2['id'])
        del_url_body = json.dumps({'trashed': 'true'})
        aiohttpretty.register_uri('PATCH', delete_url, body=del_url_body, status=200)

        query = provider._build_query(src_path.identifier)
        children_furl = furl.furl(provider.build_url('files', q=query, alt='json', pageSize=1000))
        children_url = children_furl.add(provider.FOLDER_FIELDS).url

        children_list = generate_list(3, **root_provider_fixtures['folder_metadata'])
        aiohttpretty.register_json_uri('GET', children_url, body=children_list)

        result, created = await provider.intra_move(provider, src_path, dest_path)
        expected = GoogleDriveFolderMetadata(folder, dest_path)
        expected.children = [
            provider._serialize_file(dest_path.child(file['name']), file)
            for file in children_list['files']
        ]

        assert result == expected
        assert aiohttpretty.has_call(method='PATCH', uri=move_url)
        assert aiohttpretty.has_call(method='PATCH', uri=delete_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_copy_file(self, provider, root_provider_fixtures):
        file = root_provider_fixtures['docs_file_metadata']
        src_path = WaterButlerPath('/unsure.txt', _ids=(provider.folder['id'], file['id']))
        dest_path = WaterButlerPath('/really/unsure.txt', _ids=(provider.folder['id'],
                                                                file['id'], file['id']))

        copy_furl = furl.furl(provider.build_url('files', src_path.identifier, 'copy'))
        copy_url = copy_furl.add(provider.FILE_FIELDS).url
        data = json.dumps({
            'parents': [{
                'id': dest_path.parent.identifier
            }],
            'name': dest_path.name
        }),
        aiohttpretty.register_json_uri('POST', copy_url, data=data, body=file)

        delete_url = provider.build_url('files', file['id'])
        del_url_body = json.dumps({'trashed': 'true'})
        aiohttpretty.register_uri('PATCH', delete_url, body=del_url_body, status=200)

        result, created = await provider.intra_copy(provider, src_path, dest_path)
        expected = GoogleDriveFileMetadata(file, dest_path)

        assert result == expected
        assert aiohttpretty.has_call(method='POST', uri=copy_url)
        assert aiohttpretty.has_call(method='PATCH', uri=delete_url)


class TestOperationsOrMisc:

    def test_can_duplicate_names(self, provider):
        assert provider.can_duplicate_names() is True

    def test_shares_storage_root(self, provider, other_provider):
        assert provider.shares_storage_root(other_provider) is True
        assert provider.shares_storage_root(provider) is True

    def test_can_intra_move(self, provider, other_provider):
        assert provider.can_intra_move(other_provider) is False
        assert provider.can_intra_move(provider) is True

    def test_serialize_item_raw(self, provider, root_provider_fixtures):
        file = root_provider_fixtures['docs_file_metadata']

        assert provider._serialize_file(None, file, True) == file

    def test_can_intra_copy(self, provider, other_provider, root_provider_fixtures):
        file = root_provider_fixtures['list_file']['files'][0]
        path = WaterButlerPath('/birdie.jpg', _ids=(provider.folder['id'], file['id']))

        assert provider.can_intra_copy(other_provider, path) is False
        assert provider.can_intra_copy(provider, path) is True

    def test_path_from_metadata(self, provider, root_provider_fixtures):
        file = root_provider_fixtures['docs_file_metadata']
        src_path = WaterButlerPath('/version-test.docx', _ids=(provider.folder['id'], file['id']))

        metadata = GoogleDriveFileMetadata(file, src_path)
        child_path = provider.path_from_metadata(src_path.parent, metadata)

        assert child_path.full_path == src_path.full_path
        assert child_path == src_path

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_revalidate_path_file_error(self, provider, root_provider_fixtures,
                                              error_fixtures):
        file_name = '/root/whatever/Gear1.stl'
        file_id = root_provider_fixtures['revalidate_path_file_metadata_1']['files'][0]['id']
        path = GoogleDrivePath(file_name, _ids=['0', file_id, file_id, file_id])

        parts = [[parse.unquote(x), True] for x in file_name.strip('/').split('/')]
        parts[-1][1] = False
        current_part = parts.pop(0)
        part_name, part_is_folder = current_part[0], current_part[1]
        query = _build_name_search_query(provider, part_name, provider.folder['id'], True)

        list_url = provider.build_url('files', q=query, fields='files(id)')
        aiohttpretty.register_json_uri('GET', list_url,
                                       body=error_fixtures['parts_file_missing_metadata'])

        with pytest.raises(exceptions.MetadataError) as e:
            result = await provider._resolve_path_to_ids(file_name)

        assert e.value.message == '{} not found'.format(str(path))
        assert e.value.code == 404
