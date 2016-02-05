import os
import io
import time
import asyncio
from http import client
from unittest import mock

import pytest
import aiohttpretty

from tests import utils
from tests.utils import async

from waterbutler.core import streams
from waterbutler.core import metadata
from waterbutler.core import exceptions
from waterbutler.server import settings
from waterbutler.core.path import WaterButlerPath
from waterbutler.providers.osfstorage import OSFStorageProvider
from waterbutler.providers.osfstorage.settings import FILE_PATH_COMPLETE


@pytest.fixture
def file_content():
    return b'sleepy'


@pytest.fixture
def file_like(file_content):
    return io.BytesIO(file_content)


@pytest.fixture
def file_stream(file_like):
    return streams.FileStreamReader(file_like)


@pytest.fixture
def auth():
    return {
        'id': 'cat',
        'name': 'cat',
        'email': 'cat@cat.com',
    }


@pytest.fixture
def credentials():
    return {
        'storage': {
            'access_key': 'Dont dead',
            'secret_key': 'open inside',
        },
        'archive': {},
        'parity': {},
    }


@pytest.fixture
def settings():
    return {
        'justa': 'setting',
        'nid': 'foo',
        'rootId': 'rootId',
        'baseUrl': 'https://waterbutler.io',
        'storage': {
            'provider': 'mock',
        },
        'archive': {},
        'parity': {},
    }


@pytest.fixture
def provider_and_mock(monkeypatch, auth, credentials, settings):
    mock_provider = utils.MockProvider1({}, {}, {})

    mock_provider.copy = utils.MockCoroutine()
    mock_provider.move = utils.MockCoroutine()
    mock_provider.delete = utils.MockCoroutine()
    mock_provider.upload = utils.MockCoroutine()
    mock_provider.download = utils.MockCoroutine()
    mock_provider.metadata = utils.MockCoroutine()

    mock_make_provider = mock.Mock(return_value=mock_provider)
    monkeypatch.setattr(OSFStorageProvider, 'make_provider', mock_make_provider)
    return OSFStorageProvider(auth, credentials, settings), mock_provider


@pytest.fixture
def provider(provider_and_mock):
    provider, _ = provider_and_mock
    return provider


@pytest.fixture
def osf_response():
    return {
        'data': {
            'path': 'test/path',
            'name': 'unrelatedpath',
        },
        'settings': {
            'justa': 'settings'
        }
    }

@pytest.fixture
def upload_response():
    return {
        'version': 'vid',
        'status': 'success',
        'data': {
            'downloads': 10,
            'version': 8,
            'path': '/dfl893b1pdn11kd28b',
            'checkout': None,
            'md5': 'abcdabcdabcdabcdabcdabcdabcd',
            'sha256': '123123123123123123',
        }
    }


@pytest.fixture
def mock_path():
    return WaterButlerPath('/unrelatedpath', _ids=('rootId', 'another'))


@pytest.fixture
def mock_folder_path():
    return WaterButlerPath('/unrelatedfolder/', _ids=('rootId', 'another'))


@pytest.fixture
def mock_time(monkeypatch):
    mock_time = mock.Mock(return_value=1454684930.0)
    monkeypatch.setattr(time, 'time', mock_time)


@pytest.fixture
def folder_lineage():
    return {
        'data': [
            {
                'id': '56045626cfe191ead0264305',
                'kind': 'folder',
                'name': 'Lunchbuddy',
                'path': '/56045626cfe191ead0264305/'
            },
            {
                'id': '560012ffcfe1910de19a872f',
                'kind': 'folder',
                'name': '',
                'path': '/560012ffcfe1910de19a872f/'
            }
        ]
    }


@pytest.fixture
def file_lineage():
    return {
        'data': [
            {
                'contentType': None,
                'downloads': 0,
                'id': '56152738cfe1912c7d74cad7',
                'kind': 'file',
                'md5': '067a41deb18fc7b0bbbd748f24f89ebb',
                'modified': '2015-10-07T10:07:52',
                'name': 'bar.txt',
                'path': '/56152738cfe1912c7d74cad7',
                'sha256': 'ca01ef62c58964ce437fc300418c29a471ff92a991e4123a65ca8bf65d318710',
                'size': 17,
                'version': 1
            },
            {
                'id': '560012ffcfe1910de19a872f',
                'kind': 'folder',
                'name': '',
                'path': '/560012ffcfe1910de19a872f/'}
        ]
    }


@async
@pytest.mark.aiohttpretty
def test_download(monkeypatch, provider_and_mock, osf_response, mock_path, mock_time):
    provider, inner_provider = provider_and_mock

    base_url = provider.build_url(mock_path.identifier, 'download', version=None, mode=None)
    url, _, params = provider.build_signed_url('GET', base_url)

    aiohttpretty.register_json_uri('GET', url, params=params, body=osf_response)

    yield from provider.download(mock_path)

    assert provider.make_provider.called
    assert inner_provider.download.called

    assert aiohttpretty.has_call(method='GET', uri=url, params=params)
    provider.make_provider.assert_called_once_with(osf_response['settings'])
    inner_provider.download.assert_called_once_with(path=WaterButlerPath('/test/path'), displayName='unrelatedpath')


@async
@pytest.mark.aiohttpretty
def test_delete(monkeypatch, provider, mock_path, mock_time):
    path = WaterButlerPath('/unrelatedpath', _ids=('Doesntmatter', 'another'))
    params = {'user': 'cat'}
    base_url = provider.build_url(path.identifier)
    url, _, params = provider.build_signed_url('DELETE', base_url, params=params)
    aiohttpretty.register_uri('DELETE', url, params=params, status_code=200)

    yield from provider.delete(path)

    assert aiohttpretty.has_call(method='DELETE', uri=url, check_params=False)


@async
@pytest.mark.aiohttpretty
def test_provider_metadata_empty(monkeypatch, provider, mock_folder_path, mock_time):
    base_url = provider.build_url(mock_folder_path.identifier, 'children')
    url, _, params = provider.build_signed_url('GET', base_url)
    aiohttpretty.register_json_uri('GET', url, params=params, status_code=200, body=[])

    res = yield from provider.metadata(mock_folder_path)

    assert res == []

    assert aiohttpretty.has_call(method='GET', uri=url, params=params)


@async
@pytest.mark.aiohttpretty
def test_provider_metadata(monkeypatch, provider, mock_folder_path, mock_time):
    items = [
        {
            'name': 'foo',
            'path': '/foo',
            'kind': 'file',
            'version': 10,
            'downloads': 1,
            'md5': '1234',
            'sha256': '2345',
        },
        {
            'name': 'bar',
            'path': '/bar',
            'kind': 'file',
            'version': 10,
            'downloads': 1,
            'md5': '1234',
            'sha256': '2345',
        },
        {
            'name': 'baz',
            'path': '/baz',
            'kind': 'folder'
        }
    ]
    url, _, params = provider.build_signed_url('GET', provider.build_url(mock_folder_path.identifier, 'children'))
    aiohttpretty.register_json_uri('GET', url, params=params, status=200, body=items)

    res = yield from provider.metadata(mock_folder_path)

    assert isinstance(res, list)

    for item in res:
        assert isinstance(item, metadata.BaseMetadata)
        assert item.name is not None
        assert item.path is not None
        assert item.provider == 'osfstorage'

    assert aiohttpretty.has_call(method='GET', uri=url, params=params)


class TestValidatePath:

    @async
    @pytest.mark.aiohttpretty
    def test_validate_v1_path_file(self, provider, file_lineage, mock_time):
        file_path = '56152738cfe1912c7d74cad7'

        url, _, params = provider.build_signed_url('GET', 'https://waterbutler.io/{}/lineage/'.format(file_path))
        aiohttpretty.register_json_uri('GET', url, params=params, status=200, body=file_lineage)

        # folder_path = '56045626cfe191ead0264305'
        try:
            wb_path_v1 = yield from provider.validate_v1_path('/' + file_path)
        except Exception as exc:
            pytest.fail(str(exc))

        with pytest.raises(exceptions.NotFoundError) as exc:
            yield from provider.validate_v1_path('/' + file_path + '/')

        assert exc.value.code == client.NOT_FOUND

        wb_path_v0 = yield from provider.validate_path('/' + file_path)

        assert wb_path_v1 == wb_path_v0

    @async
    @pytest.mark.aiohttpretty
    def test_validate_v1_path_folder(self, provider, folder_lineage, mock_time):
        folder_path = '56045626cfe191ead0264305'

        url, _, params = provider.build_signed_url('GET', 'https://waterbutler.io/{}/lineage/'.format(folder_path))
        aiohttpretty.register_json_uri('GET', url, params=params, status=200, body=folder_lineage)

        try:
            wb_path_v1 = yield from provider.validate_v1_path('/' + folder_path + '/')
        except Exception as exc:
            pytest.fail(str(exc))

        with pytest.raises(exceptions.NotFoundError) as exc:
            yield from provider.validate_v1_path('/' + folder_path)

        assert exc.value.code == client.NOT_FOUND

        wb_path_v0 = yield from provider.validate_path('/' + folder_path + '/')

        assert wb_path_v1 == wb_path_v0

class TestUploads:

    def patch_tasks(self, monkeypatch):
        basepath = 'waterbutler.providers.osfstorage.provider.{}'
        monkeypatch.setattr(basepath.format('os.rename'), lambda *_: None)
        monkeypatch.setattr(basepath.format('settings.RUN_TASKS'), False)
        monkeypatch.setattr(basepath.format('uuid.uuid4'), lambda: 'uniquepath')

    @async
    @pytest.mark.aiohttpretty
    def test_upload_new(self, monkeypatch, provider_and_mock, file_stream, upload_response, mock_time):
        self.patch_tasks(monkeypatch)

        path = WaterButlerPath('/newfile', _ids=('rootId', None))
        url = 'https://waterbutler.io/{}/children/'.format(path.parent.identifier)
        aiohttpretty.register_json_uri('POST', url, status=201, body=upload_response)

        provider, inner_provider = provider_and_mock
        inner_provider.metadata = utils.MockCoroutine(return_value=utils.MockFileMetadata())

        res, created = yield from provider.upload(file_stream, path)

        assert created is True
        assert res.name == 'newfile'
        assert res.extra['version'] == 8
        assert res.provider == 'osfstorage'
        assert res.extra['downloads'] == 10
        assert res.extra['checkout'] is None
        assert path.identifier_path == res.path

        inner_provider.delete.assert_called_once_with(WaterButlerPath('/uniquepath'))
        inner_provider.metadata.assert_called_once_with(WaterButlerPath('/' + file_stream.writers['sha256'].hexdigest))
        inner_provider.upload.assert_called_once_with(file_stream, WaterButlerPath('/uniquepath'), check_created=False, fetch_metadata=False)

    @async
    @pytest.mark.aiohttpretty
    def test_upload_existing(self, monkeypatch, provider_and_mock, file_stream, mock_time):
        self.patch_tasks(monkeypatch)
        provider, inner_provider = provider_and_mock

        path = WaterButlerPath('/foopath', _ids=('Test', 'OtherTest'))
        url = 'https://waterbutler.io/{}/children/'.format(path.parent.identifier)

        inner_provider.move.return_value = (utils.MockFileMetadata(), True)
        inner_provider.metadata.side_effect = exceptions.MetadataError('Boom!', code=404)

        aiohttpretty.register_json_uri('POST', url, status=200, body={'data': {'downloads': 10, 'version': 8, 'path': '/24601', 'checkout': 'hmoco', 'md5': '1234', 'sha256': '2345'}})

        res, created = yield from provider.upload(file_stream, path)

        assert created is False
        assert res.name == 'foopath'
        assert res.path == '/24601'
        assert res.extra['version'] == 8
        assert res.provider == 'osfstorage'
        assert res.extra['downloads'] == 10
        assert res.extra['checkout'] == 'hmoco'

        inner_provider.metadata.assert_called_once_with(WaterButlerPath('/' + file_stream.writers['sha256'].hexdigest))
        inner_provider.upload.assert_called_once_with(file_stream, WaterButlerPath('/uniquepath'), check_created=False, fetch_metadata=False)
        inner_provider.move.assert_called_once_with(inner_provider, WaterButlerPath('/uniquepath'), WaterButlerPath('/' + file_stream.writers['sha256'].hexdigest))

    @async
    @pytest.mark.aiohttpretty
    def test_upload_and_tasks(self, monkeypatch, provider_and_mock, file_stream, credentials, settings, mock_time):
        provider, inner_provider = provider_and_mock
        basepath = 'waterbutler.providers.osfstorage.provider.{}'
        path = WaterButlerPath('/foopath', _ids=('Test', 'OtherTest'))
        url = 'https://waterbutler.io/{}/children/'.format(path.parent.identifier)

        mock_parity = mock.Mock()
        mock_backup = mock.Mock()
        inner_provider.move.return_value = (utils.MockFileMetadata(), True)
        inner_provider.metadata.side_effect = exceptions.MetadataError('Boom!', code=404)

        aiohttpretty.register_json_uri('POST', url, status=201, body={'version': 'versionpk', 'data': {'version': 42, 'downloads': 30, 'path': '/alkjdaslke09', 'checkout': None, 'md5': 'abcd', 'sha256': 'bcde'}})

        monkeypatch.setattr(basepath.format('backup.main'), mock_backup)
        monkeypatch.setattr(basepath.format('parity.main'), mock_parity)
        monkeypatch.setattr(basepath.format('settings.RUN_TASKS'), True)
        monkeypatch.setattr(basepath.format('os.rename'), lambda *_: None)
        monkeypatch.setattr(basepath.format('uuid.uuid4'), lambda: 'uniquepath')

        res, created = yield from provider.upload(file_stream, path)

        assert created is True
        assert res.name == 'foopath'
        assert res.extra['version'] == 42
        assert res.provider == 'osfstorage'
        assert res.extra['downloads'] == 30
        assert res.extra['checkout'] is None

        inner_provider.upload.assert_called_once_with(file_stream, WaterButlerPath('/uniquepath'), check_created=False, fetch_metadata=False)
        complete_path = os.path.join(FILE_PATH_COMPLETE, file_stream.writers['sha256'].hexdigest)
        mock_parity.assert_called_once_with(complete_path, credentials['parity'], settings['parity'])
        mock_backup.assert_called_once_with(complete_path, 'versionpk', 'https://waterbutler.io/hooks/metadata/', credentials['archive'], settings['parity'])
        inner_provider.metadata.assert_called_once_with(WaterButlerPath('/' + file_stream.writers['sha256'].hexdigest))
        inner_provider.move.assert_called_once_with(inner_provider, WaterButlerPath('/uniquepath'), WaterButlerPath('/' + file_stream.writers['sha256'].hexdigest))
