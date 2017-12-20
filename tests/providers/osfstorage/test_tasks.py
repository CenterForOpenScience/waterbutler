import os
import json
import asyncio
from unittest import mock

import pytest
import aiohttpretty

from tests import utils as test_utils

from boto.glacier.exceptions import UnexpectedHTTPResponseError

from waterbutler.core import signing
from waterbutler.core.path import WaterButlerPath
from waterbutler.providers.osfstorage.settings import HMAC_ALGORITHM
from waterbutler.providers.osfstorage.settings import HMAC_SECRET
from waterbutler.providers.osfstorage.tasks import utils
from waterbutler.providers.osfstorage.tasks import backup
from waterbutler.providers.osfstorage.tasks import parity
from waterbutler.providers.osfstorage.tasks import exceptions
from waterbutler.providers.osfstorage import settings as osf_settings


@pytest.fixture
def credentials():
    return {
    }


@pytest.fixture
def settings():
    return {
        'storage': {
            'provider': 'cloud',
            'container': 'foo',
        },
    }

@pytest.fixture
def mock_provider(monkeypatch):
    mock_provider = test_utils.MockProvider1({}, {}, {})

    mock_provider.copy = test_utils.MockCoroutine()
    mock_provider.move = test_utils.MockCoroutine()
    mock_provider.delete = test_utils.MockCoroutine()
    mock_provider.upload = test_utils.MockCoroutine()
    mock_provider.download = test_utils.MockCoroutine()
    mock_provider.metadata = test_utils.MockCoroutine()

    mock_make_provider = mock.Mock(return_value=mock_provider)
    monkeypatch.setattr(parity, 'make_provider', mock_make_provider)
    return mock_provider


class TestParityTask:

    def test_main_delays(self, monkeypatch, event_loop, credentials, settings):
        task = mock.Mock()
        monkeypatch.setattr(parity, '_parity_create_files', task)

        fut = parity.main('The Best', 0, None, credentials, settings)
        event_loop.run_until_complete(fut)

        task.delay.assert_called_once_with('The Best', 0, None, credentials, settings)

    def test_creates_upload_futures(self, monkeypatch, event_loop, credentials, settings):
        paths = ['/foo/bar{}'.format(p) for p in range(10)]
        future = asyncio.Future()
        future.set_result(('faake_name', 'fake_sha256'))
        mock_upload_parity = mock.Mock()
        mock_upload_parity.return_value = future
        mock_create_parity = mock.Mock(return_value=paths)
        monkeypatch.setattr(parity, '_upload_parity', mock_upload_parity)
        monkeypatch.setattr(parity.utils, 'create_parity_files', mock_create_parity)
        monkeypatch.setattr(parity, '_push_parity_complete', mock.Mock())

        parity._parity_create_files('Triangles', None, None, credentials, settings)

        mock_create_parity.assert_called_once_with(
            os.path.join(osf_settings.FILE_PATH_COMPLETE, 'Triangles'),
            redundancy=osf_settings.PARITY_REDUNDANCY,
        )
        for p in reversed(paths):
            mock_upload_parity.assert_any_call(p, credentials, settings)

    @pytest.mark.asyncio
    async def test_uploads(self, monkeypatch, tmpdir, mock_provider):
        tempfile = tmpdir.join('test.file')
        stream = parity.streams.FileStreamReader(tempfile)
        monkeypatch.setattr(parity.streams, 'FileStreamReader', lambda x: stream)

        tempfile.write('foo')
        path = tempfile.strpath

        await parity._upload_parity(path, {}, {})

        assert mock_provider.upload.called

        mock_provider.upload.assert_called_once_with(
            stream,
            WaterButlerPath('/' + os.path.split(path)[1])
        )

    def test_calls_complete(self, monkeypatch, event_loop, mock_provider, credentials, settings):
        paths = ['/foo/bar{}'.format(p) for p in range(10)]
        paths.sort(key=lambda datum: datum[0])
        mock_complete = mock.Mock()
        monkeypatch.setattr(parity.utils, 'create_parity_files', mock.Mock(return_value=paths))
        monkeypatch.setattr(parity, '_push_parity_complete', mock_complete)

        with mock.patch('waterbutler.providers.osfstorage.tasks.parity.open', mock.mock_open(), create=False):
            parity._parity_create_files('Triangles', 0, 'http://callbackurl', credentials, settings)

        complete_call_args = mock_complete.delay.call_args[0]
        complete_call_args[2]['parity']['files'].sort(key=lambda datum: datum['name'])
        assert complete_call_args == (
            0,
            'http://callbackurl',
            {
                'parity': {
                    'redundancy': osf_settings.PARITY_REDUNDANCY,
                    'files': sorted([
                        {
                            'name': os.path.split(p)[1],
                            'sha256': 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',  # mock_provider = /dev/null
                        }
                        for p in paths
                    ], key=lambda datum: datum['name']),
                }
            },
        )

    def test_exceptions_get_raised(self, monkeypatch):
        mock_sp_call = mock.Mock(return_value=7)
        monkeypatch.setattr(utils.subprocess, 'call', mock_sp_call)
        path = 'foo/bar/baz'
        args = ['par2', 'c', '-r5', 'baz.par2', path]

        with pytest.raises(exceptions.ParchiveError) as e:
            utils.create_parity_files(path)

            assert e.value == '{0} failed with code {1}'.format(' '.join(args), 7)

            with open(os.devnull, 'wb') as DEVNULL:
                mock_sp_call.assert_called_once_with(args, stdout=DEVNULL, stderr=DEVNULL)

    def test_skip_empty_files(self, monkeypatch):
        mock_stat = mock.Mock(return_value=mock.Mock(st_size=0))
        mock_sp_call = mock.Mock()
        monkeypatch.setattr(os, 'stat', mock_stat)
        monkeypatch.setattr(utils.subprocess, 'call', mock_sp_call)
        path = 'foo/bar/baz'

        paths = utils.create_parity_files(path)
        assert paths == []
        assert not mock_sp_call.called

    @pytest.mark.aiohttpretty
    def test_push_complete(self, event_loop):
        callback_url = 'https://fakeosf.io/guidz/osfstorage/hooks/metadata/'
        aiohttpretty.register_json_uri('PUT', callback_url, status=200, body={'status': 'success'})

        parity._push_parity_complete(123, callback_url, {'some': 'metadata'})

        assert aiohttpretty.has_call(method='PUT', uri=callback_url)

    @pytest.mark.aiohttpretty
    def test_push_complete_error(self, event_loop):
        callback_url = 'https://fakeosf.io/guidz/osfstorage/hooks/metadata/'
        aiohttpretty.register_json_uri('PUT', callback_url, status=500)

        with pytest.raises(Exception):
            parity._push_parity_complete(123, callback_url, {'some': 'metadata'})


class TestBackupTask:

    def test_main_delays(self, monkeypatch, event_loop):
        task = mock.Mock()
        monkeypatch.setattr(backup, '_push_file_archive', task)

        fut = backup.main('The Best', 0, None, {}, {})
        event_loop.run_until_complete(fut)

        task.delay.assert_called_once_with('The Best', 0, None, {}, {})

    def test_tries_upload(self, monkeypatch):
        mock_vault = mock.Mock()
        mock_vault.name = 'ThreePoint'
        mock_vault.upload_archive.return_value = 3
        mock_get_vault = mock.Mock()
        mock_get_vault.return_value = mock_vault
        mock_complete = mock.Mock()
        monkeypatch.setattr(backup, 'get_vault', mock_get_vault)
        monkeypatch.setattr(backup, '_push_archive_complete', mock_complete)

        backup._push_file_archive('Triangles', None, None, {}, {})

        mock_vault.upload_archive.assert_called_once_with('Triangles', description='Triangles')

    def test_calls_complete(self, monkeypatch, credentials, settings):
        mock_vault = mock.Mock()
        mock_complete = mock.Mock()
        mock_vault.name = 'ThreePoint'
        mock_vault.upload_archive.return_value = 3
        mock_get_vault = mock.Mock()
        mock_get_vault.return_value = mock_vault
        monkeypatch.setattr(backup, 'get_vault', mock_get_vault)
        monkeypatch.setattr(backup, '_push_archive_complete', mock_complete)

        backup._push_file_archive('Triangles', 0, None, credentials, settings)

        mock_complete.delay.assert_called_once_with(
            0,
            None,
            {
                'vault': 'ThreePoint',
                'archive': 3
            },
        )

    def test_upload_error_empty_file(self, monkeypatch):
        mock_vault = mock.Mock()
        mock_vault.name = 'ThreePoint'
        mock_response = mock.Mock()
        mock_response.status = 400
        mock_response.read.return_value = json.dumps({
            'status': 400,
            'message': 'Invalid Content-Length: 0',
        }).encode('utf-8')
        error = UnexpectedHTTPResponseError(200, mock_response)
        mock_vault.upload_archive.side_effect = error
        mock_get_vault = mock.Mock()
        mock_get_vault.return_value = mock_vault
        mock_complete = mock.Mock()
        monkeypatch.setattr(backup, 'get_vault', mock_get_vault)
        monkeypatch.setattr(backup, '_push_archive_complete', mock_complete)

        backup._push_file_archive('Triangles', None, None, {}, {})

        mock_vault.upload_archive.assert_called_once_with('Triangles', description='Triangles')
        assert not mock_complete.called

    def test_upload_error(self, monkeypatch):
        mock_vault = mock.Mock()
        mock_vault.name = 'ThreePoint'
        mock_response = mock.Mock()
        mock_response.status = 400
        mock_response.read.return_value = json.dumps({
            'status': 400,
            'message': 'Jean Valjean means nothing now',
        }).encode('utf-8')
        error = UnexpectedHTTPResponseError(200, mock_response)
        mock_vault.upload_archive.side_effect = error
        mock_get_vault = mock.Mock()
        mock_get_vault.return_value = mock_vault
        mock_complete = mock.Mock()
        monkeypatch.setattr(backup, 'get_vault', mock_get_vault)
        monkeypatch.setattr(backup, '_push_archive_complete', mock_complete)

        with pytest.raises(UnexpectedHTTPResponseError):
            backup._push_file_archive('Triangles', None, None, {}, {})
        assert not mock_complete.called

    @pytest.mark.aiohttpretty
    def test_push_complete(self, event_loop):
        callback_url = 'https://fakeosf.io/guidz/osfstorage/hooks/metadata/'
        aiohttpretty.register_json_uri('PUT', callback_url, status=200, body={'status': 'success'})

        backup._push_archive_complete(123, callback_url, {'some': 'metadata'})

        assert aiohttpretty.has_call(method='PUT', uri=callback_url)

    @pytest.mark.aiohttpretty
    def test_push_complete_error(self, event_loop):
        callback_url = 'https://fakeosf.io/guidz/osfstorage/hooks/metadata/'
        aiohttpretty.register_json_uri('PUT', callback_url, status=500)

        with pytest.raises(Exception):
            backup._push_archive_complete(123, callback_url, {'some': 'metadata'})
