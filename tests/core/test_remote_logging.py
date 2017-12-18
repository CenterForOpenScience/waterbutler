from unittest import mock

import pytest

from waterbutler.core import remote_logging
from waterbutler.core.log_payload import LogPayload
from waterbutler.core.remote_logging import log_to_callback

from tests.providers.osfstorage.fixtures import (
    file_metadata_object,
    file_path,
    file_metadata,
    file_lineage,
    provider,
    auth,
    credentials,
    settings
)
from tests.core.fixtures import (
    log_payload,
    MockBadResponse,
    mock_time,
    callback_log_payload_move,
    callback_log_payload_copy,
    callback_log_payload_upload,
    mock_signed_request
)


class TestScrubPayloadForKeen:

    def test_flat_dict(self):
        payload = {
            'key': 'value',
            'key2': 'value2'
        }

        result = remote_logging._scrub_headers_for_keen(payload)

        assert result == {
            'key': 'value',
            'key2': 'value2'
        }

    def test_flat_dict_needs_scrubbing(self):
        payload = {
            'key.test': 'value',
            'key2': 'value2'
        }

        result = remote_logging._scrub_headers_for_keen(payload)

        assert result == {
            'key-test': 'value',
            'key2': 'value2'
        }

    def test_scrub_and_rename(self):
        payload = {
            'key.test': 'unique value',
            'key-test': 'value2'
        }

        result = remote_logging._scrub_headers_for_keen(payload)

        # "key.test" sorts after "key-test" and will therefore be renamed
        assert result == {
            'key-test': 'value2',
            'key-test-1': 'unique value'
        }

    def test_scrub_and_loop_rename(self):
        payload = {
            'key.test': 'value1',
            'key-test': 'value2',
            'key-test-1': 'value3'
        }

        result = remote_logging._scrub_headers_for_keen(payload)

        assert result == {
            'key-test': 'value2',
            'key-test-2': 'value1',
            'key-test-1': 'value3'

        }

    def test_max_iteration(self):
        payload = {
            'key.test': 'value1',
            'key-test': 'value2',
            'key-test-1': 'value3'
        }

        result = remote_logging._scrub_headers_for_keen(payload, MAX_ITERATIONS=1)

        assert result == {
            'key-test': 'value2',
            'key-test-1': 'value3'
        }


class TestLogPayLoad:

    def test_log_payload(self, log_payload, file_metadata_object, file_path, provider):
        assert log_payload.resource == 'guid0'
        assert log_payload.provider == provider
        assert log_payload.metadata == file_metadata_object
        assert log_payload.path == file_path

        with pytest.raises(Exception) as exc:
            LogPayload('guid0', 'osfstorage')
            assert exc.message == 'Log payload needs either a path or metadata.'


class TestLogToCallback:

    @pytest.mark.asyncio
    async def test_log_to_callback_no_logging(self):
        assert (await log_to_callback('download_file')) is None
        assert (await log_to_callback('download_zip')) is None
        assert (await log_to_callback('metadata')) is None

    @pytest.mark.asyncio
    async def test_log_to_callback_move(self,
                                        log_payload,
                                        callback_log_payload_move,
                                        mock_signed_request,
                                        mock_time):

        with mock.patch('waterbutler.core.utils.send_signed_request', mock_signed_request):
            await log_to_callback('move', log_payload, log_payload)
            mock_signed_request.assert_called_with('PUT',
                                                   log_payload.auth['callback_url'],
                                                   callback_log_payload_move)

    @pytest.mark.asyncio
    async def test_log_to_callback_copy(self,
                                        log_payload,
                                        callback_log_payload_copy,
                                        mock_signed_request,
                                        mock_time):

        with mock.patch('waterbutler.core.utils.send_signed_request', mock_signed_request):
            await log_to_callback('copy', log_payload, log_payload)
            mock_signed_request.assert_called_with('PUT',
                                                   log_payload.auth['callback_url'],
                                                   callback_log_payload_copy)

    @pytest.mark.asyncio
    async def test_log_to_callback_upload(self,
                                          log_payload,
                                          callback_log_payload_upload,
                                          mock_signed_request,
                                          mock_time):

        with mock.patch('waterbutler.core.utils.send_signed_request', mock_signed_request):
            await log_to_callback('upload', log_payload, log_payload)
            mock_signed_request.assert_called_with('PUT',
                                                   log_payload.auth['callback_url'],
                                                   callback_log_payload_upload)

    @pytest.mark.skipif(reason="This test takes too much time because it has 5 retries before "
                               "throwing the desired exception, it should take around 50-60 "
                               "seconds")
    @pytest.mark.asyncio
    async def test_log_to_callback_throws_exception(self, log_payload, mock_signed_request):

        with mock.patch('waterbutler.core.utils.send_signed_request', mock_signed_request):
            with pytest.raises(Exception) as exc:
                await log_to_callback('upload', log_payload, log_payload)
                assert exc.message == 'Callback for upload request failed with {},' \
                                      ' got {"status": "failure"}'.format(MockBadResponse())
