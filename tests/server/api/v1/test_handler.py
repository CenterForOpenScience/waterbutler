from unittest import mock

import pytest

from tests.utils import (
    HandlerTestCase,
    MockProvider,
    MockFileMetadata,
    MockCoroutine,
)
from tests.server.api.v1.fixtures import (
    mock_time,
    handler,
    http_request,
    log_payload,
    source_payload,
    destination_payload,
    payload_metadata,
    payload_path,
    serialzied_request
)

class TestSendHook:

    @pytest.mark.parametrize('action', ['move', 'copy'])
    @mock.patch('waterbutler.core.remote_logging.log_file_action')
    def test_send_hook_cant_intra_move_copy(self, mocked_log_file_action, handler, action):

        assert handler._send_hook(action) is None
        mocked_log_file_action.assert_not_called()

    @pytest.mark.parametrize('action', ['move', 'copy'])
    @mock.patch('waterbutler.core.remote_logging.log_file_action')
    def test_send_hook_can_intra_move_copy(self,
                                           mocked_log_file_action,
                                           handler,
                                           source_payload,
                                           destination_payload,
                                           serialzied_request,
                                           event_loop,
                                           action):

        setattr(handler.provider, 'can_intra_' + action, mock.Mock(return_value=True))
        assert handler._send_hook(action) is None
        mocked_log_file_action.assert_called_once_with(action,
                                                       api_version='v1',
                                                       bytes_downloaded=0,
                                                       bytes_uploaded=0,
                                                       source=source_payload,
                                                       destination=destination_payload,
                                                       request=serialzied_request)

    @pytest.mark.parametrize("action", ['create', 'create_folder', 'update'])
    @mock.patch('waterbutler.core.remote_logging.log_file_action')
    def test_send_hook_always_send_metadata(self,
                                            mocked_log_file_action,
                                            handler,
                                            payload_metadata,
                                            serialzied_request,
                                            event_loop,
                                            action):

        assert handler._send_hook(action) is None
        mocked_log_file_action.assert_called_once_with(action,
                                                       api_version='v1',
                                                       bytes_downloaded=0,
                                                       bytes_uploaded=0,
                                                       source=payload_metadata,
                                                       destination=None,
                                                       request=serialzied_request)

    @pytest.mark.parametrize("action", ['delete', 'download_file', 'download_zip', 'metadata'])
    @mock.patch('waterbutler.core.remote_logging.log_file_action')
    def test_send_hook_always_send_path(self,
                                        mocked_log_file_action,
                                        handler,
                                        source_payload,
                                        serialzied_request,
                                        event_loop,
                                        action):

        assert handler._send_hook(action) is None
        mocked_log_file_action.assert_called_once_with(action,
                                                       api_version='v1',
                                                       bytes_downloaded=0,
                                                       bytes_uploaded=0,
                                                       source=source_payload,
                                                       destination=None,
                                                       request=serialzied_request)

    @mock.patch('waterbutler.core.remote_logging.log_file_action')
    def test_send_hook_invalid_action(self, mocked_log_file_action, handler, event_loop):

        assert handler._send_hook('invalid_action') is None
        mocked_log_file_action.assert_not_called()
