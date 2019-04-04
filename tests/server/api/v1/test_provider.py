from uuid import UUID
from unittest import mock

import pytest

from waterbutler.core.path import WaterButlerPath
from waterbutler.server.api.v1.provider import list_or_value

from tests.server.api.v1.utils import mock_handler
from tests.utils import MockCoroutine, MockStream, MockWriter, MockProvider
from tests.server.api.v1.fixtures import (http_request, patch_auth_handler,
                                          handler_auth, patch_make_provider_core)


class TestUtils:

    def test_list_or_value(self):
        with pytest.raises(AssertionError):
            list_or_value('not list')

        assert list_or_value([]) is None
        assert list_or_value([b'singleitem']) == 'singleitem'
        assert list_or_value([b'decoded', b'value']) == ['decoded', 'value']


class TestProviderHandler:

    @pytest.mark.asyncio
    async def test_prepare(self, http_request, patch_auth_handler, patch_make_provider_core):
        handler = mock_handler(http_request)
        await handler.prepare()

        # check that X-WATERBUTLER-REQUEST-ID is valid UUID
        assert UUID(handler._headers['X-WATERBUTLER-REQUEST-ID'], version=4)

    @pytest.mark.asyncio
    async def test_prepare_put(self, http_request, patch_auth_handler, patch_make_provider_core,
                               handler_auth):
        handler = mock_handler(http_request)
        handler.request.method = 'PUT'
        handler.request.headers['Content-Length'] = 100
        await handler.prepare()

        assert handler.auth == handler_auth
        assert handler.provider == MockProvider()
        assert handler.path == WaterButlerPath('/file', prepend=None)

        # check that X-WATERBUTLER-REQUEST-ID is valid UUID
        assert UUID(handler._headers['X-WATERBUTLER-REQUEST-ID'], version=4)

    @pytest.mark.asyncio
    async def test_prepare_stream(self, http_request):

        handler = mock_handler(http_request)
        handler.target_path = WaterButlerPath('/file')
        await handler.prepare_stream()

    @pytest.mark.asyncio
    async def test_head(self, http_request):

        handler = mock_handler(http_request)
        handler.path = WaterButlerPath('/file')
        handler.header_file_metadata = MockCoroutine()

        await handler.head()

        handler.header_file_metadata.assert_called_with()

    @pytest.mark.asyncio
    async def test_get_folder(self, http_request):

        handler = mock_handler(http_request)
        handler.path = WaterButlerPath('/folder/')
        handler.get_folder = MockCoroutine()

        await handler.get()

        handler.get_folder.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_get_file(self, http_request):

        handler = mock_handler(http_request)
        handler.path = WaterButlerPath('/file')
        handler.get_file = MockCoroutine()

        await handler.get()

        handler.get_file.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_put_file(self, http_request):

        handler = mock_handler(http_request)
        handler.target_path = WaterButlerPath('/file')
        handler.upload_file = MockCoroutine()

        await handler.put()

        handler.upload_file.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_put_folder(self, http_request):

        handler = mock_handler(http_request)
        handler.target_path = WaterButlerPath('/folder/')
        handler.create_folder = MockCoroutine()

        await handler.put()

        handler.create_folder.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_delete(self, http_request):

        handler = mock_handler(http_request)
        handler.path = WaterButlerPath('/folder/')
        handler.provider.delete = MockCoroutine()

        await handler.delete()

        handler.provider.delete.assert_called_once_with(WaterButlerPath('/folder/', prepend=None),
                                                        confirm_delete=0)

    @pytest.mark.asyncio
    async def test_delete_confirm_delete(self, http_request):

        handler = mock_handler(http_request)
        handler.path = WaterButlerPath('/folder/')
        handler.provider.delete = MockCoroutine()
        handler.request.query_arguments['confirm_delete'] = '1'

        await handler.delete()

        handler.provider.delete.assert_called_with(WaterButlerPath('/folder/', prepend=None),
                                                   confirm_delete=1)

    @pytest.mark.asyncio
    async def test_data_received(self, http_request):

        handler = mock_handler(http_request)
        handler.path = WaterButlerPath('/folder/')
        handler.stream = None
        handler.body = b''

        await handler.data_received(b'1234567890')

        assert handler.bytes_uploaded == 10
        assert handler.body == b'1234567890'

    @pytest.mark.asyncio
    async def test_data_received_stream(self, http_request):

        handler = mock_handler(http_request)
        handler.path = WaterButlerPath('/folder/')
        handler.stream = MockStream()
        handler.writer = MockWriter()

        await handler.data_received(b'1234567890')

        assert handler.bytes_uploaded == 10
        handler.writer.write.assert_called_once_with(b'1234567890')


class TestProviderHandlerFinish:

    @pytest.mark.asyncio
    async def test_on_finish_download_file(self, http_request):

        handler = mock_handler(http_request)
        handler.request.method = 'GET'
        handler.path = WaterButlerPath('/file')
        handler._send_hook = mock.Mock()

        assert handler.on_finish() is None
        handler._send_hook.assert_called_once_with('download_file')

    @pytest.mark.asyncio
    async def test_on_finish_download_zip(self, http_request):

        handler = mock_handler(http_request)
        handler.request.method = 'GET'
        handler.request.query_arguments['zip'] = ''
        handler.path = WaterButlerPath('/folder/')
        handler._send_hook = mock.Mock()

        assert handler.on_finish() is None
        handler._send_hook.assert_called_once_with('download_zip')

    @pytest.mark.asyncio
    async def test_dont_send_hook_on_file_metadata(self, http_request):

        handler = mock_handler(http_request)
        handler.request.query_arguments['meta'] = ''
        handler.request.method = 'GET'
        handler._send_hook = mock.Mock()

        assert handler.on_finish() is None
        assert not handler._send_hook.called

    @pytest.mark.asyncio
    async def test_dont_send_hook_on_folder_metadata(self, http_request):

        handler = mock_handler(http_request)
        handler.request.method = 'GET'
        handler.path = WaterButlerPath('/folder/')
        handler._send_hook = mock.Mock()

        assert handler.on_finish() is None
        assert not handler._send_hook.called

    @pytest.mark.asyncio
    async def test_dont_send_hook_for_revisions(self, http_request):

        handler = mock_handler(http_request)
        handler.request.query_arguments['revisions'] = ''
        handler.request.method = 'GET'
        handler._send_hook = mock.Mock()

        assert handler.on_finish() is None
        assert not handler._send_hook.called

    @pytest.mark.asyncio
    async def test_on_finish_update(self, http_request):

        handler = mock_handler(http_request)
        handler.request.method = 'PUT'
        handler.path = WaterButlerPath('/file')
        handler._send_hook = mock.Mock()

        assert handler.on_finish() is None
        handler._send_hook.assert_called_once_with('update')

    @pytest.mark.asyncio
    async def test_on_finish_create(self, http_request):

        handler = mock_handler(http_request)
        handler.request.method = 'PUT'
        handler._status_code = 201
        handler.target_path = WaterButlerPath('/file')
        handler._send_hook = mock.Mock()

        assert handler.on_finish() is None
        handler._send_hook.assert_called_once_with('create')

    @pytest.mark.asyncio
    async def test_on_finish_create_folder(self, http_request):

        handler = mock_handler(http_request)
        handler.request.method = 'PUT'
        handler._status_code = 201
        handler.target_path = WaterButlerPath('/folder/')
        handler._send_hook = mock.Mock()

        assert handler.on_finish() is None
        handler._send_hook.assert_called_once_with('create_folder')

    @pytest.mark.asyncio
    async def test_on_finish_move(self, http_request):

        handler = mock_handler(http_request)
        handler.request.method = 'POST'
        handler.body = b'{"action": "rename"}'
        handler.target_path = WaterButlerPath('/folder/')
        handler._send_hook = mock.Mock()

        assert handler.on_finish() is None
        handler._send_hook.assert_called_once_with('move')

    @pytest.mark.asyncio
    async def test_on_finish_copy(self, http_request):

        handler = mock_handler(http_request)
        handler.request.method = 'POST'
        handler.body = b'{"action": "copy"}'
        handler.target_path = WaterButlerPath('/folder/')
        handler._send_hook = mock.Mock()

        assert handler.on_finish() is None
        handler._send_hook.assert_called_once_with('copy')

    @pytest.mark.asyncio
    async def test_on_finish_delete(self, http_request):

        handler = mock_handler(http_request)
        handler.request.method = 'DELETE'
        handler.target_path = WaterButlerPath('/file')
        handler._send_hook = mock.Mock()

        assert handler.on_finish() is None
        handler._send_hook.assert_called_once_with('delete')

    @pytest.mark.asyncio
    @pytest.mark.parametrize('method', [
        ('HEAD'),
        ('OPTIONS'),
    ])
    async def test_dont_send_hook_for_method(self, http_request, method):
        """Not all HTTP methods merit a callback."""

        handler = mock_handler(http_request)
        handler.request.method = method
        handler._send_hook = mock.Mock()

        assert handler.on_finish() is None
        assert not handler._send_hook.called

    @pytest.mark.asyncio
    @pytest.mark.parametrize('status', [
        (202), (206),
        (303),
        (400), (401), (403),
        (500), (501), (502),
    ])
    async def test_dont_send_hook_for_status(self, http_request, status):
        """Callbacks are only called for successful, entirely complete respsonses.  See comments
        in `on_finish` for further explanation."""

        handler = mock_handler(http_request)
        handler._status_code = status
        handler._send_hook = mock.Mock()

        assert handler.on_finish() is None
        assert not handler._send_hook.called

    @pytest.mark.asyncio
    async def test_logging_direct_partial_download_file(self, http_request):
        """For now, make sure partial+direct download requests get logged.  Behaviour may be
        changed in the future."""

        handler = mock_handler(http_request)
        handler.request.method = 'GET'
        handler.path = WaterButlerPath('/file')
        handler._status_code = 302
        handler.request.headers['Range'] = 'fake-range'
        handler._send_hook = mock.Mock()

        assert handler.on_finish() is None
        handler._send_hook.assert_called_once_with('download_file')
