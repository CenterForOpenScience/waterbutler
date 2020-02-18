from unittest import mock

from tests.server.api.v1.utils import mock_handler
from tests.server.api.v1.fixtures import (http_request, mock_exc_info,
                                          mock_exc_info_202, mock_exc_info_http)


class TestBaseHandler:

    def test_write_error(self, http_request, mock_exc_info):

        handler = mock_handler(http_request)
        handler.finish = mock.Mock()
        handler.write_error(500, mock_exc_info)
        handler.finish.assert_called_with({'message': 'OK', 'code': 500})

    def test_write_error_202(self, http_request, mock_exc_info_202):

        handler = mock_handler(http_request)
        handler.finish = mock.Mock()
        handler.write_error(500, mock_exc_info_202)
        handler.finish.assert_called_with()

    @mock.patch('tornado.web.app_log.error')
    def test_log_exception_uncaught(self, mocked_error, http_request, mock_exc_info):

        handler = mock_handler(http_request)
        handler.log_exception(*mock_exc_info)
        mocked_error.assert_called_with('Uncaught exception %s\n',
                                        'GET /v1/resources/test/providers/test/path/mock (None)',
                                        exc_info=mock_exc_info)

    @mock.patch('tornado.web.gen_log.warning')
    def test_log_exception_http_error(self, mocked_warning, http_request, mock_exc_info_http):

        handler = mock_handler(http_request)
        handler.log_exception(*mock_exc_info_http)
        mocked_warning.assert_called_with('%d %s: test http exception',
                                          500,
                                          'GET /v1/resources/test/providers/test/path/mock (None)')
