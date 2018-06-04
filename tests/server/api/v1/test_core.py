from unittest import mock

from tests.server.api.v1.fixtures import (http_request, handler, mock_exc_info, mock_exc_info_202,
                                          mock_exc_info_http)


class TestBaseHandler:

    def test_write_error(self, handler, mock_exc_info):
        handler.finish = mock.Mock()
        handler.captureException = mock.Mock()

        handler.write_error(500, mock_exc_info)

        handler.finish.assert_called_with({'message': 'OK', 'code': 500})
        handler.captureException.assert_called_with(mock_exc_info)

    def test_write_error_202(self, handler, mock_exc_info_202):
        handler.finish = mock.Mock()
        handler.captureException = mock.Mock()

        handler.write_error(500, mock_exc_info_202)

        handler.finish.assert_called_with()
        handler.captureException.assert_called_with(mock_exc_info_202, data={'level': 'info'})

    @mock.patch('tornado.web.app_log.error')
    def test_log_exception_uncaught(self, mocked_error, handler, mock_exc_info):

        handler.log_exception(*mock_exc_info)

        mocked_error.assert_called_with('Uncaught exception %s\n',
                                        'GET /v1/resources/test/providers/test/path/mock (None)',
                                        exc_info=mock_exc_info)

    @mock.patch('tornado.web.gen_log.warning')
    def test_log_exception_http_error(self, mocked_warning, handler, mock_exc_info_http):
        handler.log_exception(*mock_exc_info_http)

        mocked_warning.assert_called_with('%d %s: test http exception',
                                          500,
                                          'GET /v1/resources/test/providers/test/path/mock (None)')
