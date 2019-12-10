from http import client
from unittest import mock

import pytest

from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from tests.utils import MockCoroutine
from tests.server.api.v1.utils import mock_handler
from tests.server.api.v1.fixtures import (http_request, handler_auth,
                                          mock_folder_metadata, mock_file_metadata)


class TestValidatePut:

    @pytest.mark.asyncio
    async def test_postvalidate_put_file(self, http_request):

        handler = mock_handler(http_request)
        handler.path = WaterButlerPath('/file')
        handler.kind = 'file'
        handler.get_query_argument = mock.Mock(return_value=None)

        await handler.postvalidate_put()

        assert handler.target_path == handler.path
        handler.get_query_argument.assert_called_once_with('name', default=None)

    @pytest.mark.asyncio
    async def test_postvalidate_put_folder(self, http_request):

        handler = mock_handler(http_request)
        handler.path = WaterButlerPath('/Folder1/')
        handler.kind = 'folder'
        handler.get_query_argument = mock.Mock(return_value='child!')
        handler.provider.exists = MockCoroutine(return_value=False)
        handler.provider.can_duplicate_names = mock.Mock(return_value=False)

        await handler.postvalidate_put()

        assert handler.target_path == WaterButlerPath('/Folder1/child!/')
        handler.get_query_argument.assert_called_once_with('name', default=None)

        handler.provider.exists.assert_has_calls([
            mock.call(WaterButlerPath('/Folder1/child!', prepend=None)),
            mock.call(WaterButlerPath('/Folder1/child!', prepend=None))
        ])

    @pytest.mark.asyncio
    async def test_postvalidate_put_folder_naming_conflict(self, http_request):

        handler = mock_handler(http_request)
        handler.path = WaterButlerPath('/Folder1/')
        handler.kind = 'folder'
        handler.get_query_argument = mock.Mock(return_value='child!')
        handler.provider.exists = MockCoroutine(return_value=True)

        with pytest.raises(exceptions.NamingConflict) as exc:
            await handler.postvalidate_put()

        assert exc.value.message == 'Cannot complete action: file or folder "child!" already ' \
                                    'exists in this location'

        assert handler.target_path == WaterButlerPath('/Folder1/child!/')
        handler.get_query_argument.assert_called_once_with('name', default=None)
        handler.provider.exists.assert_called_once_with(
            WaterButlerPath('/Folder1/child!', prepend=None))

    @pytest.mark.asyncio
    async def test_postvalidate_put_cant_duplicate_names(self, http_request):

        handler = mock_handler(http_request)
        handler.path = WaterButlerPath('/Folder1/')
        handler.kind = 'folder'
        handler.provider.can_duplicate_names = mock.Mock(return_value=False)
        handler.get_query_argument = mock.Mock(return_value='child!')
        handler.provider.exists = MockCoroutine(return_value=False)

        await handler.postvalidate_put()

        assert handler.target_path == WaterButlerPath('/Folder1/child!/')
        handler.get_query_argument.assert_called_once_with('name', default=None)
        handler.provider.exists.assert_called_with(WaterButlerPath('/Folder1/child!', prepend=None))
        handler.provider.can_duplicate_names.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_postvalidate_put_cant_duplicate_names_and_naming_conflict(self, http_request):

        handler = mock_handler(http_request)
        handler.path = WaterButlerPath('/Folder1/')
        handler.kind = 'folder'
        handler.provider.can_duplicate_names = mock.Mock(return_value=False)
        handler.get_query_argument = mock.Mock(return_value='child!')
        handler.provider.exists = MockCoroutine(side_effect=[False, True])

        with pytest.raises(exceptions.NamingConflict) as exc:
            await handler.postvalidate_put()

        assert exc.value.message == 'Cannot complete action: file or folder "child!" already ' \
                                    'exists in this location'

        handler.provider.can_duplicate_names.assert_called_once_with()
        handler.get_query_argument.assert_called_once_with('name', default=None)
        handler.provider.exists.assert_called_with(
            WaterButlerPath('/Folder1/child!', prepend=None))

    def test_invalid_kind(self, http_request):

        handler = mock_handler(http_request)
        handler.get_query_argument = mock.Mock(return_value='notafolder')

        with pytest.raises(exceptions.InvalidParameters) as exc:
            handler.prevalidate_put()

        handler.get_query_argument.assert_called_once_with('kind', default='file')
        assert exc.value.message == 'Kind must be file, folder or unspecified (interpreted as ' \
                                    'file), not notafolder'

    def test_default_kind(self, http_request):

        handler = mock_handler(http_request)
        handler.get_query_argument = mock.Mock(return_value='file')
        handler.request.headers.get = mock.Mock(side_effect=Exception('Breakout'))

        with pytest.raises(Exception) as exc:
            handler.prevalidate_put()

        assert handler.kind == 'file'
        assert exc.value.args == ('Breakout', )
        handler.get_query_argument.assert_called_once_with('kind', default='file')
        handler.request.headers.get.assert_called_once_with('Content-Length')

    def test_length_required_for_files(self, http_request):

        handler = mock_handler(http_request)
        handler.request.headers = {}
        handler.get_query_argument = mock.Mock(return_value='file')

        with pytest.raises(exceptions.InvalidParameters) as exc:
            handler.prevalidate_put()

        assert exc.value.code == client.LENGTH_REQUIRED
        assert exc.value.message == 'Content-Length is required for file uploads'
        handler.get_query_argument.assert_called_once_with('kind', default='file')

    def test_payload_with_folder(self, http_request):

        handler = mock_handler(http_request)
        handler.request.headers = {'Content-Length': 5000}
        handler.get_query_argument = mock.Mock(return_value='folder')

        with pytest.raises(exceptions.InvalidParameters) as exc:
            handler.prevalidate_put()

        assert exc.value.code == client.REQUEST_ENTITY_TOO_LARGE
        assert exc.value.message == 'Folder creation requests may not have a body'
        handler.get_query_argument.assert_called_once_with('kind', default='file')

    def test_payload_with_invalid_content_length(self, http_request):

        handler = mock_handler(http_request)
        handler.request.headers = {'Content-Length': 'notanumber'}
        handler.get_query_argument = mock.Mock(return_value='file')

        with pytest.raises(exceptions.InvalidParameters) as exc:
            handler.prevalidate_put()

        assert exc.value.code == client.BAD_REQUEST
        assert exc.value.message == 'Invalid Content-Length'
        handler.get_query_argument.assert_called_once_with('kind', default='file')

    @pytest.mark.asyncio
    async def test_name_required_for_dir(self, http_request):

        handler = mock_handler(http_request)
        handler.path = WaterButlerPath('/', folder=True)
        handler.get_query_argument = mock.Mock(return_value=None)

        with pytest.raises(exceptions.InvalidParameters) as exc:
            await handler.postvalidate_put()

        assert exc.value.message == 'Missing required parameter \'name\''
        handler.get_query_argument.assert_called_once_with('name', default=None)

    @pytest.mark.asyncio
    async def test_name_refused_for_file(self, http_request):

        handler = mock_handler(http_request)
        handler.path = WaterButlerPath('/foo.txt', folder=False)
        handler.get_query_argument = mock.Mock(return_value='bar.txt')

        with pytest.raises(exceptions.InvalidParameters) as exc:
            await handler.postvalidate_put()

        assert exc.value.message == "'name' parameter doesn't apply to actions on files"
        handler.get_query_argument.assert_called_once_with('name', default=None)

    @pytest.mark.asyncio
    async def test_kind_must_be_folder(self, http_request):

        handler = mock_handler(http_request)
        handler.path = WaterButlerPath('/adlkjf')
        handler.get_query_argument = mock.Mock(return_value=None)
        handler.kind = 'folder'

        with pytest.raises(exceptions.InvalidParameters) as exc:
            await handler.postvalidate_put()

        assert exc.value.message == 'Path must be a folder (and end with a "/") if trying to ' \
                                    'create a subfolder'
        assert exc.value.code == client.CONFLICT
        handler.get_query_argument.assert_called_once_with('name', default=None)


class TestCreateFolder:

    @pytest.mark.asyncio
    async def test_create_folder(self, http_request, mock_folder_metadata):

        handler = mock_handler(http_request)
        handler.resource = '3rqws'
        handler.provider.create_folder = MockCoroutine(return_value=mock_folder_metadata)
        handler.target_path = WaterButlerPath('/apath/')
        handler.set_status = mock.Mock()

        await handler.create_folder()

        handler.set_status.assert_called_once_with(201)
        handler.write.assert_called_once_with({
            'data': mock_folder_metadata.json_api_serialized('3rqws')
        })
        handler.provider.create_folder.assert_called_once_with(WaterButlerPath('/apath/'))


class TestUploadFile:

    @pytest.mark.asyncio
    async def test_created(self, http_request, mock_file_metadata):

        handler = mock_handler(http_request)
        handler.resource = '3rqws'
        handler.uploader.set_result((mock_file_metadata, True))
        handler.set_status = mock.Mock()

        await handler.upload_file()

        assert handler.wsock.close.called
        assert handler.writer.close.called
        handler.set_status.assert_called_once_with(201)
        handler.write.assert_called_once_with({
            'data': mock_file_metadata.json_api_serialized('3rqws')
        })

    @pytest.mark.asyncio
    async def test_not_created(self, http_request, mock_file_metadata):

        handler = mock_handler(http_request)
        handler.resource = '3rqws'
        handler.uploader.set_result((mock_file_metadata, False))
        handler.set_status = mock.Mock()

        await handler.upload_file()

        assert handler.wsock.close.called
        assert handler.writer.close.called
        assert handler.set_status.called is False
        handler.write.assert_called_once_with({
            'data': mock_file_metadata.json_api_serialized('3rqws')
        })
