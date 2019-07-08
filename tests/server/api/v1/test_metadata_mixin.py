import json

import pytest

from tests.utils import MockCoroutine
from waterbutler.core.path import WaterButlerPath
from tests.server.api.v1.fixtures import (http_request, handler, handler_auth, mock_stream,
                                          mock_partial_stream, mock_file_metadata,
                                          mock_folder_children, mock_revision_metadata)


class TestMetadataMixin:

    @pytest.mark.asyncio
    async def test_header_file_metadata(self, handler, mock_file_metadata):

        handler.provider.metadata = MockCoroutine(return_value=mock_file_metadata)

        await handler.header_file_metadata()

        assert handler._headers['Content-Length'] == '1337'
        assert handler._headers['Last-Modified'] == b'Wed, 25 Sep 1991 18:20:30 GMT'
        assert handler._headers['Content-Type'] == b'application/octet-stream'
        expected = bytes(json.dumps(mock_file_metadata.json_api_serialized(handler.resource)),
                         'latin-1')
        assert handler._headers['X-Waterbutler-Metadata'] == expected

    @pytest.mark.asyncio
    async def test_get_folder(self, handler, mock_folder_children):
        # The get_folder method expected behavior is to return folder children's metadata, not the
        # metadata of the actual folder. This should be true of all providers.

        handler.provider.metadata = MockCoroutine(return_value=mock_folder_children)

        serialized_data = [x.json_api_serialized(handler.resource) for x in mock_folder_children]

        await handler.get_folder()

        handler.write.assert_called_once_with({'data': serialized_data})

    @pytest.mark.asyncio
    async def test_get_folder_download_as_zip(self, handler):
        # Including 'zip' in the query params should trigger the download_as_zip method

        handler.download_folder_as_zip = MockCoroutine()
        handler.request.query_arguments['zip'] = ''

        await handler.get_folder()

        handler.download_folder_as_zip.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_get_file_metadata(self, handler):
        handler.file_metadata = MockCoroutine()
        handler.request.query_arguments['meta'] = ''

        await handler.get_file()

        handler.file_metadata.assert_awaited_once()

    @pytest.mark.asyncio
    @pytest.mark.parametrize('query_param', ['versions', 'revisions'])
    async def test_get_file_versions(self, query_param, handler):
        # Query parameters versions and revisions are equivalent, but versions is preferred for
        # clarity.
        handler.get_file_revisions = MockCoroutine()
        handler.request.query_arguments[query_param] = ''

        await handler.get_file()

        handler.get_file_revisions.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_get_file_download_file(self, handler):

        handler.download_file = MockCoroutine()
        await handler.get_file()

        handler.download_file.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_download_file_headers(self, handler, mock_stream):

        mock_stream.name = 'peanut-butter.docx'
        handler.provider.download = MockCoroutine(return_value=mock_stream)
        handler.path = WaterButlerPath('/test_file')

        await handler.download_file()

        assert handler._headers['Content-Length'] == bytes(str(mock_stream.size), 'latin-1')
        assert handler._headers['Content-Type'] == bytes(mock_stream.content_type, 'latin-1')
        disposition = b'attachment; filename="peanut-butter.docx"; filename*=UTF-8\'\'peanut-butter.docx'
        assert handler._headers['Content-Disposition'] == disposition

        handler.write_stream.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_download_file_headers_no_stream_name(self, handler, mock_stream):

        handler.provider.download = MockCoroutine(return_value=mock_stream)
        handler.path = WaterButlerPath('/test_file')

        await handler.download_file()

        assert handler._headers['Content-Length'] == bytes(str(mock_stream.size), 'latin-1')
        assert handler._headers['Content-Type'] == bytes(mock_stream.content_type, 'latin-1')
        disposition = b'attachment; filename="test_file"; filename*=UTF-8\'\'test_file'
        assert handler._headers['Content-Disposition'] == disposition

        handler.write_stream.assert_awaited_once()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("given_arg,expected_name,filtered_name", [
        (['résumé.doc'], b'r%C3%A9sum%C3%A9.doc', b'resume.doc'),
        ([''],           b'test_file',            b'test_file'),
        ([],             b'test_file',            b'test_file'),
    ])
    async def test_download_file_with_display_name(self, handler, mock_stream, given_arg,
                                                   expected_name, filtered_name):

        handler.provider.download = MockCoroutine(return_value=mock_stream)
        handler.path = WaterButlerPath('/test_file')
        handler.request.query_arguments['displayName'] = given_arg

        await handler.download_file()

        assert handler._headers['Content-Length'] == bytes(str(mock_stream.size), 'latin-1')
        assert handler._headers['Content-Type'] == bytes(mock_stream.content_type, 'latin-1')
        disposition = b'attachment; filename="' + filtered_name + b'"; filename*=UTF-8\'\'' + expected_name
        assert handler._headers['Content-Disposition'] == disposition

        handler.write_stream.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_download_file_range_request_header(self, handler, mock_partial_stream):

        handler.request.headers['Range'] = 'bytes=10-100'
        handler.provider.download = MockCoroutine(return_value=mock_partial_stream)
        handler.path = WaterButlerPath('/test_file')

        await handler.download_file()

        assert handler._headers['Content-Range'] == bytes(mock_partial_stream.content_range,
                                                          'latin-1')
        assert handler.get_status() == 206
        handler.write_stream.assert_called_once_with(mock_partial_stream)

    @pytest.mark.asyncio
    async def test_download_file_stream_redirect(self, handler):

        handler.provider.download = MockCoroutine(return_value='stream')
        await handler.download_file()

        handler.redirect.assert_called_once_with('stream')

    @pytest.mark.asyncio
    @pytest.mark.parametrize("extension, mimetype", [
        ('.csv', 'text/csv'),
        ('.md', 'text/x-markdown')
    ])
    async def test_download_file_safari_mime_type(self, extension, mimetype, handler, mock_stream):
        """ If the file extention is in mime_types override the content type to fix issues with
        safari shoving in new file extensions """

        handler.path = WaterButlerPath('/test_path.{}'.format(extension))
        handler.provider.download = MockCoroutine(return_value=mock_stream)

        await handler.download_file()

        handler.write_stream.assert_called_once_with(mock_stream)
        assert handler._headers['Content-Type'] == bytes(mimetype, 'latin-1')

    @pytest.mark.asyncio
    async def test_file_metadata(self, handler, mock_file_metadata):

        handler.provider.metadata = MockCoroutine(return_value=mock_file_metadata)

        await handler.file_metadata()

        handler.write.assert_called_once_with({
            'data': mock_file_metadata.json_api_serialized(handler.resource)
        })

    @pytest.mark.asyncio
    async def test_file_metadata_version(self, handler, mock_file_metadata):
        handler.provider.metadata = MockCoroutine(return_value=mock_file_metadata)
        handler.requested_version = 'version id'

        await handler.file_metadata()

        handler.provider.metadata.assert_called_once_with(handler.path, revision='version id')
        handler.write.assert_called_once_with({
            'data': mock_file_metadata.json_api_serialized(handler.resource)
        })

    @pytest.mark.asyncio
    async def test_get_file_revisions_raw(self, handler, mock_revision_metadata):
        handler.provider.revisions = MockCoroutine(return_value=mock_revision_metadata)

        await handler.get_file_revisions()

        handler.write.assert_called_once_with({
            'data': [r.json_api_serialized() for r in mock_revision_metadata]
        })

    @pytest.mark.asyncio
    async def test_download_folder_as_zip(self, handler, mock_stream):

        handler.provider.zip = MockCoroutine(return_value=mock_stream)
        handler.path = WaterButlerPath('/test_file')

        await handler.download_folder_as_zip()

        assert handler._headers['Content-Type'] == bytes('application/zip', 'latin-1')
        expected = b'attachment; filename="test_file.zip"; filename*=UTF-8\'\'test_file.zip'
        assert handler._headers['Content-Disposition'] == expected

        handler.write_stream.assert_called_once_with(mock_stream)

    @pytest.mark.asyncio
    async def test_download_folder_as_zip_root(self, handler, mock_stream):

        handler.provider.zip = MockCoroutine(return_value=mock_stream)
        handler.path = WaterButlerPath('/')

        await handler.download_folder_as_zip()

        assert handler._headers['Content-Type'] == bytes('application/zip', 'latin-1')
        expected = b'attachment; filename="MockProvider-archive.zip"; filename*=UTF-8\'\'MockProvider-archive.zip'
        assert handler._headers['Content-Disposition'] == expected

        handler.write_stream.assert_called_once_with(mock_stream)
