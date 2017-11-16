import json

import tornado
from unittest import mock

from waterbutler.server.api.v1.provider import ProviderHandler
from waterbutler.core.path import WaterButlerPath
from tests.utils import (HandlerTestCase,
                         MockProvider,
                         MockFileMetadata,
                         MockFolderMetadata,
                         MockCoroutine,
                         MockStream,
                         MockFileRevisionMetadata
                         )

class TestMetadataMixin(HandlerTestCase):

    def setUp(self):
        super().setUp()
        self.resp = tornado.httputil.HTTPServerRequest(
            uri='/v1/resources/test/providers/test/path/mock',
            method='GET')
        self.resp.connection = tornado.http1connection.HTTP1ConnectionParameters()
        self.resp.connection.set_close_callback = mock.Mock()

        self.handler = ProviderHandler(self.get_app(), self.resp)
        self.handler.provider = MockProvider()
        self.handler.path = WaterButlerPath('/test_path')
        self.handler.resource = 'test_resource'
        self.handler.write = mock.Mock()
        self.handler.write_stream = MockCoroutine()


    @tornado.testing.gen_test
    async def test_header_file_metadata(self):

        metadata = MockFileMetadata()
        self.handler.provider.metadata = MockCoroutine(return_value=metadata)

        await self.handler.header_file_metadata()

        assert self.handler._headers['Content-Length'] == '1337'
        assert self.handler._headers['Last-Modified'] == b'Wed, 25 Sep 1991 18:20:30 GMT'
        assert self.handler._headers['Content-Type'] == b'application/octet-stream'
        expected = bytes(json.dumps(metadata.json_api_serialized(self.handler.resource)), 'latin-1')
        assert self.handler._headers['X-Waterbutler-Metadata'] == expected

    @tornado.testing.gen_test
    async def test_get_folder(self):
        # The get_folder method expected behavior is to return folder children's metadata, not the
        # metadata of the actual folder. This should be true of all providers.
        folder_children_metadata = [MockFolderMetadata(), MockFileMetadata(), MockFileMetadata()]

        self.handler.provider.metadata = MockCoroutine(return_value=folder_children_metadata)

        serialized_data = [x.json_api_serialized(self.handler.resource)
                           for x in folder_children_metadata]

        await self.handler.get_folder()

        self.handler.write.assert_called_once_with({'data': serialized_data})

    @tornado.testing.gen_test
    async def test_get_folder_download_as_zip(self):
        # Including 'zip' in the query params should trigger the download_as_zip method

        self.handler.download_folder_as_zip = MockCoroutine()
        self.handler.request.query_arguments['zip'] = ''

        await self.handler.get_folder()

        assert self.handler.download_folder_as_zip.call_count == 1
        # self.handler.download_folder_as_zip.assert_called_once() only works with python 3.6

    @tornado.testing.gen_test
    async def test_get_file_metadata(self):
        self.handler.file_metadata = MockCoroutine()
        self.handler.request.query_arguments['meta'] = ''

        await self.handler.get_file()

        assert self.handler.file_metadata.call_count == 1
        # self.handler.file_metadata.assert_called_once() only works with python 3.6

    @tornado.testing.gen_test
    async def test_get_file_versions(self):
        # Query parameters versions and revisions are equivalent, but versions is preferred for
        # clarity.
        self.handler.get_file_revisions = MockCoroutine()
        self.handler.request.query_arguments['versions'] = ''

        await self.handler.get_file()

        assert self.handler.get_file_revisions.call_count == 1
        # self.handler.get_file_revisions.assert_called_once() only works with python 3.6

        self.handler.request.query_arguments.clear()
        self.handler.get_file_revisions = MockCoroutine()
        self.handler.request.query_arguments['revisions'] = ''

        await self.handler.get_file()

        assert self.handler.get_file_revisions.call_count == 1
        # self.handler.get_file_revisions.assert_called_once() only works with python 3.6

    @tornado.testing.gen_test
    async def test_get_file_download_file(self):

        self.handler.download_file = MockCoroutine()
        await self.handler.get_file()

        assert self.handler.download_file.call_count == 1
        # self.handler.download_file.assert_called_once() only works with python 3.6

    @tornado.testing.gen_test
    async def test_download_file_headers(self):
        stream = MockStream()

        self.handler.provider.download = MockCoroutine(return_value=stream)

        await self.handler.download_file()

        assert self.handler._headers['Content-Length'] == bytes(str(stream.size), 'latin-1')
        assert self.handler._headers['Content-Type'] == bytes(stream.content_type, 'latin-1')
        assert self.handler._headers['Content-Disposition'] == bytes('attachment;filename="{}"'
            .format(self.handler.path.name), 'latin-1')

        assert self.handler.write_stream.call_count == 1
        # self.handler.write_stream.assert_called_once() only works with python 3.6

    @tornado.testing.gen_test
    async def test_download_file_range_request_header(self):
        stream = MockStream()
        stream.partial = True
        stream.content_range = 'bytes=10-100'

        self.handler.provider.download = MockCoroutine(return_value=stream)

        await self.handler.download_file()

        assert self.handler._headers['Content-Range'] == bytes(stream.content_range, 'latin-1')
        assert self.handler.get_status() == 206

        self.handler.write_stream.assert_called_once_with(stream)

    @tornado.testing.gen_test
    async def test_file_metadata(self):
        metadata = MockFileMetadata()
        self.handler.provider.metadata = MockCoroutine(return_value=metadata)

        await self.handler.file_metadata()

        self.handler.write.assert_called_once_with(
            {'data' : metadata.json_api_serialized(self.handler.resource)})

    @tornado.testing.gen_test
    async def test_file_metadata_version(self):
        metadata = MockFileMetadata()
        self.handler.provider.metadata = MockCoroutine(return_value=metadata)
        self.handler.request.query_arguments['version'] = ['version id']

        await self.handler.file_metadata()

        self.handler.provider.metadata.assert_called_once_with(self.handler.path,
                                                               revision='version id')
        self.handler.write.assert_called_once_with(
            {'data' : metadata.json_api_serialized(self.handler.resource)})

    @tornado.testing.gen_test
    async def test_get_file_revisions_raw(self):
        revision_metadata = [MockFileRevisionMetadata()]
        self.handler.provider.revisions = MockCoroutine(return_value=revision_metadata)

        await self.handler.get_file_revisions()

        self.handler.write.assert_called_once_with(
            {'data': [r.json_api_serialized() for r in revision_metadata]})

    @tornado.testing.gen_test
    async def test_download_folder_as_zip(self):

        stream = MockStream()
        self.handler.provider.zip = MockCoroutine(return_value=stream)

        await self.handler.download_folder_as_zip()

        assert self.handler._headers['Content-Type'] == bytes('application/zip', 'latin-1')
        assert self.handler._headers['Content-Disposition'] == bytes('attachment;filename="{}"'
            .format(self.handler.path.name + '.zip'), 'latin-1')

        self.handler.write_stream.assert_called_once_with(stream)