import os
import json
import asyncio

import tornado.httputil

from waterbutler.core import mime_types
from waterbutler.server import utils


# TODO split this into metadata.py and data.py
# for getting metadata and the actual files, respectively
class MetadataMixin:

    @asyncio.coroutine
    def header_file_metadata(self):
        # Going with version as its the most correct term
        # TODO Change all references of revision to version @chrisseto
        # revisions will still be accepted until necessary changes are made to OSF
        version = self.get_query_argument('version', default=None) or self.get_query_argument('revision', default=None)
        data = yield from self.provider.metadata(self.path, revision=version)

        # Not setting etag for the moment
        # self.set_header('Etag', data.etag)  # This may not be appropriate
        if data.size is not None:
            self.set_header('Content-Length', data.size)
        if data.modified is not None:
            self.set_header('Last-Modified', data.modified)
        self.set_header('Content-Type', data.content_type or 'application/octet-stream')
        self.set_header('X-Waterbutler-Metadata', json.dumps(data.json_api_serialized(self.resource)))

    @asyncio.coroutine
    def get_folder(self):
        if 'zip' in self.request.query_arguments:
            return (yield from self.download_folder_as_zip())

        data = yield from self.provider.metadata(self.path)
        return self.write({'data': [x.json_api_serialized(self.resource) for x in data]})

    @asyncio.coroutine
    def get_file(self):
        if 'meta' in self.request.query_arguments:
            return (yield from self.file_metadata())

        if 'versions' in self.request.query_arguments or 'revisions' in self.request.query_arguments:
            # Going with versions as its the most correct term
            # TODO Change all references of revision to version @chrisseto
            return (yield from self.get_file_revisions())

        return (yield from self.download_file())

    @asyncio.coroutine
    def download_file(self):
        if 'Range' not in self.request.headers:
            request_range = None
        else:
            request_range = tornado.httputil._parse_request_range(self.request.headers['Range'])

        version = self.get_query_argument('version', default=None) or self.get_query_argument('revision', default=None)
        stream = yield from self.provider.download(
            self.path,
            revision=version,
            range=request_range,
            accept_url='direct' not in self.request.query_arguments,
            mode=self.get_query_argument('mode', default=None),
        )

        if isinstance(stream, str):
            return self.redirect(stream)

        if getattr(stream, 'partial', None):
            # Use getattr here as not all stream may have a partial attribute
            # Plus it fixes tests
            self.set_status(206)
            self.set_header('Content-Range', stream.content_range)

        if stream.content_type is not None:
            self.set_header('Content-Type', stream.content_type)

        if stream.size is not None:
            self.set_header('Content-Length', str(stream.size))

        # Build `Content-Disposition` header from `displayName` override,
        # headers of provider response, or file path, whichever is truthy first
        name = self.get_query_argument('displayName', default=None) or getattr(stream, 'name', None) or self.path.name
        self.set_header('Content-Disposition', utils.make_disposition(name))

        _, ext = os.path.splitext(name)
        # If the file extention is in mime_types
        # override the content type to fix issues with safari shoving in new file extensions
        if ext in mime_types:
            self.set_header('Content-Type', mime_types[ext])

        yield self.write_stream(stream)

    @asyncio.coroutine
    def file_metadata(self):
        version = self.get_query_argument('version', default=None) or self.get_query_argument('revision', default=None)

        return self.write({
            'data': (yield from self.provider.metadata(self.path, revision=version)).json_api_serialized(self.resource)
        })

    @asyncio.coroutine
    def get_file_revisions(self):
        result = self.provider.revisions(self.path)

        if asyncio.iscoroutine(result):
            result = yield from result

        return self.write({'data': [r.json_api_serialized() for r in result]})

    @asyncio.coroutine
    def download_folder_as_zip(self):
        self.set_header('Content-Type', 'application/zip')
        self.set_header(
            'Content-Disposition',
            utils.make_disposition((self.path.name or 'download') + '.zip')
        )

        result = yield from self.provider.zip(self.path)

        yield self.write_stream(result)
