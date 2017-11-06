import os
import json
import asyncio

import pytz
import tornado.httputil
from dateutil.parser import parse as datetime_parser

from waterbutler.core import mime_types
from waterbutler.server import utils


# TODO split this into metadata.py and data.py
# for getting metadata and the actual files, respectively
class MetadataMixin:

    async def header_file_metadata(self):
        # Going with version as its the most correct term
        # TODO Change all references of revision to version @chrisseto
        # revisions will still be accepted until necessary changes are made to OSF
        version = self.get_query_argument('version', default=None) or self.get_query_argument('revision', default=None)
        data = await self.provider.metadata(self.path, revision=version)

        # Not setting etag for the moment
        # self.set_header('Etag', data.etag)  # This may not be appropriate

        if data.size is not None:
            self.set_header('Content-Length', data.size)

        if data.modified_utc is not None:
            last_modified = datetime_parser(data.modified_utc)
            last_modified_gmt = last_modified.astimezone(pytz.timezone('GMT'))
            self.set_header('Last-Modified', last_modified_gmt.strftime('%a, %d %b %Y %H:%M:%S %Z'))

        self.set_header('Content-Type', data.content_type or 'application/octet-stream')
        self.set_header('X-Waterbutler-Metadata', json.dumps(data.json_api_serialized(self.resource)))

    async def get_folder(self):
        if 'zip' in self.request.query_arguments:
            return (await self.download_folder_as_zip())

        version = (
            self.get_query_argument('version', default=None) or
            self.get_query_argument('revision', default=None)
        )

        data = await self.provider.metadata(self.path, version=version, revision=version)
        return self.write({'data': [x.json_api_serialized(self.resource) for x in data]})

    async def get_file(self):
        if 'meta' in self.request.query_arguments:
            return (await self.file_metadata())

        if 'versions' in self.request.query_arguments or 'revisions' in self.request.query_arguments:
            # Going with versions as its the most correct term
            # TODO Change all references of revision to version @chrisseto
            return (await self.get_file_revisions())

        return (await self.download_file())

    async def download_file(self):
        if 'Range' not in self.request.headers:
            request_range = None
        else:
            # As per RFC 2616 14.16, if an invalid Range header is specified,
            # the request will be treated as if the header didn't exist.
            request_range = tornado.httputil._parse_request_range(self.request.headers['Range'])

        version = self.get_query_argument('version', default=None) or self.get_query_argument('revision', default=None)
        stream = await self.provider.download(
            self.path,
            revision=version,
            range=request_range,
            accept_url='direct' not in self.request.query_arguments,
            mode=self.get_query_argument('mode', default=None),
        )

        if isinstance(stream, str):
            return self.redirect(stream)

        size = stream.size
        if request_range:
            start, end = request_range
            if (start is not None and start >= size) or end == 0:
                # As per RFC 2616 14.35.1, a range is not satisfiable only: if
                # the first requested byte is equal to or greater than the
                # content, or when a suffix with length 0 is specified
                self.set_status(416)  # Range Not Satisfiable
                self.set_header("Content-Type", "text/plain")
                self.set_header("Content-Range", "bytes */%s" % (size,))
                return
            if start is not None and start < 0:
                start += size

            if end is not None and end > size:
                # Clients sometimes blindly use a large range to limit their
                # download size; cap the endpoint at the actual file size.
                end = size
            # Note: only return HTTP 206 if less than the entire range has been
            # requested. Not only is this semantically correct, but Chrome
            # refuses to play audio if it gets an HTTP 206 in response to
            # ``Range: bytes=0-``.
            if size != (end or size) - (start or 0):
                self.set_status(206)  # Partial Content
                self.set_header("Content-Range",
                                tornado.httputil._get_content_range(start, end, size))
        else:
            start = end = None

        if start is not None and end is not None:
            content_length = end - start
        elif end is not None:
            content_length = end
        elif start is not None:
            content_length = size - start
        else:
            content_length = size

        self.set_header("Content-Length", content_length)

        # Build `Content-Disposition` header from `displayName` override,
        # headers of provider response, or file path, whichever is truthy first
        name = self.get_query_argument('displayName', default=None) or getattr(stream, 'name', None) or self.path.name
        self.set_header('Content-Disposition', utils.make_disposition(name))

        _, ext = os.path.splitext(name)
        # If the file extention is in mime_types
        # override the content type to fix issues with safari shoving in new file extensions
        if ext in mime_types:
            self.set_header('Content-Type', mime_types[ext])

        await self.write_stream(stream, request_range)

    async def file_metadata(self):
        version = self.get_query_argument('version', default=None) or self.get_query_argument('revision', default=None)

        return self.write({
            'data': (await self.provider.metadata(self.path, revision=version)).json_api_serialized(self.resource)
        })

    async def get_file_revisions(self):
        result = self.provider.revisions(self.path)

        if asyncio.iscoroutine(result):
            result = await result

        return self.write({'data': [r.json_api_serialized() for r in result]})

    async def download_folder_as_zip(self):
        zipfile_name = self.path.name or '{}-archive'.format(self.provider.NAME)
        self.set_header('Content-Type', 'application/zip')
        self.set_header(
            'Content-Disposition',
            utils.make_disposition(zipfile_name + '.zip')
        )

        result = await self.provider.zip(self.path)

        await self.write_stream(result)
