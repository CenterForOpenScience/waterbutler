import json
import logging
from aiohttp import MultiDict

import pytest

from tests.providers.googlecloud.fixtures.providers import mock_auth, mock_creds, mock_settings

from tests.providers.googlecloud.fixtures.files import (file_name, meta_file_extra,
                                                        meta_file_resp_headers_raw,
                                                        file_obj_name, file_wb_path,
                                                        meta_file_raw, meta_file_parsed,)

from tests.providers.googlecloud.fixtures.folders import (folder_name,
                                                          meta_folder_resp_headers_raw,
                                                          folder_obj_name, folder_wb_path,
                                                          meta_folder_raw, meta_folder_parsed, )

from waterbutler.providers.googlecloud import utils
from waterbutler.providers.googlecloud import (BaseGoogleCloudMetadata, GoogleCloudFileMetadata,
                                               GoogleCloudFolderMetadata, GoogleCloudProvider, )

logger = logging.getLogger(__name__)


@pytest.fixture()
def mock_provider(mock_auth, mock_creds, mock_settings):
    return GoogleCloudProvider(mock_auth, mock_creds, mock_settings)


class TestGoogleCloudFileMetadata:

    def test_file_resp_headers(self, file_obj_name, meta_file_raw, meta_file_parsed):

        # Quirks:
        #
        # JSON and Python Dictionary does not support multi-value key.  The real response headers
        # returned by `aiohttp` is of an immutable type `aiohttp._multidict.CIMultiDictProxy`.  For
        # test to work, use `aiohttp.MultiDict` instead for both file and folder tests.
        #
        resp_headers_dict = MultiDict(json.loads(meta_file_raw))
        google_hash = resp_headers_dict.get('x-goog-hash', None)
        assert google_hash and utils.verify_raw_google_hash_header(google_hash)

        resp_headers_dict.pop('x-goog-hash')
        google_hash_list = google_hash.split(',')
        for google_hash in google_hash_list:
            resp_headers_dict.add('x-goog-hash', google_hash)

        metadata_json = BaseGoogleCloudMetadata.get_metadata_from_resp_headers(
            file_obj_name,
            resp_headers_dict
        )
        metadata_json_expected = json.loads(meta_file_parsed)

        assert metadata_json == metadata_json_expected

    def test_folder_resp_headers(self, folder_obj_name, meta_folder_raw, meta_folder_parsed):

        resp_headers_dict = MultiDict(json.loads(meta_folder_raw))
        google_hash = resp_headers_dict.get('x-goog-hash', None)
        assert google_hash and utils.verify_raw_google_hash_header(google_hash)

        resp_headers_dict.pop('x-goog-hash')
        google_hash_list = google_hash.split(',')
        for google_hash in google_hash_list:
            resp_headers_dict.add('x-goog-hash', google_hash)

        metadata_json = BaseGoogleCloudMetadata.get_metadata_from_resp_headers(
            folder_obj_name,
            resp_headers_dict
        )
        metadata_json_expected = json.loads(meta_folder_parsed)

        assert metadata_json == metadata_json_expected

    def test_file_metadata(self, file_name, file_obj_name, meta_file_parsed, meta_file_extra):

        metadata_json = json.loads(meta_file_parsed)
        metadata_extra = json.loads(meta_file_extra)
        metadata = GoogleCloudFileMetadata(metadata_json)

        assert isinstance(metadata, BaseGoogleCloudMetadata)
        assert metadata.provider == 'googlecloud'
        assert metadata.path == '/{}'.format(file_obj_name)
        assert metadata.name == file_name
        assert metadata.kind == 'file'
        assert metadata.content_type == 'text/plain'
        assert metadata.modified == 'Thu, 01 Mar 2018 19:04:45 GMT'
        assert metadata.modified_utc == '2018-03-01T19:04:45+00:00'
        assert metadata.created_utc is None
        assert metadata.etag == '9a46947c9c622d7792125d8ea44c4638'
        assert metadata.size == 85
        assert metadata.extra == dict(metadata_extra)


class TestGoogleCloudFolderMetadata:

    def test_folder_metadata(self, folder_name, folder_obj_name, meta_folder_parsed):

        metadata_json = json.loads(meta_folder_parsed)
        metadata = GoogleCloudFolderMetadata(metadata_json)

        assert isinstance(metadata, BaseGoogleCloudMetadata)
        assert metadata.provider == 'googlecloud'
        assert metadata.path == '/{}'.format(folder_obj_name)
        assert metadata.name == folder_name
        assert metadata.kind == 'folder'
