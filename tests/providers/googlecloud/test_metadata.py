import json

import pytest
from multidict import MultiDict

from tests.providers.googlecloud.fixtures.files import (file_name,
                                                        file_wb_path,
                                                        file_obj_name,
                                                        meta_file_raw,
                                                        meta_file_extra,
                                                        meta_file_parsed,
                                                        meta_file_resp_headers_raw)
from tests.providers.googlecloud.fixtures.folders import (folder_name,
                                                          folder_wb_path,
                                                          folder_obj_name,
                                                          meta_folder_raw,
                                                          meta_folder_parsed,
                                                          meta_folder_resp_headers_raw)

from waterbutler.core import exceptions
from waterbutler.providers.googlecloud import utils
from waterbutler.providers.googlecloud.metadata import (BaseGoogleCloudMetadata,
                                                        GoogleCloudFileMetadata,
                                                        GoogleCloudFolderMetadata)



class TestMetadataInitialization:

    def test_metadata_from_dict(self, meta_file_parsed):

        resp_headers = dict(json.loads(meta_file_parsed))
        metadata = GoogleCloudFileMetadata(resp_headers)

        assert metadata
        assert metadata.etag == '9a46947c9c622d7792125d8ea44c4638'

    def test_metadata_from_resp_headers(self, file_obj_name, meta_file_raw):

        resp_headers = utils.get_multi_dict_from_python_dict(dict(json.loads(meta_file_raw)))
        metadata = GoogleCloudFileMetadata.new_from_resp_headers(file_obj_name, resp_headers)

        assert metadata
        assert metadata.etag == '9a46947c9c622d7792125d8ea44c4638'

    def test_metadata_from_resp_headers_missing_object_name(self, meta_file_raw):

        resp_headers = utils.get_multi_dict_from_python_dict(dict(json.loads(meta_file_raw)))

        with pytest.raises(exceptions.MetadataError):
            GoogleCloudFileMetadata.new_from_resp_headers('', resp_headers)

    def test_metadata_from_resp_headers_invalid_resp_headers(self, file_obj_name, meta_file_raw):

        resp_headers = json.loads(meta_file_raw)

        with pytest.raises(exceptions.MetadataError):
            GoogleCloudFileMetadata.new_from_resp_headers(file_obj_name, resp_headers)

    def test_metadata_from_resp_headers_missing_resp_headers(self, file_obj_name):

        with pytest.raises(exceptions.MetadataError):
            GoogleCloudFileMetadata.new_from_resp_headers(file_obj_name, MultiDict({}))


class TestGoogleCloudFileMetadata:

    def test_file_resp_headers(self, file_obj_name, meta_file_raw, meta_file_parsed):

        resp_headers = utils.get_multi_dict_from_python_dict(dict(json.loads(meta_file_raw)))
        metadata_json = BaseGoogleCloudMetadata.get_metadata_from_resp_headers(
            file_obj_name,
            resp_headers
        )
        metadata_json_expected = json.loads(meta_file_parsed)

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
        assert metadata.size_as_int == 85
        assert metadata.extra == dict(metadata_extra)


class TestGoogleCloudFolderMetadata:

    def test_folder_resp_headers(self, folder_obj_name, meta_folder_raw, meta_folder_parsed):

        resp_headers = utils.get_multi_dict_from_python_dict(dict(json.loads(meta_folder_raw)))
        metadata_json = BaseGoogleCloudMetadata.get_metadata_from_resp_headers(
            folder_obj_name,
            resp_headers
        )
        metadata_json_expected = json.loads(meta_folder_parsed)

        assert metadata_json == metadata_json_expected

    def test_folder_metadata(self, folder_name, folder_obj_name, meta_folder_parsed):

        metadata_json = json.loads(meta_folder_parsed)
        metadata = GoogleCloudFolderMetadata(metadata_json)

        assert isinstance(metadata, BaseGoogleCloudMetadata)
        assert metadata.provider == 'googlecloud'
        assert metadata.path == '/{}'.format(folder_obj_name)
        assert metadata.name == folder_name
        assert metadata.kind == 'folder'
