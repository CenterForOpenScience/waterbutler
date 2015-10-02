import pytest
import hashlib

from tests import utils
from unittest import mock
from tests.utils import async
from waterbutler.core import metadata
from waterbutler.core import exceptions


class TestBaseMetadata:

    def test_file_metadata(self):
        file_metadata = utils.MockFileMetadata()

        assert file_metadata.is_file == True
        assert file_metadata.is_folder == False

    def test_folder_metadata(self):
        folder_metadata = utils.MockFolderMetadata()

        assert folder_metadata.is_folder == True
        assert folder_metadata.is_file == False

    def test_file_json_api_serialize(self):
        file_metadata = utils.MockFileMetadata()
        serialized = file_metadata.json_api_serialized('n0d3z')
        link_suffix = '/v1/resources/n0d3z/providers/mock/Foo.name'
        etag = hashlib.sha256('{}::{}'.format('mock', 'etag').encode('utf-8')).hexdigest()

        assert serialized['id'] == 'mock/Foo.name'
        assert serialized['type'] == 'files'
        assert serialized['attributes'] == {
            'extra': {},
            'kind': 'file',
            'name': 'Foo.name',
            'path': '/Foo.name',
            'provider': 'mock',
            'materialized': '/Foo.name',
            'etag': etag,
            'contentType': 'application/octet-stream',
            'modified': 'never',
            'size': 1337,
        }
        assert serialized['links']['new_folder'] == None
        assert serialized['links']['move'].endswith(link_suffix)
        assert serialized['links']['upload'].endswith(link_suffix)
        assert serialized['links']['download'].endswith(link_suffix)
        assert serialized['links']['delete'].endswith(link_suffix)

    def test_folder_json_api_serialize(self):
        folder_metadata = utils.MockFolderMetadata()
        serialized = folder_metadata.json_api_serialized('n0d3z')
        link_suffix = '/v1/resources/n0d3z/providers/mock/Bar/'
        etag = hashlib.sha256('{}::{}'.format('mock', 'etag').encode('utf-8')).hexdigest()

        assert serialized['id'] == 'mock/Bar/'
        assert serialized['type'] == 'files'
        assert serialized['attributes'] == {
            'extra': {},
            'kind': 'folder',
            'name': 'Bar',
            'path': '/Bar/',
            'provider': 'mock',
            'materialized': '/Bar/',
            'etag': etag,
            'size': None,
        }
        assert serialized['links']['new_folder'].endswith(link_suffix)
        assert serialized['links']['move'].endswith(link_suffix)
        assert serialized['links']['upload'].endswith(link_suffix)
        assert serialized['links']['download'] == None
        assert serialized['links']['delete'].endswith(link_suffix)

    def test_folder_json_api_serialize(self):
        folder_metadata = utils.MockFolderMetadata()
        folder_metadata.children = [utils.MockFileMetadata()]
        serialized = folder_metadata.json_api_serialized('n0d3z')
        child = serialized['attributes']['children'][0]
        etag = hashlib.sha256('{}::{}'.format('mock', 'etag').encode('utf-8')).hexdigest()

        assert len(serialized['attributes']['children']) == 1
        assert child == {
            'extra': {},
            'kind': 'file',
            'name': 'Foo.name',
            'path': '/Foo.name',
            'provider': 'mock',
            'materialized': '/Foo.name',
            'etag': etag,
            'contentType': 'application/octet-stream',
            'modified': 'never',
            'size': 1337,
        }
