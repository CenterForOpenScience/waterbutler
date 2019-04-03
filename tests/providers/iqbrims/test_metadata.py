import pytest

import os

from waterbutler.providers.iqbrims.provider import IQBRIMSPath
from waterbutler.providers.iqbrims.provider import IQBRIMSPathPart
from waterbutler.providers.iqbrims.metadata import IQBRIMSRevision
from waterbutler.providers.iqbrims.metadata import IQBRIMSFileMetadata
from waterbutler.providers.iqbrims.metadata import IQBRIMSFolderMetadata

from tests.providers.iqbrims.fixtures import(
    error_fixtures,
    root_provider_fixtures,
    revision_fixtures,
    sharing_fixtures,
)


@pytest.fixture
def basepath():
    return IQBRIMSPath('/conrad')


class TestMetadata:

    def test_file_metadata_drive(self, basepath, root_provider_fixtures):
        item = root_provider_fixtures['list_file']['items'][0]
        path = basepath.child(item['title'])
        parsed = IQBRIMSFileMetadata(item, path)

        assert parsed.provider == 'iqbrims'
        assert parsed.id == item['id']
        assert path.name == item['title']
        assert parsed.name == item['title']
        assert parsed.size == item['fileSize']
        assert parsed.modified == item['modifiedDate']
        assert parsed.content_type == item['mimeType']
        assert parsed.extra == {
            'revisionId': item['version'],
            'webView': item['alternateLink'],
            'hashes': {'md5': item['md5Checksum']},
        }
        assert parsed.path == '/' + os.path.join(*[x.raw for x in path.parts])
        assert parsed.materialized_path == str(path)
        assert parsed.is_iqbrims_doc is False
        assert parsed.export_name == item['title']

    def test_file_metadata_drive_slashes(self, basepath, root_provider_fixtures):
        item = root_provider_fixtures['file_forward_slash']
        path = basepath.child(item['title'])
        parsed = IQBRIMSFileMetadata(item, path)

        assert parsed.provider == 'iqbrims'
        assert parsed.id == item['id']
        assert parsed.name == item['title']
        assert parsed.name == path.name
        assert parsed.size == item['fileSize']
        assert parsed.modified == item['modifiedDate']
        assert parsed.content_type == item['mimeType']
        assert parsed.extra == {
            'revisionId': item['version'],
            'webView': item['alternateLink'],
            'hashes': {'md5': item['md5Checksum']},
        }
        assert parsed.path == '/' + os.path.join(*[x.raw for x in path.parts])
        assert parsed.materialized_path == str(path)
        assert parsed.is_iqbrims_doc is False
        assert parsed.export_name == item['title']

    def test_file_metadata_docs(self, basepath, root_provider_fixtures):
        item = root_provider_fixtures['docs_file_metadata']
        path = basepath.child(item['title'])
        parsed = IQBRIMSFileMetadata(item, path)

        assert parsed.name == item['title'] + '.gdoc'
        assert parsed.extra == {
            'revisionId': item['version'],
            'downloadExt': '.docx',
            'webView': item['alternateLink'],
        }
        assert parsed.is_iqbrims_doc is True
        assert parsed.export_name == item['title'] + '.docx'

    def test_folder_metadata(self, root_provider_fixtures):
        item = root_provider_fixtures['folder_metadata']
        path = IQBRIMSPath('/we/love/you/conrad').child(item['title'], folder=True)
        parsed = IQBRIMSFolderMetadata(item, path)

        assert parsed.provider == 'iqbrims'
        assert parsed.id == item['id']
        assert parsed.name == item['title']
        assert parsed.extra == {'revisionId': item['version']}
        assert parsed.path == '/' + os.path.join(*[x.raw for x in path.parts]) + '/'
        assert parsed.materialized_path == str(path)
        assert parsed.export_name == item['title']

    def test_folder_metadata_slash(self, root_provider_fixtures):
        item = root_provider_fixtures['folder_metadata_forward_slash']
        path = IQBRIMSPath('/we/love/you/conrad').child(item['title'], folder=True)
        parsed = IQBRIMSFolderMetadata(item, path)

        assert parsed.provider == 'iqbrims'
        assert parsed.id == item['id']
        assert parsed.name == item['title']
        assert parsed.extra == {'revisionId': item['version']}
        assert parsed.path == '/' + os.path.join(*[x.raw for x in path.parts]) + '/'
        assert parsed.materialized_path == str(path)
        assert parsed.export_name == item['title']

    def test_revision_metadata(self, revision_fixtures):
        item = revision_fixtures['revision_metadata']
        parsed = IQBRIMSRevision(item)
        assert parsed.version_identifier == 'revision'
        assert parsed.version == item['id']
        assert parsed.modified == item['modifiedDate']
