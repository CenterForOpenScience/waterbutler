import pytest

from waterbutler.providers.dropbox.metadata import (DropboxFileMetadata,
                                                    DropboxFolderMetadata,
                                                    DropboxRevision)

from tests.providers.dropbox.fixtures import provider_fixtures, revision_fixtures


class TestDropboxMetadata:

    def test_file_metadata(self, provider_fixtures):
        data = DropboxFileMetadata(provider_fixtures['file_metadata'], '/Photos')

        assert data.name == 'Getting_Started.pdf'
        assert data.path == '/Getting_Started.pdf'
        assert data.size == 124778
        assert data.size_as_int == 124778
        assert type(data.size_as_int) == int

        assert data.modified == '2016-06-13T19:08:17Z'
        assert data.created_utc is None
        assert data.content_type is None
        assert data.etag == '2ba1017a0c1e'
        assert data.extra == {
            'hashes': {'dropbox': 'meow'},
            'id': 'id:8y8sAJlrhuAAAAAAAAAAAQ',
            'revisionId': '2ba1017a0c1e'
        }
        assert data.serialized() == {
            'extra': {
                'revisionId': '2ba1017a0c1e',
                'id': 'id:8y8sAJlrhuAAAAAAAAAAAQ',
                'hashes': {'dropbox': 'meow'}
            },
            'kind': 'file',
            'name': 'Getting_Started.pdf',
            'path': '/Getting_Started.pdf',
            'provider': 'dropbox',
            'materialized': '/Getting_Started.pdf',
            'etag': '98872cd97c368927d590ce829141d99ddc9f970b70a8bf61cb45bfb48d9675fd',
            'contentType': None,
            'modified': '2016-06-13T19:08:17Z',
            'modified_utc': '2016-06-13T19:08:17+00:00',
            'created_utc': None,
            'size': 124778,
            'sizeInt': 124778,
        }
        assert data.kind == 'file'
        assert data.materialized_path == '/Getting_Started.pdf'
        assert data.provider == 'dropbox'
        assert data.is_file is True

        link_url = 'http://localhost:7777/v1/resources/jverwz/providers/dropbox/Getting_Started.pdf'
        assert data._entity_url('jverwz') == link_url
        assert data._json_api_links('jverwz') == {
            'delete': link_url,
            'download': link_url,
            'upload': '{}?kind=file'.format(link_url),
            'move': link_url,
        }
        assert data.json_api_serialized('jverwz') == {
            'id': 'dropbox/Getting_Started.pdf',
            'type': 'files',
            'attributes': {
                'extra': {
                    'revisionId': '2ba1017a0c1e',
                    'id': 'id:8y8sAJlrhuAAAAAAAAAAAQ',
                    'hashes': {'dropbox': 'meow'}
                },
                'kind': 'file',
                'name': 'Getting_Started.pdf',
                'path': '/Getting_Started.pdf',
                'provider': 'dropbox',
                'materialized': '/Getting_Started.pdf',
                'etag': '98872cd97c368927d590ce829141d99ddc9f970b70a8bf61cb45bfb48d9675fd',
                'contentType': None,
                'modified': '2016-06-13T19:08:17Z',
                'modified_utc': '2016-06-13T19:08:17+00:00',
                'created_utc': None,
                'size': 124778,
                'sizeInt': 124778,
                'resource': 'jverwz'
            },
            'links': {
                'move': link_url,
                'upload': '{}?kind=file'.format(link_url),
                'delete': link_url,
                'download': link_url,
            }
        }

    def test_folder_metadata(self, provider_fixtures):
        data = DropboxFolderMetadata(provider_fixtures['folder_metadata'], '/Photos')

        assert data.name == 'newfolder'
        assert data.path == '/newfolder/'
        assert data.etag is None
        assert data.serialized() == {
            'extra': {},
            'kind': 'folder',
            'name': 'newfolder',
            'path': '/newfolder/',
            'provider': 'dropbox',
            'materialized': '/newfolder/',
            'etag': 'bbd6cc654c4a3ca1124b69fccb392ec9754e18e9094effb525192509f8e1b901'
        }

        link_url = 'http://localhost:7777/v1/resources/mucuew/providers/dropbox/newfolder/'
        assert data.json_api_serialized('mucuew') == {
            'id': 'dropbox/newfolder/',
            'type': 'files',
            'attributes': {
                'extra': {},
                'kind': 'folder',
                'name': 'newfolder',
                'path': '/newfolder/',
                'provider': 'dropbox',
                'materialized': '/newfolder/',
                'etag': 'bbd6cc654c4a3ca1124b69fccb392ec9754e18e9094effb525192509f8e1b901',
                'resource': 'mucuew',
                'size': None,
                'sizeInt': None,
            },
            'links': {
                'move': link_url,
                'upload': '{}?kind=file'.format(link_url),
                'delete': link_url,
                'new_folder': '{}?kind=folder'.format(link_url),
            }
        }
        assert data._json_api_links('mucuew') == {
            'move': link_url,
            'upload': '{}?kind=file'.format(link_url),
            'delete': link_url,
            'new_folder': '{}?kind=folder'.format(link_url),
        }
        assert data.children is None
        assert data._entity_url('mucuew') == link_url
        assert data.is_folder is True
        assert data.is_file is False
        assert data.provider == 'dropbox'
        assert data.materialized_path == '/newfolder/'
        assert data.extra == {}

    def test_revision_metadata(self, revision_fixtures):
        item = revision_fixtures['file_revision_metadata']['entries'][0]
        data = DropboxRevision(item)
        assert data.version_identifier == 'revision'
        assert data.version == '95bb27d11'
        assert data.modified == '2017-08-25T17:36:44Z'
        assert data.modified_utc == '2017-08-25T17:36:44+00:00'
        assert data.extra == {
            'id': 'id:jki_ZJstdSAAAAAAAAAABw',
            'revisionId': '95bb27d11'
        }
        assert data.serialized() == {
            'extra': {
                'id': 'id:jki_ZJstdSAAAAAAAAAABw',
                'revisionId': '95bb27d11'
            },
            'modified': '2017-08-25T17:36:44Z',
            'modified_utc': '2017-08-25T17:36:44+00:00',
            'version': '95bb27d11',
            'versionIdentifier': 'revision'
        }

        assert data.json_api_serialized() == {
            'id': '95bb27d11',
            'type': 'file_versions',
            'attributes': {
                'extra': {
                    'revisionId': '95bb27d11',
                    'id': 'id:jki_ZJstdSAAAAAAAAAABw'
                },
                'version': '95bb27d11',
                'modified': '2017-08-25T17:36:44Z',
                'modified_utc': '2017-08-25T17:36:44+00:00',
                'versionIdentifier': 'revision'
            }
        }
