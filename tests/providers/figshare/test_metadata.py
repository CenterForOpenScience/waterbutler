import pytest

from waterbutler.providers.figshare.metadata import (FigshareFileMetadata,
                                                     FigshareFolderMetadata,
                                                     FigshareFileRevisionMetadata)

from tests.providers.figshare.fixtures import root_provider_fixtures


class TestFigshareFileMetadata:

    def test_file_metadata(self, root_provider_fixtures):
        base_meta = root_provider_fixtures['file_article_metadata']
        data = FigshareFileMetadata(
            base_meta,
            base_meta['files'][0]
        )

        assert data.id == 6530715
        assert data.name == 'file'
        assert data.article_id == 4037952
        assert data.article_name == 'file_article'
        assert data.path == '/4037952/6530715'
        assert data.materialized_path == '/file_article/file'
        assert data.upload_path == '/4037952/6530715'
        assert data.size == 7
        assert data.content_type is None
        assert data.modified is None
        assert data.modified_utc is None
        assert data.created_utc is None
        assert data.can_delete is True
        assert data.is_public is False
        assert data.etag == 'draft:4037952:b3e656f8b0828a31f3ed396a1c868786'
        assert data.web_view == 'https://figshare.com/account/articles/4037952'
        assert data.extra == {
            'fileId': 6530715,
            'articleId': 4037952,
            'status': 'draft',
            'downloadUrl': 'https://ndownloader.figshare.com/files/6530715',
            'canDelete': True,
            'webView': 'https://figshare.com/account/articles/4037952',
            'hashes': {
                'md5': 'b3e656f8b0828a31f3ed396a1c868786'
            }
        }

        assert data.kind == 'file'
        assert data.serialized() == {
            'extra': {
                'fileId': 6530715,
                'articleId': 4037952,
                'status': 'draft',
                'downloadUrl': 'https://ndownloader.figshare.com/files/6530715',
                'canDelete': True,
                'webView': 'https://figshare.com/account/articles/4037952',
                'hashes': {
                    'md5': 'b3e656f8b0828a31f3ed396a1c868786'
                }
            },
            'kind': 'file',
            'name': 'file',
            'path': '/4037952/6530715',
            'provider': 'figshare',
            'materialized': '/file_article/file',
            'etag': 'c22302dc6826efe0a70f9528a3edd3ec3af340d52ac248b2bf23fd64d6ffea8d',
            'contentType': None,
            'modified': None,
            'modified_utc': None,
            'created_utc': None,
            'size': 7
        }

        api_url = 'http://localhost:7777/v1/resources/cn42d/providers/figshare/4037952/6530715'
        assert data.json_api_serialized('cn42d') == {
            'id': 'figshare/4037952/6530715',
            'type': 'files',
            'attributes': {
                'extra': {
                    'fileId': 6530715,
                    'articleId': 4037952,
                    'status': 'draft',
                    'downloadUrl': 'https://ndownloader.figshare.com/files/6530715',
                    'canDelete': True,
                    'webView': 'https://figshare.com/account/articles/4037952',
                    'hashes': {
                        'md5': 'b3e656f8b0828a31f3ed396a1c868786'
                    }
                },
                'kind': 'file',
                'name': 'file',
                'path': '/4037952/6530715',
                'provider': 'figshare',
                'materialized': '/file_article/file',
                'etag': 'c22302dc6826efe0a70f9528a3edd3ec3af340d52ac248b2bf23fd64d6ffea8d',
                'contentType': None,
                'modified': None,
                'modified_utc': None,
                'created_utc': None,
                'size': 7,
                'resource': 'cn42d'
            },
            'links': {
                'move': api_url,
                'upload': '{}?kind=file'.format(api_url),
                'delete': api_url,
                'download': api_url,
            }
        }

        assert data._json_api_links('cn42d') == {
            'move': api_url,
            'upload': '{}?kind=file'.format(api_url),
            'delete': api_url,
            'download': api_url,
        }

    def test_public_file_metadata(self, root_provider_fixtures):
        item = root_provider_fixtures['file_article_metadata']
        public_metadata = FigshareFileMetadata(item, item['files'][0])
        public_metadata.raw['url'] = 'https://api.figshare.com/v2'

        assert public_metadata.name == 'file'
        assert public_metadata.is_public is True
        assert public_metadata.web_view == 'https://figshare.com/articles/4037952'

    def test_metadata_article_identifier(self, root_provider_fixtures):
        item = root_provider_fixtures['file_article_metadata']
        article_metadata = FigshareFileMetadata(item, item['files'][0])
        article_metadata.raw['url'] = 'https://api.figshare.com/v2/account/articles/'

        assert article_metadata.article_name == ''
        assert article_metadata.path == '/6530715'
        assert article_metadata.materialized_path == '/file'

    def test_folder_metadata(self, root_provider_fixtures):
        data = FigshareFolderMetadata(root_provider_fixtures['folder_article_metadata'])

        assert data.id == 4040019
        assert data.name == 'folder_article'
        assert data.path == '/4040019/'
        assert data.materialized_path == '/folder_article/'
        assert data.size is None
        assert data.modified == '2016-10-18T20:47:25Z'
        assert data.created_utc is None
        assert data.etag == 'draft::::4040019'
        assert data.kind == 'folder'
        assert data.extra == {
            'id': 4040019,
            'doi': '',
            'status': 'draft'
        }

        assert data.serialized() == {
            'extra': {
                'id': 4040019,
                'doi': '',
                'status': 'draft'
            },
            'kind': 'folder',
            'name': 'folder_article',
            'path': '/4040019/',
            'provider': 'figshare',
            'materialized': '/folder_article/',
            'etag': '6bef522e6f14597fd939b6b5c29e99091dc0b0badcac332da6e75bec0a69cf5e'
        }

        api_url = 'http://localhost:7777/v1/resources/45hjnz/providers/figshare/4040019/'
        assert data.json_api_serialized('45hjnz') == {
            'id': 'figshare/4040019/',
            'type': 'files',
            'attributes': {
                'extra': {
                    'id': 4040019,
                    'doi': '',
                    'status': 'draft'
                },
                'kind': 'folder',
                'name': 'folder_article',
                'path': '/4040019/',
                'provider': 'figshare',
                'materialized': '/folder_article/',
                'etag': '6bef522e6f14597fd939b6b5c29e99091dc0b0badcac332da6e75bec0a69cf5e',
                'resource': '45hjnz',
                'size': None
            },
            'links': {
                'move': api_url,
                'upload': '{}?kind=file'.format(api_url),
                'delete': api_url,
                'new_folder': '{}?kind=folder'.format(api_url),
            }
        }

        assert data._json_api_links('45hjnz') == {
            'move': api_url,
            'upload': '{}?kind=file'.format(api_url),
            'delete': api_url,
            'new_folder': '{}?kind=folder'.format(api_url),
        }

    def test_revision_metadata(self):
        data = FigshareFileRevisionMetadata()

        assert data.modified is None
        assert data.modified_utc is None
        assert data.version_identifier == 'revision'
        assert data.version == 'latest'
        assert data.extra == {}
        assert data.serialized() == {
            'extra': {},
            'version': 'latest',
            'modified': None,
            'modified_utc': None,
            'versionIdentifier': 'revision',
        }

        assert data.json_api_serialized() == {
            'id': 'latest',
            'type': 'file_versions',
            'attributes': {
                'extra': {},
                'version': 'latest',
                'modified': None,
                'modified_utc': None,
                'versionIdentifier': 'revision',
            }
        }
