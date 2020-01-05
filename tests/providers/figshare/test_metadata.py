import pytest

from waterbutler.providers.figshare.metadata import (FigshareFileMetadata,
                                                     FigshareFolderMetadata,
                                                     FigshareFileRevisionMetadata)

from tests.providers.figshare.fixtures import (project_article_type_1_metadata,
                                               project_article_type_3_metadata)


class TestFigshareFileMetadata:

    def test_private_file_metadata(self, project_article_type_1_metadata):
        base_meta = project_article_type_1_metadata['private']
        data = FigshareFileMetadata(base_meta, base_meta['files'][0])

        assert data.id == 15562817
        assert data.name == 'FigurePrivate01.png'
        assert data.article_id == 8305859
        assert data.article_name == 'FigurePrivate'
        assert data.path == '/8305859/15562817'
        assert data.materialized_path == '/FigurePrivate/FigurePrivate01.png'
        assert data.upload_path == '/8305859/15562817'
        assert data.size == 89281
        assert data.size_as_int == 89281
        assert type(data.size_as_int) == int

        assert data.content_type is None
        assert data.modified is None
        assert data.modified_utc is None
        assert data.created_utc is None
        assert data.can_delete is True
        assert data.is_public is False
        assert data.etag == 'draft:8305859:cae3869aa4b144a3aa5cffe979359836'
        assert data.web_view == 'https://figshare.com/account/articles/8305859'
        assert data.extra == {
            'fileId': 15562817,
            'articleId': 8305859,
            'status': 'draft',
            'downloadUrl': 'https://ndownloader.figshare.com/files/15562817',
            'canDelete': True,
            'webView': 'https://figshare.com/account/articles/8305859',
            'hashingInProgress': False,
            'hashes': {
                'md5': 'cae3869aa4b144a3aa5cffe979359836'
            }
        }

        assert data.kind == 'file'
        assert data.serialized() == {
            'extra': {
                'fileId': 15562817,
                'articleId': 8305859,
                'status': 'draft',
                'downloadUrl': 'https://ndownloader.figshare.com/files/15562817',
                'canDelete': True,
                'webView': 'https://figshare.com/account/articles/8305859',
                'hashingInProgress': False,
                'hashes': {
                    'md5': 'cae3869aa4b144a3aa5cffe979359836'
                }
            },
            'kind': 'file',
            'name': 'FigurePrivate01.png',
            'path': '/8305859/15562817',
            'provider': 'figshare',
            'materialized': '/FigurePrivate/FigurePrivate01.png',
            'etag': '0113713d8af08db5fb6a0f6565b115d98d0f6c284b808a58997f1e73bdec397e',
            'contentType': None,
            'modified': None,
            'modified_utc': None,
            'created_utc': None,
            'size': 89281,
            'sizeInt': 89281,
        }

        api_url = 'http://localhost:7777/v1/resources/cn42d/providers/figshare/8305859/15562817'
        assert data.json_api_serialized('cn42d') == {
            'id': 'figshare/8305859/15562817',
            'type': 'files',
            'attributes': {
                'extra': {
                    'fileId': 15562817,
                    'articleId': 8305859,
                    'status': 'draft',
                    'downloadUrl': 'https://ndownloader.figshare.com/files/15562817',
                    'canDelete': True,
                    'webView': 'https://figshare.com/account/articles/8305859',
                    'hashingInProgress': False,
                    'hashes': {
                        'md5': 'cae3869aa4b144a3aa5cffe979359836'
                    }
                },
                'kind': 'file',
                'name': 'FigurePrivate01.png',
                'path': '/8305859/15562817',
                'provider': 'figshare',
                'materialized': '/FigurePrivate/FigurePrivate01.png',
                'etag': '0113713d8af08db5fb6a0f6565b115d98d0f6c284b808a58997f1e73bdec397e',
                'contentType': None,
                'modified': None,
                'modified_utc': None,
                'created_utc': None,
                'size': 89281,
                'sizeInt': 89281,
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

    def test_public_file_metadata(self, project_article_type_1_metadata):
        item = project_article_type_1_metadata['public']
        public_metadata = FigshareFileMetadata(item, item['files'][0])

        assert public_metadata.id == 15451592
        assert public_metadata.name == 'Figure01.png'
        assert public_metadata.article_id == 8263730
        assert public_metadata.article_name == 'Figure'
        assert public_metadata.is_public is True
        assert public_metadata.web_view == 'https://figshare.com/articles/Figure01_png/8263730'
        assert public_metadata.extra.get('status') == 'public'

    def test_metadata_article_identifier(self, project_article_type_1_metadata):
        item = project_article_type_1_metadata['private']
        article_metadata = FigshareFileMetadata(item, item['files'][0])
        article_metadata.raw['url'] = 'https://api.figshare.com/v2/account/articles/8263730'

        assert article_metadata.id == 15562817
        assert article_metadata.name == 'FigurePrivate01.png'
        assert article_metadata.article_id == 8305859
        assert article_metadata.article_name == ''
        assert article_metadata.path == '/15562817'
        assert article_metadata.materialized_path == '/FigurePrivate01.png'

    def test_private_folder_metadata(self, project_article_type_3_metadata):
        data = FigshareFolderMetadata(project_article_type_3_metadata['private'])

        assert data.id == 8269766
        assert data.name == 'DatasetPrivate'
        assert data.path == '/8269766/'
        assert data.materialized_path == '/DatasetPrivate/'
        assert data.size is None
        assert data.modified == '2019-06-13T16:00:12Z'
        assert data.created_utc is None
        assert data.etag == 'draft::::8269766'
        assert data.kind == 'folder'
        assert data.extra == {
            'id': 8269766,
            'doi': '',
            'status': 'draft'
        }

        assert data.serialized() == {
            'extra': {
                'id': 8269766,
                'doi': '',
                'status': 'draft'
            },
            'kind': 'folder',
            'name': 'DatasetPrivate',
            'path': '/8269766/',
            'provider': 'figshare',
            'materialized': '/DatasetPrivate/',
            'etag': 'd7dc67b05e9c50c8adefabc9e2ff2cfaabad26913e8c9916396f067216941389'
        }

        api_url = 'http://localhost:7777/v1/resources/45hjnz/providers/figshare/8269766/'
        assert data.json_api_serialized('45hjnz') == {
            'id': 'figshare/8269766/',
            'type': 'files',
            'attributes': {
                'extra': {
                    'id': 8269766,
                    'doi': '',
                    'status': 'draft'
                },
                'kind': 'folder',
                'name': 'DatasetPrivate',
                'path': '/8269766/',
                'provider': 'figshare',
                'materialized': '/DatasetPrivate/',
                'etag': 'd7dc67b05e9c50c8adefabc9e2ff2cfaabad26913e8c9916396f067216941389',
                'resource': '45hjnz',
                'size': None,
                'sizeInt': None,
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

    def test_public_folder_metadata(self, project_article_type_3_metadata):
        data = FigshareFolderMetadata(project_article_type_3_metadata['public'])

        assert data.id == 8263811
        assert data.name == 'Dataset'
        assert data.extra.get('status') == 'public'

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
