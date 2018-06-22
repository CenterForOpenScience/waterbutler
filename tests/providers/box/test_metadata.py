import pytest

from waterbutler.core.path import WaterButlerPath
from waterbutler.providers.box.metadata import (BoxFolderMetadata,
                                                BoxFileMetadata,
                                                BoxRevision)

from tests.providers.box.fixtures import(intra_fixtures,
                                         revision_fixtures,
                                         root_provider_fixtures)


class TestBoxMetadata:

    def test_file_metadata(self, root_provider_fixtures):
        item = root_provider_fixtures['file_metadata']['entries'][0]
        dest_path = WaterButlerPath('/charmander/name.txt', _ids=('0', item['id'], item['id']))
        data = BoxFileMetadata(item, dest_path)
        assert data.name == 'tigers.jpeg'
        assert data.path == '/5000948880'
        assert data.provider == 'box'
        assert data.size == 629644
        assert data.size_as_int == 629644
        assert type(data.size_as_int) == int
        assert data.modified == '2012-12-12T11:04:26-08:00'
        assert data.created_utc == '2012-12-12T18:55:30+00:00'
        assert data.content_type is None
        assert data.etag == '3::5000948880'
        assert data.extra == {
            'etag': '3',
            'hashes': {
                'sha1': '134b65991ed521fcfe4724b7d814ab8ded5185dc'
            }
        }

        assert data.serialized() == {
            'extra': {
                'etag': '3',
                'hashes': {
                    'sha1': '134b65991ed521fcfe4724b7d814ab8ded5185dc'
                }
            },
            'kind': 'file',
            'name': 'tigers.jpeg',
            'path': '/5000948880',
            'provider': 'box',
            'materialized': '/charmander/name.txt',
            'etag': 'c14cacfd0701ec2d45e0b8cb89e996f00e2ec861897e8a9656adc12781c889f8',
            'contentType': None,
            'modified': '2012-12-12T11:04:26-08:00',
            'modified_utc': '2012-12-12T19:04:26+00:00',
            'created_utc': '2012-12-12T18:55:30+00:00',
            'size': 629644,
            'sizeInt': 629644,
        }
        assert data.kind == 'file'
        assert data.modified_utc == '2012-12-12T19:04:26+00:00'
        assert data.json_api_serialized('cn42d') == {
            'id': 'box/5000948880',
            'type': 'files',
            'attributes': {
                'extra': {
                    'etag': '3',
                    'hashes': {
                        'sha1': '134b65991ed521fcfe4724b7d814ab8ded5185dc'
                    }
                },
                'kind': 'file',
                'name': 'tigers.jpeg',
                'path': '/5000948880',
                'provider': 'box',
                'materialized': '/charmander/name.txt',
                'etag': 'c14cacfd0701ec2d45e0b8cb89e996f00e2ec861897e8a9656adc12781c889f8',
                'contentType': None, 'modified': '2012-12-12T11:04:26-08:00',
                'modified_utc': '2012-12-12T19:04:26+00:00',
                'created_utc': '2012-12-12T18:55:30+00:00',
                'size': 629644,
                'sizeInt': 629644,
                'resource': 'cn42d'
            },
            'links': {
                'move': 'http://localhost:7777/v1/resources/cn42d/providers/box/5000948880',
                'upload': ('http://localhost:7777/v1/resources'
                    '/cn42d/providers/box/5000948880?kind=file'),
                'delete': 'http://localhost:7777/v1/resources/cn42d/providers/box/5000948880',
                'download': 'http://localhost:7777/v1/resources/cn42d/providers/box/5000948880'
            }
        }

    def test_folder_metadata(self, intra_fixtures):
        item = intra_fixtures['intra_folder_metadata']
        dest_path = WaterButlerPath('/moveablefolder/', _ids=('0', item['id']), folder=True)
        data = BoxFolderMetadata(item, dest_path)
        assert data.name == 'moveablefolder'
        assert data.path == '/36833297084/'
        assert data.provider == 'box'
        assert data.materialized_path == '/moveablefolder/'
        assert data.is_folder is True
        assert data.serialized() == {
            'extra': {},
            'kind': 'folder',
            'name': 'moveablefolder',
            'path': '/36833297084/',
            'provider': 'box',
            'materialized': '/moveablefolder/',
            'etag': '299a515e98fe1c548c6fe6141d01a43e739cf965b0d324aa0961924162f7af79'
        }
        assert data.json_api_serialized('7ycmyr') == {
            'id': 'box/36833297084/',
            'type': 'files',
            'attributes': {
                'extra': {},
                'kind': 'folder',
                'name': 'moveablefolder',
                'path': '/36833297084/',
                'provider': 'box',
                'materialized': '/moveablefolder/',
                'etag': '299a515e98fe1c548c6fe6141d01a43e739cf965b0d324aa0961924162f7af79',
                'resource': '7ycmyr',
                'size': None,
                'sizeInt': None,
            },
            'links': {
                'move': 'http://localhost:7777/v1/resources/7ycmyr/providers/box/36833297084/',
                'upload': ('http://localhost:7777/v1/resources/'
                    '7ycmyr/providers/box/36833297084/?kind=file'),
                'delete': 'http://localhost:7777/v1/resources/7ycmyr/providers/box/36833297084/',
                'new_folder': ('http://localhost:7777/v1/resources/'
                    '7ycmyr/providers/box/36833297084/?kind=folder')
            }
        }
        assert data.children is None
        assert data.kind == 'folder'
        assert data.etag is None

    def test_revision_metadata(self, revision_fixtures):
        item = revision_fixtures['revisions_list_metadata']['entries'][0]
        data = BoxRevision(item)
        assert data.version == '25065971851'
        assert data.version_identifier == 'revision'
        assert data.modified == '2015-02-24T09:26:02-08:00'
        assert data.modified_utc == '2015-02-24T17:26:02+00:00'
        assert data.serialized() == {
            'extra': {},
            'version': '25065971851',
            'modified': '2015-02-24T09:26:02-08:00',
            'modified_utc': '2015-02-24T17:26:02+00:00',
            'versionIdentifier': 'revision'
        }
        assert data.json_api_serialized() == {
            'id': '25065971851',
            'type': 'file_versions',
            'attributes': {
                'extra': {},
                'version': '25065971851',
                'modified': '2015-02-24T09:26:02-08:00',
                'modified_utc': '2015-02-24T17:26:02+00:00',
                'versionIdentifier': 'revision'
            }
        }
        assert data.extra == {}
