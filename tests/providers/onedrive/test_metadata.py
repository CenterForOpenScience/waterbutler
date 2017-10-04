import pytest

from waterbutler.providers.onedrive.path import OneDrivePath
from waterbutler.providers.onedrive.metadata import (OneDriveFileMetadata,
                                                     OneDriveFolderMetadata,
                                                     OneDriveRevisionMetadata)

from tests.providers.onedrive.fixtures import (revision_fixtures,
                                               root_provider_fixtures)


class TestOneDriveMetadata:

    def test_build_file_metadata(self, root_provider_fixtures):
        od_path = OneDrivePath.new_from_response(root_provider_fixtures['file_metadata'], 'root')

        metadata = OneDriveFileMetadata(root_provider_fixtures['file_metadata'], od_path)
        assert metadata.provider == 'onedrive'
        assert metadata.created_utc == '2017-08-17T17:49:39+00:00'
        assert metadata.materialized_path == '/toes.txt'
        assert metadata.extra == {
            'id': root_provider_fixtures['file_id'],
            'etag': 'aRjRENTBFNDAwREZFN0Q0RSEyOTEuMg',
            'webView': 'https://1drv.ms/t/s!AE59_g1ADtX0giM',
        }
        assert metadata.name == 'toes.txt'
        assert metadata.path == '/{}'.format(root_provider_fixtures['file_id'])
        assert metadata.size == 11
        assert metadata.size_as_int == 11
        assert metadata.modified == '2017-08-17T17:49:50.38Z'
        assert metadata.modified_utc == '2017-08-17T17:49:50+00:00'
        assert metadata.content_type == 'text/plain'
        assert metadata.etag == 'aRjRENTBFNDAwREZFN0Q0RSEyOTEuMg'

        action_url = ('http://localhost:7777/v1/resources/mst3k/providers'
                     '/onedrive/{}'.format(root_provider_fixtures['file_id']))
        assert metadata._json_api_links('mst3k') == {
            'delete': None,
            'upload': None,
            'move': action_url,
            'download': action_url,
        }

    def test_build_folder_metadata(self, root_provider_fixtures):
        od_path = OneDrivePath.new_from_response(root_provider_fixtures['folder_metadata'], 'root')

        metadata = OneDriveFolderMetadata(root_provider_fixtures['folder_metadata'], od_path)
        assert metadata.provider == 'onedrive'
        assert metadata.name == 'teeth'
        assert metadata.path == '/F4D50E400DFE7D4E!290/'
        assert metadata.etag == 'aRjRENTBFNDAwREZFN0Q0RSEyOTAuMA'
        assert metadata.materialized_path == '/teeth/'
        assert metadata.extra == {
            'id': 'F4D50E400DFE7D4E!290',
            'etag': 'aRjRENTBFNDAwREZFN0Q0RSEyOTAuMA',
            'webView': 'https://1drv.ms/f/s!AE59_g1ADtX0giI',
            'modified_utc': '2017-08-17T17:53:21+00:00',
            'created_utc': '2017-08-17T17:49:26+00:00',
        }

        assert metadata._json_api_links('mst3k') == {
            'delete': None,
            'upload': None,
            'move': ('http://localhost:7777/v1/resources/mst3k/providers'
                     '/onedrive/{}/'.format(root_provider_fixtures['folder_id'])),
            'new_folder': None,
        }

    def test_build_revision_metadata(self, revision_fixtures):
        metadata = OneDriveRevisionMetadata(revision_fixtures['file_revisions']['value'][0])

        assert metadata.serialized() == {
            'extra': {},
            'version': 'current',
            'modified': '2017-11-30T15:42:33.447Z',
            'modified_utc': '2017-11-30T15:42:33+00:00',
            'versionIdentifier': 'revision',
        }
