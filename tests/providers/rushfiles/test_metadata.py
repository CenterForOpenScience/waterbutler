import pytest

import os

from waterbutler.core import utils
from waterbutler.providers.rushfiles.metadata import (RushFilesPath, RushFilesFileMetadata, RushFilesFolderMetadata)

from tests.providers.rushfiles.fixtures import(
    root_provider_fixtures,
)

@pytest.fixture
def basepath():
    return RushFilesPath('/conrad')
    
class TestMetadata:

    def test_file_metadata(self, basepath, root_provider_fixtures):
        item = root_provider_fixtures['file_metadata']
        path = basepath.child(item['PublicName'])
        parsed = RushFilesFileMetadata(item, path)

        assert parsed.provider == 'rushfiles'
        assert path.name == item['PublicName']
        assert parsed.path == '/' + os.path.join(*[x.raw for x in path.parts])
        assert parsed.modified == item['LastWriteTime']
        assert parsed.size == item['EndOfFile']
        assert parsed.etag == item['InternalName'] + '-' + str(item['Tick'])
        assert parsed.created_utc == utils.normalize_datetime(item['CreationTime'])
        assert parsed.content_type == None
        assert parsed.upload_name == item['UploadName']
        assert parsed.extra == {'internalName': item['InternalName'],
                                'shareId': item['ShareId'],
                                'parentId': item['ParrentId'],
                            }
    
    def test_folder_metadata(self, root_provider_fixtures):
        item = root_provider_fixtures['folder_metadata']
        path = RushFilesPath('/we/love/you/conrad').child(item['PublicName'], folder=True)
        parsed = RushFilesFolderMetadata(item, path)

        assert parsed.provider == 'rushfiles'
        assert parsed.name == item['PublicName']
        assert parsed.path == '/' + os.path.join(*[x.raw for x in path.parts]) + '/'
        assert parsed.extra == {'internalName': item['InternalName'],
                                'shareId': item['ShareId'],
                                'parentId': item['ParrentId'],
                            }
