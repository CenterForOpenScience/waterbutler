import pytest

from tests import utils
from waterbutler.core.path import WaterButlerPath


@pytest.fixture
def provider1():
    return utils.MockProvider1({'user': 'name'}, {'pass': 'word'}, {})


@pytest.fixture
def provider2():
    return utils.MockProvider2({'user': 'name'}, {'pass': 'phrase'}, {})


@pytest.fixture
def dest_path():
    return WaterButlerPath('/older folder/new folder/')

@pytest.fixture
def folder_children():
    return [utils.MockFileMetadata(), utils.MockFolderMetadata(), utils.MockFileMetadata()]

@pytest.fixture
def src_path():
    return WaterButlerPath('/folder with children/')

@pytest.fixture
def mock_folder():
    return utils.MockFolderMetadata()
