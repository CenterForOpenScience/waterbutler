import pytest


@pytest.fixture
def owner():
    return 'cslzchen'


@pytest.fixture
def repo():
    return 'waterbutler-public'


@pytest.fixture
def file_metadata():
    return {
        'size': 20,
        'path': 'folder2-lvl1/folder1-lvl2/folder1-lvl3/file0002.20bytes.txt',
        'timestamp': '2019-04-26T15:13:12+00:00',
        'created_utc': '2019-04-25T06:18:21+00:00',
        'revision': 'dd8c7b642e32'
    }


@pytest.fixture
def folder_metadata():
    return {'name': 'folder1-lvl3'}


@pytest.fixture
def revision_metadata():
    return {
        'revision': 'ad0412ab6f8e',
        'size': 20,
        'path': 'file0001.20bytes.txt',
        'raw_node': 'ad0412ab6f8e6d614701e290843e160d002cc124',
        'raw_author': 'longze chen <cs.longze.chen@gmail.com',
        'branch': None,
        'timestamp': '2019-04-25T11:58:30+00:00',
        'utctimestamp': '2019-04-25T11:58:30+00:00'
    }
