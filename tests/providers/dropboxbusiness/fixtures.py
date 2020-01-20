import pytest

from waterbutler.providers.dropboxbusiness import DropboxBusinessProvider


@pytest.fixture
def settings():
    return {'folder': '/Photos',
            'admin_dbmid': 'dbmid:dummy',
            'team_folder_id': '1234567890'}


@pytest.fixture
def settings_root():
    return {'folder': '/',
            'admin_dbmid': 'dbmid:dummy',
            'team_folder_id': '1234567890'}


@pytest.fixture
def provider(auth, credentials, settings):
    return DropboxBusinessProvider(auth, credentials, settings)


@pytest.fixture
def other_provider(auth, other_credentials, settings):
    return DropboxBusinessProvider(auth, other_credentials, settings)


@pytest.fixture
def provider_root(auth, credentials, settings_root):
    return DropboxBusinessProvider(auth, credentials, settings_root)
