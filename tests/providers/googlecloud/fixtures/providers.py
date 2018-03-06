import pytest


@pytest.fixture()
def mock_auth():
    return {'name': 'Roger Deng', 'email': 'roger@deng.com'}


@pytest.fixture()
def mock_auth_2():
    return {'name': 'Deng Roger', 'email': 'deng@roger.com'}


@pytest.fixture()
def mock_creds():
    return {'token': 'GlxLBdGqh56rEStTEs0KeMdEFmRJlGpg7e95y8jvzQoHbFZrnPDNB'}


@pytest.fixture()
def mock_creds_2():
    return {'token': 'eMdEFmRJlGpg7e95y8jvzQoHbFZrnPDNBsYTIG2txg8SmacwtERkU'}


@pytest.fixture()
def mock_settings():
    return {'bucket': 'gcloud-test.longzechen.com', 'region': 'US-EAST1'}


@pytest.fixture()
def mock_settings_2():
    return {'bucket': 'gcloud-test-2.longzechen.com', 'region': 'US-EAST1'}
