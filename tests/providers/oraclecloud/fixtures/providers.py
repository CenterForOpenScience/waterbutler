import pytest


@pytest.fixture()
def mock_auth():
    return {"name": "Test User", "email": "test@osf.io"}


@pytest.fixture()
def mock_creds():
    return {
        "access_key": "fake-access-key-id",
        "secret_key": "fake-secret-access-key",
        "region": "us-ashburn-1",
    }


@pytest.fixture()
def mock_settings():
    return {
        "bucket": "test-bucket",
        "namespace": "test-namespace",
    }
