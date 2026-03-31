import pytest


@pytest.fixture()
def mock_auth():
    return {"name": "Test User", "email": "test@osf.io"}


@pytest.fixture()
def mock_creds():
    return {
        "oci_user": "ocid1.user.oc1..aaaaaaaafakeuser",
        "oci_fingerprint": "aa:bb:cc:dd:ee:ff:00:11:22:33:44:55:66:77:88:99",
        "oci_tenancy": "ocid1.tenancy.oc1..aaaaaaaafaketenancy",
        "oci_region": "us-ashburn-1",
        "oci_key_content": "-----BEGIN RSA PRIVATE KEY-----\nfake\n-----END RSA PRIVATE KEY-----",
    }


@pytest.fixture()
def mock_settings():
    return {
        "bucket": "test-bucket",
        "namespace": "test-namespace",
    }
