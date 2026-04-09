import base64
import hashlib

import pytest

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from waterbutler.providers.oraclecloud.signing import (
    OCISigner,
    _build_signing_string,
    _compute_body_sha256,
    _rsa_sha256_sign,
    load_private_key,
)

OCI_HOST = "objectstorage.us-ashburn-1.oraclecloud.com"
OCI_BASE = f"https://{OCI_HOST}"


@pytest.fixture()
def rsa_key():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


@pytest.fixture()
def rsa_pem(rsa_key):
    return rsa_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("ascii")


@pytest.fixture()
def signer(rsa_pem):
    return OCISigner(
        tenancy="ocid1.tenancy.oc1..test",
        user="ocid1.user.oc1..test",
        fingerprint="aa:bb:cc:dd",
        private_key_content=rsa_pem,
    )


class TestLoadPrivateKey:

    def test_load_pem_string(self, rsa_pem):
        assert load_private_key(rsa_pem) is not None

    def test_invalid_key_raises(self):
        with pytest.raises(ValueError):
            load_private_key("not-a-real-key")


class TestBuildSigningString:

    def test_get_request(self):
        result = _build_signing_string(
            ["date", "(request-target)", "host"],
            {"date": "Thu, 01 Jan 2025 00:00:00 GMT"},
            "GET", "/n/ns/b/bkt/o/file.txt", OCI_HOST,
        )
        lines = result.split("\n")
        assert lines[0] == "date: Thu, 01 Jan 2025 00:00:00 GMT"
        assert lines[1] == "(request-target): get /n/ns/b/bkt/o/file.txt"
        assert lines[2] == f"host: {OCI_HOST}"

    def test_put_includes_body_headers(self):
        result = _build_signing_string(
            ["date", "(request-target)", "host",
             "content-length", "content-type", "x-content-sha256"],
            {
                "date": "Thu, 01 Jan 2025 00:00:00 GMT",
                "content-length": "1024",
                "content-type": "application/octet-stream",
                "x-content-sha256": "abc123",
            },
            "PUT", "/n/ns/b/bkt/o/file.txt", OCI_HOST,
        )
        lines = result.split("\n")
        assert len(lines) == 6
        assert lines[3] == "content-length: 1024"
        assert lines[4] == "content-type: application/octet-stream"
        assert lines[5] == "x-content-sha256: abc123"


class TestComputeBodySha256:

    def test_empty(self):
        expected = base64.b64encode(hashlib.sha256(b"").digest()).decode()
        assert _compute_body_sha256(b"") == expected

    def test_with_content(self):
        expected = base64.b64encode(hashlib.sha256(b"hello world").digest()).decode()
        assert _compute_body_sha256(b"hello world") == expected


class TestRsaSha256Sign:

    def test_sign_and_verify(self, rsa_key):
        msg = "date: Thu, 01 Jan 2025 00:00:00 GMT"
        sig_b64 = _rsa_sha256_sign(rsa_key, msg)

        # verify round-trips correctly
        rsa_key.public_key().verify(
            base64.b64decode(sig_b64),
            msg.encode("ascii"),
            padding.PKCS1v15(),
            hashes.SHA256(),
        )


class TestOCISigner:

    def test_api_key_format(self, signer):
        assert signer.api_key == "ocid1.tenancy.oc1..test/ocid1.user.oc1..test/aa:bb:cc:dd"

    def test_get_request(self, signer):
        headers = signer.sign_request("GET", f"{OCI_BASE}/n/ns/b/bkt/o/file.txt", {})

        assert "authorization" in headers
        assert "date" in headers
        assert "host" in headers
        assert headers["authorization"].startswith('Signature algorithm="rsa-sha256"')
        assert 'keyId="ocid1.tenancy.oc1..test/ocid1.user.oc1..test/aa:bb:cc:dd"' in headers["authorization"]
        assert "x-content-sha256" not in headers

    def test_put_with_body(self, signer):
        body = b"test data"
        headers = signer.sign_request(
            "PUT", f"{OCI_BASE}/n/ns/b/bkt/o/file.txt", {}, body=body,
        )

        assert "x-content-sha256" in headers
        assert headers["content-length"] == str(len(body))
        assert headers["content-type"] == "application/octet-stream"
        assert "x-content-sha256" in headers["authorization"]

    def test_put_without_body(self, signer):
        headers = signer.sign_request(
            "PUT", f"{OCI_BASE}/n/ns/b/bkt/o/file.txt", {},
        )
        assert headers["content-length"] == "0"
        assert "x-content-sha256" in headers

    def test_preserves_extra_headers(self, signer):
        headers = signer.sign_request(
            "GET", f"{OCI_BASE}/n/ns/b/bkt/o/file.txt", {"x-custom": "value"},
        )
        assert headers["x-custom"] == "value"

    def test_query_string_in_request_target(self, signer):
        url = f"{OCI_BASE}/n/ns/b/bkt/o?prefix=folder/&delimiter=/"
        headers = signer.sign_request("GET", url, {})
        assert "authorization" in headers
