import datetime as dt_mod
from unittest import mock

import pytest

from waterbutler.providers.oraclecloud.signing import (
    ALGORITHM,
    EMPTY_SHA256,
    UNSIGNED_PAYLOAD,
    SigV4Signer,
    _derive_signing_key,
)


@pytest.fixture()
def signer():
    return SigV4Signer(
        access_key="AKEXAMPLE",
        secret_key="SECRET",
        region="us-ashburn-1",
    )


@pytest.fixture()
def frozen_now():
    return dt_mod.datetime(2025, 3, 1, 12, 0, 0, tzinfo=dt_mod.timezone.utc)


@pytest.fixture()
def frozen_signer(signer, frozen_now):
    """Signer with frozen time for deterministic output."""
    with mock.patch("waterbutler.providers.oraclecloud.signing.datetime") as mock_dt:
        mock_dt.datetime.now.return_value = frozen_now
        mock_dt.timezone = dt_mod.timezone
        yield signer


class TestDeriveSigningKey:

    def test_returns_32_bytes(self):
        key = _derive_signing_key("secret", "20250301", "us-ashburn-1", "s3")
        assert isinstance(key, bytes)
        assert len(key) == 32

    def test_deterministic(self):
        k1 = _derive_signing_key("secret", "20250301", "us-ashburn-1", "s3")
        k2 = _derive_signing_key("secret", "20250301", "us-ashburn-1", "s3")
        assert k1 == k2

    def test_varies_with_date(self):
        k1 = _derive_signing_key("secret", "20250301", "us-ashburn-1", "s3")
        k2 = _derive_signing_key("secret", "20250302", "us-ashburn-1", "s3")
        assert k1 != k2

    def test_varies_with_region(self):
        k1 = _derive_signing_key("secret", "20250301", "us-ashburn-1", "s3")
        k2 = _derive_signing_key("secret", "20250301", "eu-frankfurt-1", "s3")
        assert k1 != k2

    def test_varies_with_secret(self):
        k1 = _derive_signing_key("secret-a", "20250301", "us-ashburn-1", "s3")
        k2 = _derive_signing_key("secret-b", "20250301", "us-ashburn-1", "s3")
        assert k1 != k2


class TestSigV4Signer:

    def test_authorization_header_format(self, frozen_signer):
        headers = frozen_signer.sign_request(
            "GET",
            "https://ns.compat.objectstorage.us-ashburn-1.oraclecloud.com/bkt/key",
        )
        auth = headers["Authorization"]
        assert auth.startswith(
            f"{ALGORITHM} Credential=AKEXAMPLE/20250301/us-ashburn-1/s3/aws4_request"
        )
        assert "SignedHeaders=" in auth
        assert "Signature=" in auth

    def test_amz_date_header(self, frozen_signer):
        headers = frozen_signer.sign_request("GET", "https://host/bkt/key")
        assert headers["x-amz-date"] == "20250301T120000Z"

    def test_content_sha256_defaults_to_empty(self, frozen_signer):
        headers = frozen_signer.sign_request("GET", "https://host/bkt/key")
        assert headers["x-amz-content-sha256"] == EMPTY_SHA256

    def test_content_sha256_with_unsigned_payload(self, frozen_signer):
        headers = frozen_signer.sign_request(
            "PUT", "https://host/bkt/key", payload_hash=UNSIGNED_PAYLOAD
        )
        assert headers["x-amz-content-sha256"] == UNSIGNED_PAYLOAD

    def test_preserves_extra_headers(self, frozen_signer):
        headers = frozen_signer.sign_request(
            "PUT",
            "https://host/bkt/key",
            headers={"Content-Type": "text/plain", "Content-Length": "42"},
        )
        assert headers["Content-Type"] == "text/plain"
        assert headers["Content-Length"] == "42"

    def test_host_not_in_output(self, frozen_signer):
        """``host`` is used for signing but not returned (aiohttp sets it)."""
        headers = frozen_signer.sign_request(
            "GET", "https://host.example.com/bkt/key"
        )
        assert "host" not in headers
        assert "Host" not in headers

    def test_signed_headers_sorted(self, frozen_signer):
        headers = frozen_signer.sign_request(
            "PUT",
            "https://host/bkt/key",
            headers={"Zebra": "z", "Alpha": "a"},
        )
        auth = headers["Authorization"]
        sh_part = [p for p in auth.split(", ") if p.startswith("SignedHeaders=")][0]
        signed = sh_part.split("=", 1)[1].split(";")
        assert signed == sorted(signed)

    def test_host_included_in_signed_headers(self, frozen_signer):
        headers = frozen_signer.sign_request("GET", "https://host/bkt/key")
        auth = headers["Authorization"]
        sh_part = [p for p in auth.split(", ") if p.startswith("SignedHeaders=")][0]
        signed_names = sh_part.split("=", 1)[1].split(";")
        assert "host" in signed_names

    def test_different_methods_different_signatures(self, frozen_signer):
        url = "https://host/bkt/key"
        h_get = frozen_signer.sign_request("GET", url)
        h_put = frozen_signer.sign_request("PUT", url)
        assert h_get["Authorization"] != h_put["Authorization"]

    def test_different_urls_different_signatures(self, frozen_signer):
        h1 = frozen_signer.sign_request("GET", "https://host/bkt/key1")
        h2 = frozen_signer.sign_request("GET", "https://host/bkt/key2")
        assert h1["Authorization"] != h2["Authorization"]

    def test_query_string_parameters_signed(self, frozen_signer):
        headers = frozen_signer.sign_request(
            "GET",
            "https://host/bkt?list-type=2&prefix=f%2F&delimiter=%2F",
        )
        assert "Authorization" in headers

    def test_deterministic_with_same_inputs(self, frozen_signer):
        url = "https://host/bkt/key"
        h1 = frozen_signer.sign_request("GET", url)
        h2 = frozen_signer.sign_request("GET", url)
        assert h1["Authorization"] == h2["Authorization"]

    def test_credential_scope_contains_region_and_service(self, frozen_signer):
        headers = frozen_signer.sign_request("GET", "https://host/bkt/key")
        auth = headers["Authorization"]
        assert "us-ashburn-1/s3/aws4_request" in auth

    def test_different_payload_hashes_different_signatures(self, frozen_signer):
        url = "https://host/bkt/key"
        h1 = frozen_signer.sign_request("PUT", url, payload_hash=EMPTY_SHA256)
        h2 = frozen_signer.sign_request("PUT", url, payload_hash="abc123")
        assert h1["Authorization"] != h2["Authorization"]
