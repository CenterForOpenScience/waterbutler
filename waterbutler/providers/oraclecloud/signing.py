"""AWS Signature Version 4 signing for OCI S3-compatible API.

Implements SigV4 header-based authentication without any boto/botocore dependency.
Used by :class:`~waterbutler.providers.oraclecloud.provider.OracleCloudProvider` to sign
requests to Oracle Cloud Infrastructure's Amazon S3 Compatibility API.

Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
"""

import datetime
import hashlib
import hmac
import logging
from urllib.parse import parse_qsl, quote, urlparse

logger = logging.getLogger(__name__)

ALGORITHM = "AWS4-HMAC-SHA256"
EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()
UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD"


def _hmac_sha256(key: bytes, msg: str) -> bytes:
    """HMAC-SHA256 of a UTF-8 string keyed with the given bytes."""
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def _hmac_sha256_hex(key: bytes, msg: bytes) -> str:
    """HMAC-SHA256 of a UTF-8 string keyed with the given bytes, in hex."""
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).hexdigest()


def _sha256_hex(data: bytes) -> str:
    """Hex-encoded SHA-256 digest."""
    return hashlib.sha256(data).hexdigest()


def _derive_signing_key(
    secret_key: str, date_stamp: str, region: str, service: str
) -> bytes:
    """Derive the four-level HMAC signing key used by SigV4.

    ::

        kDate    = HMAC("AWS4" + secret, date)
        kRegion  = HMAC(kDate,   region)
        kService = HMAC(kRegion, service)
        kSigning = HMAC(kService, "aws4_request")
    """
    k_date = _hmac_sha256(("AWS4" + secret_key).encode("utf-8"), date_stamp)
    k_region = _hmac_sha256(k_date, region)
    k_service = _hmac_sha256(k_region, service)
    return _hmac_sha256(k_service, "aws4_request")


class SigV4Signer:
    """Signs HTTP requests using AWS Signature Version 4.

    Produces the ``Authorization``, ``x-amz-date``, and ``x-amz-content-sha256``
    headers required by OCI's Amazon S3 Compatibility API.

    Reference: https://docs.oracle.com/en-us/iaas/Content/Object/Tasks/s3compatibleapi.htm
    """

    def __init__(
        self,
        access_key: str,
        secret_key: str,
        region: str,
        service: str = "s3",
    ) -> None:
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.service = service

    def sign_request(
        self,
        method: str,
        url: str,
        headers: dict[str, str] | None = None,
        payload_hash: str = EMPTY_SHA256,
    ) -> dict[str, str]:
        """Add SigV4 signing headers to *headers* and return them.

        The ``host`` value is derived from *url* and included in the canonical
        request for signing, but is **not** returned in the result dict because
        aiohttp sets it automatically from the URL.

        :param method: HTTP verb (``GET``, ``PUT``, ``HEAD``, ``DELETE``, ...)
        :param url: Fully-qualified URL with scheme, host, path, and query
        :param headers: Extra request headers to include in the signature
        :param payload_hash: Hex SHA-256 of the body, or :data:`UNSIGNED_PAYLOAD`
        :returns: Headers dict ready to pass to ``make_request``
        """
        if headers is None:
            headers = {}

        parsed = urlparse(url)
        host = parsed.netloc
        canonical_uri = parsed.path or "/"

        # Canonical query string: parse, sort, and re-encode per SigV4 rules.
        qs_pairs = parse_qsl(parsed.query, keep_blank_values=True)
        qs_pairs.sort(key=lambda kv: (kv[0], kv[1]))
        canonical_querystring = "&".join(
            f"{quote(k, safe='')}={quote(v, safe='')}" for k, v in qs_pairs
        )

        now = datetime.datetime.now(datetime.timezone.utc)
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = now.strftime("%Y%m%d")

        # Collect all headers for signing (lowercased keys, trimmed values).
        headers_for_signing: dict[str, str] = {
            k.lower(): v.strip() for k, v in headers.items()
        }
        headers_for_signing["host"] = host
        headers_for_signing["x-amz-date"] = amz_date
        headers_for_signing["x-amz-content-sha256"] = payload_hash

        signed_header_names = sorted(headers_for_signing)
        canonical_headers = "".join(
            f"{k}:{headers_for_signing[k]}\n" for k in signed_header_names
        )
        signed_headers_str = ";".join(signed_header_names)

        # Step 1 -- Canonical request
        canonical_request = "\n".join(
            [
                method.upper(),
                canonical_uri,
                canonical_querystring,
                canonical_headers,
                signed_headers_str,
                payload_hash,
            ]
        )

        # Step 2 -- String to sign
        credential_scope = (
            f"{date_stamp}/{self.region}/{self.service}/aws4_request"
        )
        string_to_sign = "\n".join(
            [
                ALGORITHM,
                amz_date,
                credential_scope,
                _sha256_hex(canonical_request.encode("utf-8")),
            ]
        )

        # Step 3 -- Signature
        signing_key = _derive_signing_key(
            self.secret_key, date_stamp, self.region, self.service
        )
        signature = hmac.new(
            signing_key, string_to_sign.encode("utf-8"), hashlib.sha256
        ).hexdigest()

        # Step 4 -- Assemble output headers (host excluded; aiohttp sets it)
        result = dict(headers)
        result["x-amz-date"] = amz_date
        result["x-amz-content-sha256"] = payload_hash
        result["Authorization"] = (
            f"{ALGORITHM} "
            f"Credential={self.access_key}/{credential_scope}, "
            f"SignedHeaders={signed_headers_str}, "
            f"Signature={signature}"
        )
        return result

    def sign_request_query(
        self,
        method: str,
        url: str,
        headers: dict[str, str] | None = None,
        payload_hash: str = UNSIGNED_PAYLOAD,
    ) -> dict[str, str]:
        """Add SigV4 signing headers to *headers* and return them.

        The ``host`` value is derived from *url* and included in the canonical
        request for signing, but is **not** returned in the result dict because
        aiohttp sets it automatically from the URL.

        :param method: HTTP verb (``GET``, ``PUT``, ``HEAD``, ``DELETE``, ...)
        :param url: Fully-qualified URL with scheme, host, path, and query
        :param headers: Extra request headers to include in the signature
        :param payload_hash: Hex SHA-256 of the body, or :data:`UNSIGNED_PAYLOAD`
        :returns: url ready to pass to ``make_request``
        """
        if headers is None:
            headers = {}

        parsed = urlparse(url)
        host = parsed.netloc
        canonical_uri = parsed.path or "/"

        # Canonical query string: parse, sort, and re-encode per SigV4 rules.
        qs_pairs = parse_qsl(parsed.query, keep_blank_values=True)
        qs_pairs.sort(key=lambda kv: (kv[0], kv[1]))
        canonical_querystring = "&".join(
            f"{quote(k, safe='')}={quote(v, safe='')}" for k, v in qs_pairs
        )

        now = datetime.datetime.now(datetime.timezone.utc)
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = now.strftime("%Y%m%d")

        # Collect all headers for signing (lowercased keys, trimmed values).
        headers_for_signing: dict[str, str] = {
            k.lower(): v.strip() for k, v in headers.items()
        }
        headers_for_signing["host"] = host
        headers_for_signing["x-amz-date"] = amz_date
        headers_for_signing["x-amz-content-sha256"] = payload_hash

        signed_header_names = sorted(headers_for_signing)
        canonical_headers = "".join(
            f"{k}:{headers_for_signing[k]}\n" for k in signed_header_names
        )
        signed_headers_str = ";".join(signed_header_names)

        # Step 1 -- Canonical request
        canonical_request = "\n".join(
            [
                method.upper(),
                canonical_uri,
                canonical_querystring,
                canonical_headers,
                signed_headers_str,
                payload_hash,
            ]
        )

        # Step 2 -- String to sign
        credential_scope = (
            f"{date_stamp}/{self.region}/{self.service}/aws4_request"
        )
        string_to_sign = "\n".join(
            [
                ALGORITHM,
                amz_date,
                credential_scope,
                _sha256_hex(canonical_request.encode("utf-8")),
            ]
        )

        # Step 3 -- Signature
        signing_key = _derive_signing_key(
            self.secret_key, date_stamp, self.region, self.service
        )
        signature = hmac.new(
            signing_key, string_to_sign.encode("utf-8"), hashlib.sha256
        ).hexdigest()

        # Step 4 -- Assemble output headers (host excluded; aiohttp sets it)
        # result = dict(headers)
        # result["x-amz-date"] = amz_date
        # result["x-amz-content-sha256"] = payload_hash
        # result["Authorization"] = (
        #     f"{ALGORITHM} "
        #     f"Credential={self.access_key}/{credential_scope}, "
        #     f"SignedHeaders={signed_headers_str}, "
        #     f"Signature={signature}"
        # )
        # return result

        spob = 'aws4_request'
        signed_url = url + '?X-Amz-Algorithm=AWS4-HMAC-SHA256'
        signed_url = signed_url + f'&X-Amz-Credential={self.access_key}%2F{date_stamp}%2F{self.region}%2F{self.service}%2F{spob}'
        signed_url = signed_url + f'&X-Amz-Date={amz_date}'
        signed_url = signed_url + '&X-Amz-Expires=3600'
        signed_url = signed_url + '&X-Amz-SignedHeaders=host'
        return signed_url + f'&X-Amz-Signature={signature}'
