import base64
import hashlib
import logging
from email.utils import formatdate
from urllib.parse import urlsplit

from cryptography.exceptions import UnsupportedAlgorithm
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey

logger = logging.getLogger(__name__)

ALGORITHM = "rsa-sha256"
SIGNATURE_VERSION = "1"
DEFAULT_CONTENT_TYPE = "application/octet-stream"

BASE_SIGNED_HEADERS = ["date", "(request-target)", "host"]
BODY_SIGNED_HEADERS = ["content-length", "content-type", "x-content-sha256"]
BODY_METHODS = {"PUT", "POST", "PATCH"}


def load_private_key(private_key_content: str | bytes) -> RSAPrivateKey:
    """Load a PEM RSA private key, raising ValueError if it can't be parsed."""
    if isinstance(private_key_content, str):
        private_key_content = private_key_content.encode("ascii")
    try:
        return serialization.load_pem_private_key(private_key_content, password=None)
    except (ValueError, TypeError, UnsupportedAlgorithm) as exc:
        raise ValueError(f"Could not load OCI private key: {exc}") from exc


def _compute_body_sha256(body: bytes) -> str:
    """Base64-encoded SHA-256 of the body (OCI's x-content-sha256 value)."""
    return base64.b64encode(hashlib.sha256(body).digest()).decode("ascii")


def _build_signing_string(
    header_names: list[str],
    headers: dict[str, str],
    method: str,
    path: str,
    host: str,
) -> str:
    """Render the named headers as newline-joined ``name: value`` lines.

    ``(request-target)`` and ``host`` come from the request line; everything else is
    looked up in *headers*.  Order follows *header_names*.
    """
    lines = []
    for name in header_names:
        if name == "(request-target)":
            lines.append(f"(request-target): {method.lower()} {path}")
        elif name == "host":
            lines.append(f"host: {host}")
        else:
            lines.append(f"{name}: {headers[name]}")
    return "\n".join(lines)


def _rsa_sha256_sign(private_key: RSAPrivateKey, signing_string: str) -> str:
    """RSA-SHA256 (PKCS#1 v1.5) signature of *signing_string*, base64-encoded."""
    signature = private_key.sign(
        signing_string.encode("ascii"), padding.PKCS1v15(), hashes.SHA256()
    )
    return base64.b64encode(signature).decode("ascii")


class OCISigner:
    """Signs OCI Object Storage native-API requests with a customer RSA API key.

    ``keyId`` is the usual ``<tenancy>/<user>/<fingerprint>`` triple; the matching PEM
    private key does the signing.
    """

    def __init__(
        self,
        tenancy: str,
        user: str,
        fingerprint: str,
        private_key_content: str | bytes,
    ) -> None:
        self.api_key = f"{tenancy}/{user}/{fingerprint}"
        self._private_key = load_private_key(private_key_content)

    def sign_request(
        self,
        method: str,
        url: str,
        headers: dict[str, str] | None = None,
        body: bytes | None = None,
    ) -> dict[str, str]:
        """Return *headers* with the OCI signing headers added.

        :param method: HTTP verb
        :param url: fully-qualified request URL
        :param headers: extra headers to keep on the request
        :param body: request body for PUT/POST/PATCH; ignored for other methods
        """
        method = method.upper()
        result = dict(headers) if headers else {}

        split = urlsplit(url)
        host = split.netloc
        request_target = split.path or "/"
        if split.query:
            request_target = f"{request_target}?{split.query}"

        date_header = formatdate(usegmt=True)
        signing_values = {"date": date_header}
        signed_headers = list(BASE_SIGNED_HEADERS)

        if method in BODY_METHODS:
            body = body or b""
            signing_values["content-length"] = str(len(body))
            signing_values["content-type"] = DEFAULT_CONTENT_TYPE
            signing_values["x-content-sha256"] = _compute_body_sha256(body)
            signed_headers += BODY_SIGNED_HEADERS
            result.update({name: signing_values[name] for name in BODY_SIGNED_HEADERS})

        signing_string = _build_signing_string(
            signed_headers, signing_values, method, request_target, host
        )
        signature = _rsa_sha256_sign(self._private_key, signing_string)

        result["date"] = date_header
        result["host"] = host
        result["authorization"] = (
            f'Signature algorithm="{ALGORITHM}",'
            f'headers="{" ".join(signed_headers)}",'
            f'keyId="{self.api_key}",'
            f'signature="{signature}",'
            f'version="{SIGNATURE_VERSION}"'
        )
        return result
