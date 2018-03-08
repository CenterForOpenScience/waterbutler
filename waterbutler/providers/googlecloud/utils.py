import re
import base64
import binascii
from urllib.parse import urlparse, quote

import typing

from waterbutler.core.path import WaterButlerPath
from waterbutler.core.exceptions import WaterButlerError


def get_obj_name(path: WaterButlerPath, is_folder: bool=False) -> str:
    """Get the object name of the object with the given Waterbutler Path.

    The "object name" is used by Google Cloud Storage API in path or query parameters to refer
    to the object as an external identifier. The "folder" name ends with a '/' while the "file"
    name does not. In addition, both do not start with a '/'.

    :param path: the WaterbutlerPath
    :param is_folder: the folder flag
    :rtype: str
    """

    return validate_path_or_name(path.path.lstrip('/'), is_folder=is_folder)


def build_path(obj_name: str, is_folder: bool=False) -> str:
    """Convert the object name to a path string which can pass validation.

    :param obj_name: the object name of the objects
    :param is_folder: the folder flag
    :rtype str:
    """

    return validate_path_or_name(
        obj_name if obj_name.startswith('/') else '/' + obj_name,
        is_folder=is_folder
    )


def validate_path_or_name(path_or_name: str, is_folder: bool=False) -> str:
    """Validate that path or object name.

    :param path_or_name: the path or the object name
    :param is_folder: the folder flag
    :rtype str:
    """

    if is_folder:
        assert path_or_name.endswith('/')
    else:
        assert not path_or_name.endswith('/')

    return path_or_name


def build_url(base: str, *segments, **query) -> str:
    """Customized URL building function for Google Cloud Storage API.  Encode '/' in URL path and
    query parameters. Please note that ``quote()`` is called with ``safe=''``.  This is because
    objects' names in Google Cloud Storage contains '/'s which need to be encoded.

    :param base: the base URL
    :param segments: the path segments tuple
    :param query: the query pairs dict
    :rtype: str
    """

    parsed_base = urlparse(base).geturl()

    if segments:
        path_segments = []
        for segment in segments:
            # Do not strip leading or trailing `/` from segments
            path_segments.append(quote(segment, safe=''))
        path = '/'.join(path_segments)
    else:
        path = ''

    if query:
        query_pairs = []
        for key, value in query.items():
            key_value_pair = [quote(key, safe=''), quote(value, safe='')]
            query_pairs.append('='.join(key_value_pair))
        queries = '?' + '&'.join(query_pairs)
    else:
        queries = ''

    path = '/' + path if path else ''

    return ''.join([parsed_base, path, queries])


def decode_and_hexlify_hashes(hash_str: str) -> typing.Union[str, None]:
    """Decode a Base64-encoded string and return a hexlified string.

    This helper function inputs and outputs "string".  However, both ``base64.b64decode()`` and
    ``binascii.hexlify()`` operates on "bytes".  Must call ``.encode()`` and ``.decode()`` perform
    conversion between "bytes" and "string".

    :param hash_str: the Base64-encoded hash string
    :rtype str:
    """

    return binascii.hexlify(base64.b64decode(hash_str.encode())).decode() if hash_str else None


def build_canonical_ext_headers_str(headers: dict) -> str:
    """Build a string for canonical extension headers, which is part of the string to sign.

    Here is the rule to follow when building the string:
        cloud.google.com/storage/docs/access-control/signed-urls#about-canonical-extension-headers

    For the limited version of the Google Cloud provider, only ``_intra_copy_file`` uses a canonical
    extension header.  TODO [Phase 1.5]: fully implement this function

    :param headers: the canonical extension headers
    :rtype str:
    """

    # Return an empty string instead of ``None`` so it can be properly concatenated without if check
    if not headers:
        return ''

    if len(headers) != 1:
        raise WaterButlerError('The limited provider only supports one canonical extension header.')

    for key, value in headers.items():
        return '{}:{}\n'.format(key.strip().lower(), value.strip())


def verify_raw_google_hash_header(google_hash: str) -> bool:
    """Verify the format of the raw value of the "x-goog-hash" header.  This is used for test only.

    :param google_hash: the raw value of the "x-goog-hash" header
    :rtype bool:
    """

    return True if re.match(r'(crc32c=.*==),(md5=.*==)', google_hash) else False
