import re
import base64
import typing
import binascii
from aiohttp import MultiDict
from urllib.parse import urlparse, quote

from waterbutler.core.path import WaterButlerPath
from waterbutler.core.exceptions import WaterButlerError


def get_obj_name(path: WaterButlerPath, is_folder: bool=False) -> str:
    """Get the object name of the file or folder with the given Waterbutler Path.

    Quirks:

        Object Name is used by the Google Cloud Storage API (both XML and JSON) in the request path,
        queries and headers to identify the object.  Folders' names always end with a ``'/'`` and
        files' names never do. In addition, neither of them starts with a ``'/'``.

    :param path: the WaterButler path of the object
    :param is_folder: the folder flag
    :rtype str:
    """

    return validate_path_or_name(path.path.lstrip('/'), is_folder=is_folder)


def build_path(obj_name: str, is_folder: bool=False) -> str:
    """Convert the object name to a path string which can pass WaterButler path validation.

    :param obj_name: the object name of the object
    :param is_folder: the folder flag
    :rtype str:
    """

    return validate_path_or_name(
        obj_name if obj_name.startswith('/') else '/{}'.format(obj_name),
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
    """Build URL with ``'/'`` encoded in path segments and queries for Google Cloud API.

    Quirk:

        Objects' names in Google Cloud Storage contain ``'/'`` which must be encoded.  WB calls
        ``urllib.parse.quote()`` with optional argument ``safe=''``.  The default is ``safe='/'``.

    :param base: the base URL
    :param segments: the path segments tuple
    :param query: the queries dictionary
    :rtype: str
    """

    parsed_base = urlparse(base).geturl()

    if not segments:
        path = ''
    else:
        path_segments = []
        for segment in segments:
            # Do not strip leading or trailing `/` from segments
            path_segments.append(quote(segment, safe=''))
        path = '/'.join(path_segments)

    if not query:
        queries = ''
    else:
        query_pairs = []
        for key, value in query.items():
            key_value_pair = [quote(key, safe=''), quote(value, safe='')]
            query_pairs.append('='.join(key_value_pair))
        queries = '?' + '&'.join(query_pairs)

    path = '/' + path if path else ''

    return ''.join([parsed_base, path, queries])


def decode_and_hexlify_hashes(hash_str: str) -> typing.Union[str, None]:
    """Decode a Base64-encoded string and return a hexlified string.

    Quirks:

        This helper function inputs and outputs string.  However, both ``base64.b64decode()`` and
        ``binascii.hexlify()`` operate on bytes.  WB must call ``.encode()`` and ``.decode()`` to
        convert bytes and string back and forth.

    :param hash_str: the Base64-encoded hash string
    :rtype str:
    """

    return binascii.hexlify(base64.b64decode(hash_str.encode())).decode() if hash_str else None


def build_canonical_ext_headers_str(headers: dict) -> str:
    """Build a string for canonical extension headers, which is part of the string to sign.

    Quirks:

        Google Cloud Storage has very strict rules for building this string. See: https://cloud.goog
        le.com/storage/docs/access-control/signed-urls#about-canonical-extension-headers

        For this very limited version of the Google Cloud provider, only ``_intra_copy_file`` uses
        the canonical extension header and it uses only one.  There is no need for extra effort to
        remove ``x-goog-encryption-key`` and ``x-goog-encryption-key-sha256`` or to perform a lexi-
        cographical sort.  TODO [Phase 2]: fully implement this function when needed

    :param headers: the canonical extension headers
    :rtype str:
    """

    # Return ``''`` instead of ``None`` so that it can be properly concatenated
    if not headers:
        return ''

    if len(headers) != 1:
        raise WaterButlerError('The limited provider only supports one canonical extension header.')

    headers_str = ''
    for key, value in headers.items():
        headers_str += '{}:{}\n'.format(key.strip().lower(), value.strip())

    return headers_str


def verify_raw_google_hash_header(google_hash: str) -> bool:
    """Verify the format of the raw value of the "x-goog-hash" header.

    Note: For now this method is used for test only.

    :param google_hash: the raw value of the "x-goog-hash" header
    :rtype bool:
    """

    return bool(re.match(r'(crc32c=[A-Za-z0-9+/=]+),(md5=[A-Za-z0-9+/=]+)', google_hash))


def get_multi_dict_from_python_dict(resp_headers_dict: dict) -> MultiDict:
    """Construct an ``aiohttp.MultiDict`` instance from a Python dictionary.

    Note: For now, this method is used for test only.

    Quirks:

        Neither Python dictionary nor JSON supports multi-value key.  The response headers returned
        by ``aiohttp`` is of immutable type ``aiohttp._multidict.CIMultiDictProxy``.  WB uses the
        parent abstract class ``aiohttp.MultiDict`` instead for both files and folders in test.

    :param resp_headers_dict: the raw response headers dictionary
    :rtype MultiDict:
    """

    resp_headers = MultiDict(resp_headers_dict)
    google_hash = resp_headers.get('x-goog-hash', None)
    if google_hash:
        assert verify_raw_google_hash_header(google_hash)
        google_hash_list = google_hash.split(',')
        resp_headers.pop('x-goog-hash')
        for google_hash in google_hash_list:
            resp_headers.add('x-goog-hash', google_hash)

    return resp_headers
