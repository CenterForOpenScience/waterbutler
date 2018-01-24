import re
import json
import typing
from urllib.parse import urlparse, quote

from waterbutler.core.path import WaterButlerPath
from waterbutler.providers.googlecloud import settings as pd_settings


def get_obj_name(path: WaterButlerPath, is_folder: bool=False) -> str:
    """Get the object name of the object with the given Waterbutler Path.

    The "object name" is used by Google Cloud Storage API in path or query parameters to refer
    to the object as an external identifier. The "folder" name ends with a '/' while the "file"
    name does not. In addition, both do not start with a '/'.

    :param path: the WaterbutlerPath
    :param is_folder: the folder flag
    :rtype: str
    """

    relative_path = path.path.lstrip('/')

    if is_folder:
        assert relative_path.endswith('/')
    else:
        assert not relative_path.endswith('/')

    return relative_path


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


def build_payload_from_req_map(req_list_failed: list, req_map: dict) -> str:
    """Build the payload of the batch request for the failed requests given a list of failed ones
    an previously-built map of request id and partial payload.

    :param req_list_failed: the list of id of the failed requests
    :param req_map: the requests map of id and payload parts
    :rtype str:
    """

    payload = ''
    for req_id in req_list_failed:
        payload += req_map.get(req_id)
    payload += '--{}--'.format(pd_settings.BATCH_BOUNDARY)

    return payload


def parse_batch_delete_resp(resp: str) -> list:
    """Parse the response from batch delete.  Find failed requests and return a list of their id.

    1. Expected: HTTP 204 No Content
    2. Ignored: HTTP 404 Not Found

    "HTTP 404 Not Found" should never happen if the batch request is built correctly and if the data
    on the Cloud Storage is not corrupted.  However, we need to ignore it when it happened to
    prevent the delete request from hanging.

    :param resp: the response string
    :rtype list:
    """

    req_list_failed = []

    delimiter = 'Content-Type: application/http'
    expected_part = 'HTTP/1.1 204 No Content'
    ignored_part = 'HTTP/1.1 404 Not Found'
    resp_parts = resp.split(delimiter)
    for resp_part in resp_parts:
        if expected_part not in resp_part and ignored_part not in resp_part:
            req_id = get_req_id_from_resp_part(resp_part)
            if req_id > 0:
                req_list_failed.append(req_id)

    return req_list_failed


def parse_batch_copy_resp(resp: str) -> typing.Tuple[typing.List[dict], typing.List[int]]:
    """Parse the response from batch copy.  Return a list of raw metadata of the successful requests
    and a list of id for the failed requests.

    1. Expected: HTTP 200 OK
    2. Ignored: HTTP 404 Not Found

    "HTTP 404 Not Found" should never happen if the batch request is built correctly and if the data
    on the Cloud Storage is not corrupted.  However, we need to ignore it when it happened to
    prevent the copy request from hanging.

    :param resp: the response string
    :rtype list:
    :rtype list:
    """

    req_list_failed = []
    metadata_list = []

    delimiter = 'Content-Type: application/http'
    expected_part = 'HTTP/1.1 200 OK'
    ignored_part = 'HTTP/1.1 404 Not Found'

    resp_parts = resp.split(delimiter)
    for resp_part in resp_parts:
        if expected_part in resp_part:
            metadata = get_metadata_from_resp_part(resp_part)
            if metadata:
                metadata_list.append(metadata)
        elif ignored_part not in resp_part:
            req_id = get_req_id_from_resp_part(resp_part)
            if req_id > 0:
                req_list_failed.append(req_id)

    return metadata_list, req_list_failed


def get_metadata_from_resp_part(resp_part: str) -> dict:
    """Retrieve the metadata part of an successful response. Parse it and return a dict.

    :param resp_part: the response part that contains either one or no metadata
    :rtype dict:
    """

    regex = r'{(.|\r\n|\r|\n)*}'
    matched = re.search(regex, resp_part)

    if matched:
        try:
            metadata = matched.group(0)
            return dict(json.loads(metadata))
        except (IndexError, TypeError, json.JSONDecodeError):
            return {}
    return {}


def get_req_id_from_resp_part(resp_part: str) -> int:
    """Retrieve the request id from partial response, which is the integer part of the Content-ID.

    :param resp_part: the response part that contains either one or no "Content-ID" header
    :rtype int:     -1 for error
                     0 for no header found
               [1-100] for id found
    """

    regex = r'Content-ID: <response(.*)\+(.*)>(\r\n|\r|\n)'
    matched = re.search(regex, resp_part)

    if matched:
        try:
            header = matched.group(0)
            return int((header.split('+')[1]).split('>')[0])
        except IndexError:
            return -1
    return 0
