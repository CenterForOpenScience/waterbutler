import json
import time
import asyncio
import logging

import furl
import aiohttp

from waterbutler import settings
from waterbutler.core import utils
from waterbutler.sizes import KBs, MBs, GBs
from waterbutler.version import __version__
from waterbutler.tasks import settings as task_settings


logger = logging.getLogger(__name__)


@utils.async_retry(retries=5, backoff=5)
async def log_to_callback(action, source=None, destination=None, start_time=None, errors=[],
                          request={}):
    """PUT a logging payload back to the callback given by the auth provider."""

    auth = getattr(destination, 'auth', source.auth)
    ref_url_domain = ''
    log_payload = {
        'action': action,
        'action_meta': {},
        'auth': auth,
        'time': time.time() + 60,
        'errors': errors,
    }

    if request:
        log_payload['request_meta'] = {
            'method': request['request']['method'],
            'url': request['request']['url'],
            'referrer': request['referrer']['url'],
            'user_agent': request['tech']['ua'],
        }
        referrer = request['referrer']['url']
        if referrer:
            ref_url = furl.furl(request['referrer']['url'])
            ref_url_port = ':{}'.format(ref_url.port) if ref_url.port else ''
            ref_url_domain = '{}://{}{}'.format(ref_url.scheme, ref_url.host, ref_url_port)

    if start_time:
        log_payload['email'] = time.time() - start_time > task_settings.WAIT_TIMEOUT

    if action in ('move', 'copy'):
        log_payload['source'] = source.serialize()
        log_payload['destination'] = destination.serialize()
    else:
        log_payload['metadata'] = source.serialize()
        log_payload['provider'] = log_payload['metadata']['provider']

    if action in ['download_file', 'download_zip']:
        is_mfr_render = (ref_url_domain == settings.MFR_DOMAIN or
                         settings.MFR_IDENTIFYING_HEADER in request['request']['headers'])
        log_payload['action_meta']['is_mfr_render'] = is_mfr_render

    resp_status, resp_data = await utils.send_signed_request('PUT', auth['callback_url'], log_payload)

    if resp_status // 100 != 2:
        raise Exception(
            'Callback for {} request failed with {!r}, got {}'.format(
                action, resp_status, resp_data.decode('utf-8')
            )
        )

    logger.info('Callback for {} request succeeded with {}'.format(action, resp_data.decode('utf-8')))


async def log_to_keen(action, api_version, request, source, destination=None, errors=None,
                      bytes_downloaded=0, bytes_uploaded=0):
    """Send events to Keen describing the action that occurred.  A scrubbed version of the payload
    suitable for public display is also sent."""
    if not settings.KEEN_ENABLE_LOGGING:
        return

    keen_payload = {
        'meta': {
            'wb_version': __version__,
            'api_version': api_version,
            'epoch': 1,
        },
        'request': request['request'],  # .info added via keen addons
        'tech': request['tech'],  # .info added via keen addons
        'anon': {  # intended for anonymized geolocation, never implemented
            'continent': None,
            'country': None,
        },
        'action': {
            'type': action,
            'bytes_downloaded': bytes_downloaded,
            'bytes_uploaded': bytes_uploaded,
            'bytes_dl': _format_bytes(bytes_downloaded),
            'bytes_up': _format_bytes(bytes_uploaded),
            'errors': errors,
            'is_mfr_render': False,
        },
        'files': {
            'source': _munge_file_metadata(source.serialize()),
            'destination': None if destination is None else _munge_file_metadata(destination.serialize())
        },
        'auth': {
            'source': source.auth,
            'destination': None if destination is None else destination.auth,
        },
        'providers': {
            'source': None,
            'destination': None,
        },
        'geo': {},  # added via keen addons
        'keen': {
            'addons': [
                {
                    'name': 'keen:url_parser',
                    'input': {
                        'url': 'request.url'
                    },
                    'output': 'request.info',
                },
                {  # private
                    'name': 'keen:ip_to_geo',
                    'input': {
                        'ip': 'tech.ip',
                        'remove_ip_property': True,
                    },
                    'output': 'geo',
                },
                {  # private
                    'name': 'keen:ua_parser',
                    'input': {
                        'ua_string': 'tech.ua',
                    },
                    'output': 'tech.info',
                },
            ],
        },
    }

    if settings.MFR_IDENTIFYING_HEADER in request['request']['headers']:
        keen_payload['action']['is_mfr_render'] = True

    if request['referrer']['url'] is not None:
        if request['referrer']['url'].startswith('{}'.format(settings.MFR_DOMAIN)):
            keen_payload['action']['is_mfr_render'] = True

        keen_payload['referrer'] = request['referrer']  # .info added via keen addons
        keen_payload['keen']['addons'].append({
            'name': 'keen:referrer_parser',
            'input': {
                'referrer_url': 'referrer.url',
                'page_url': 'request.url'
            },
            'output': 'referrer.info'
        })
        keen_payload['keen']['addons'].append({
            'name': 'keen:url_parser',
            'input': {
                'url': 'referrer.url'
            },
            'output': 'referrer.info',
        })

    if hasattr(source, 'provider'):
        keen_payload['providers']['source'] = source.provider.provider_metrics.serialize()

    if destination is not None and hasattr(destination, 'provider'):
        keen_payload['providers']['destination'] = destination.provider.provider_metrics.serialize()

    # send the private payload
    if settings.KEEN_PRIVATE_LOG_ACTIONS:
        await _send_to_keen(keen_payload, 'file_access', settings.KEEN_PRIVATE_PROJECT_ID,
                            settings.KEEN_PRIVATE_WRITE_KEY, action, domain='private')

    if (
        errors is not None or
        action not in ('download_file', 'download_zip') or
        keen_payload['action']['is_mfr_render'] or
        not settings.KEEN_PUBLIC_LOG_ACTIONS
    ):
        return

    # build and ship the public file stats payload
    file_metadata = keen_payload['files']['source']
    public_payload = _build_public_file_payload(action, request, file_metadata)
    await _send_to_keen(public_payload, 'file_stats', settings.KEEN_PUBLIC_PROJECT_ID,
                        settings.KEEN_PUBLIC_WRITE_KEY, action, domain='public')


@utils.async_retry(retries=5, backoff=5)
async def _send_to_keen(payload, collection, project_id, write_key, action, domain='private'):
    """Serialize and send an event to Keen.  If an error occurs, try up to five more times.
    Will raise an excpetion if the event cannot be sent."""

    serialized = json.dumps(payload).encode('UTF-8')
    logger.debug("Serialized payload: {}".format(serialized))
    headers = {
        'Content-Type': 'application/json',
        'Authorization': write_key,
    }
    url = '{0}/{1}/projects/{2}/events/{3}'.format(settings.KEEN_API_BASE_URL,
                                                   settings.KEEN_API_VERSION,
                                                   project_id, collection)

    async with aiohttp.request('POST', url, headers=headers, data=serialized) as resp:
        if resp.status == 201:
            logger.info('Successfully logged {} to {} collection in {} Keen'.format(action, collection, domain))
        else:
            raise Exception('Failed to log {} to {} collection in {} Keen. Status: {} Error: {}'.format(
                action, collection, domain, str(int(resp.status)), await resp.read()
            ))
        return


def log_file_action(action, source, api_version, destination=None, request={},
                    start_time=None, errors=None, bytes_downloaded=None, bytes_uploaded=None):
    """Kick off logging actions in the background. Returns array of asyncio.Tasks."""
    return [
        log_to_callback(action, source=source, destination=destination,
                        start_time=start_time, errors=errors, request=request,),
        asyncio.ensure_future(
            log_to_keen(action, source=source, destination=destination,
                        errors=errors, request=request, api_version=api_version,
                        bytes_downloaded=bytes_downloaded, bytes_uploaded=bytes_uploaded,),
        ),
    ]


async def wait_for_log_futures(*args, **kwargs):
    """Background actions that are still running when a celery task returns may not complete.
    This method allows the celery task to wait for logging to finish before returning."""
    return await asyncio.wait(
        log_file_action(*args, **kwargs),
        return_when=asyncio.ALL_COMPLETED
    )


def _munge_file_metadata(metadata):
    if metadata is None:
        return None

    try:
        file_extra = metadata.pop('extra')
    except KeyError:
        pass
    else:
        metadata['extra'] = {
            'common': {},
            metadata['provider']: file_extra,
        }

    # synthetic fields to make Keen queries easier/prettier
    metadata['full_path'] = '/'.join([
        '', metadata['resource'], metadata['provider'], metadata['path'].lstrip('/')
    ])
    metadata['full_materialized'] = '/'.join([
        '', metadata['resource'], metadata['provider'], metadata['materialized'].lstrip('/')
    ])

    return metadata


def _build_public_file_payload(action, request, file_metadata):
    public_payload = {
        'meta': {
            'epoch': 1,
        },
        'request': {
            'url': request['request']['url']
        },
        'anon': {
            'country': None,
            'continent': None,
        },
        'action': {
            'type': action,
        },
        'file': file_metadata,
        'keen': {
            'addons': [
                {
                    'name': 'keen:url_parser',
                    'input': {
                        'url': 'request.url'
                    },
                    'output': 'request.info',
                },
            ],
        },
    }

    try:
        public_payload['node'] = {'id': file_metadata['resource']}
    except KeyError:
        pass

    if request['referrer']['url'] is not None:
        public_payload['referrer'] = request['referrer']  # .info added via keen addons
        public_payload['keen']['addons'].append({
            'name': 'keen:referrer_parser',
            'input': {
                'referrer_url': 'referrer.url',
                'page_url': 'request.url'
            },
            'output': 'referrer.info'
        })
        public_payload['keen']['addons'].append({
            'name': 'keen:url_parser',
            'input': {
                'url': 'referrer.url'
            },
            'output': 'referrer.info',
        })

    return public_payload


def _scrub_headers_for_keen(payload, MAX_ITERATIONS=10):
    """ Scrub unwanted characters like \\.\\ from the keys in the keen payload """

    scrubbed_payload = {}
    for key in sorted(payload):
        scrubbed_key = key.replace('.', '-')

        # if our new scrubbed key is already in the payload, we need to increment it
        if scrubbed_key in scrubbed_payload:
            for i in range(1, MAX_ITERATIONS + 1):  # try MAX_ITERATION times, then give up & drop it
                incremented_key = '{}-{}'.format(scrubbed_key, i)
                if incremented_key not in scrubbed_payload:  # we found an unused key!
                    scrubbed_payload[incremented_key] = payload[key]
                    break
        else:
            scrubbed_payload[scrubbed_key] = payload[key]

    return scrubbed_payload


def _serialize_request(request):
    """Serialize the original request so we can log it across celery."""
    if request is None:
        return {}

    headers_dict = {}
    for (k, v) in sorted(request.headers.get_all()):
        if k not in ('Authorization', 'Cookie', 'User-Agent',):
            headers_dict[k] = v

    headers_dict = _scrub_headers_for_keen(headers_dict)
    serialized = {
        'tech': {
            'ip': request.remote_ip,
            'ua': request.headers['User-Agent'],
        },
        'request': {
            'method': request.method,
            'url': request.full_url(),
            'time': request.request_time(),
            'headers': headers_dict,
        },
        'referrer': {
            'url': None,
        },
    }

    if 'Referer' in request.headers:
        serialized['referrer']['url'] = request.headers['Referer']

    return serialized


def _format_bytes(nbr_bytes):
    if nbr_bytes is None:
        return {}

    return {
        'b': nbr_bytes,
        'kb': nbr_bytes / KBs,
        'mb': nbr_bytes / MBs,
        'gb': nbr_bytes / GBs,
    }
