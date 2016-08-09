import re
import json
import logging

import aiohttp
from geoip import geolite2

import waterbutler
from waterbutler import settings


logger = logging.getLogger(__name__)


async def log_download(action, payload, request, api_version, size=0):
    if settings.KEEN_PRIVATE_PROJECT_ID is None or action not in ('download_file', 'download_zip', ):
        return

    logger.info('Request IP is: {}'.format(request['ip']))
    location = None
    if request['ip'] and re.match('\d+\.\d+\.\d+\.\d+', request['ip']):  # needs IPv4 format
        location = geolite2.lookup(request['ip'])

    referrer = request.pop('referrer', '')
    is_mfr_render = request.pop('is_mfr_render')
    file_meta = payload.serialize()
    keen_payload = {
        'meta': {
            'wb_version': waterbutler.__version__,
            'api_version': api_version,
            'epoch': 1,
        },
        'request': request,  # .info, .geo added via keen addons
        'referrer': {
            'url': referrer,
            'info': {},  # .info added via keen addons
        },
        'action': {
            'type': action,
            'bytes': size,
            'is_mfr_render': is_mfr_render,
        },
        'file': file_meta,
        'anon': {
            'continent': getattr(location, 'continent', None),
            'country': getattr(location, 'country', None),
        },
        'keen': {
            'addons': [
                {
                    'name': 'keen:url_parser',
                    'input': {
                        'url': 'request.url'
                    },
                    'output': 'request.info'
                },
                {  # private
                    'name': 'keen:ip_to_geo',
                    'input': {
                        'ip': 'request.ip'
                    },
                    'output': 'request.geo',
                },
                {
                    'name': 'keen:referrer_parser',
                    'input': {
                        'referrer_url': 'referrer.url',
                        'page_url': 'request.url'
                    },
                    'output': 'referrer.info'
                },
                {
                    'name': 'keen:url_parser',
                    'input': {
                        'url': 'referrer.url'
                    },
                    'output': 'referrer.info'
                },
            ],
        },
    }

    # synthetic fields to make Keen queries easier/prettier
    keen_payload['file']['full_path'] = '/'.join([
        '', file_meta['resource'], file_meta['provider'], file_meta['path'].lstrip('/')
    ])
    keen_payload['file']['full_materialized'] = '/'.join([
        '', file_meta['resource'], file_meta['provider'], file_meta['materialized'].lstrip('/')
    ])
    keen_payload['action']['subtype'] = 'view_file' if action == 'download_file' and is_mfr_render else action

    collection = 'file_access'

    # send the private payload
    await _send_to_keen(keen_payload, collection, settings.KEEN_PRIVATE_PROJECT_ID,
                        settings.KEEN_PRIVATE_WRITE_KEY, 'private')

    # sanitize then send the public payload
    for field in ('ip', 'headers', 'ua', ):
        del keen_payload['request'][field]
    for addon in keen_payload['keen']['addons']:
        if addon['name'] in ('keen:ip_to_geo',):
            keen_payload['keen']['addons'].remove(addon)
    await _send_to_keen(keen_payload, collection, settings.KEEN_PUBLIC_PROJECT_ID,
                        settings.KEEN_PUBLIC_WRITE_KEY, 'public')


async def _send_to_keen(payload, collection, project_id, write_key, domain='private'):
    serialized = json.dumps(payload).encode('UTF-8')
    headers = {
        'Content-Type': 'application/json',
        'Authorization': write_key,
    }
    url = '{0}/{1}/projects/{2}/events/{3}'.format(settings.KEEN_API_BASE_URL,
                                                   settings.KEEN_API_VERSION,
                                                   project_id, collection)

    async with await aiohttp.request('POST', url, headers=headers, data=serialized) as resp:
        if resp.status == 201:
            logger.info('Successfully logged download to {} collection in {} Keen'.format(
                collection, domain
            ))
        else:
            raise Exception('Failed to log to {} collection in {} Keen. Status: {} Error: {}'.format(
                collection, domain, str(int(resp.status)), await resp.read()
            ))
        return
