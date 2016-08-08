import json
import logging

import aiohttp

import waterbutler
from waterbutler import settings


logger = logging.getLogger(__name__)


async def log_download(action, payload, request, api_version, size=0):
    if settings.KEEN_PROJECT_ID is None or action not in ('download_file', 'download_zip', ):
        return

    url = '{0}/{1}/projects/{2}/events/{3}'.format(settings.KEEN_API_BASE_URL,
                                                   settings.KEEN_API_VERSION,
                                                   settings.KEEN_PROJECT_ID,
                                                   'file_access')

    referrer = request.pop('referrer', '')
    is_mfr_render = request.pop('is_mfr_render')
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
        'file': payload.serialize(),
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

    serialized = json.dumps(keen_payload).encode('UTF-8')
    headers = {
        'Content-Type': 'application/json',
        'Authorization': settings.KEEN_WRITE_KEY,
    }

    async with await aiohttp.request('POST', url, headers=headers, data=serialized) as resp:
        if resp.status == 201:
            logger.info('Successfully logged download to Keen')
        else:
            raise Exception('Failed to log to Keen. Status: {} Error: {}'.format(
                str(int(resp.status)), await resp.read()
            ))
        return
