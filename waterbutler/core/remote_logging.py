import time
import asyncio
import logging

from waterbutler.core import utils
from waterbutler.tasks import settings as task_settings


logger = logging.getLogger(__name__)


@utils.async_retry(retries=5, backoff=5)
async def log_to_callback(action, source=None, destination=None, start_time=None, errors=[]):
    if action in ('download_file', 'download_zip'):
        logger.debug('Not logging for {} action'.format(action))
        return

    auth = getattr(destination, 'auth', source.auth)
    log_payload = {
        'action': action,
        'auth': auth,
        'time': time.time() + 60,
        'errors': errors,
    }

    if start_time:
        log_payload['email'] = time.time() - start_time > task_settings.WAIT_TIMEOUT

    if action in ('move', 'copy'):
        log_payload['source'] = source.serialize()
        log_payload['destination'] = destination.serialize()
    else:
        log_payload['metadata'] = source.serialize()
        log_payload['provider'] = log_payload['metadata']['provider']

    if action in ('download_file', 'download_zip'):
        logger.info('Not logging for {} action'.format(action))
        return

    resp = await utils.send_signed_request('PUT', auth['callback_url'], log_payload)
    resp_data = await resp.read()

    if resp.status // 100 != 2:
        raise Exception(
            'Callback for {} request failed with {!r}, got {}'.format(
                action, resp, resp_data.decode('utf-8')
            )
        )

    logger.info('Callback for {} request succeeded with {}'.format(action, resp_data.decode('utf-8')))


def log_file_action(action, source, api_version, destination=None, request={},
                    start_time=None, errors=None, size=None):
    return [
        log_to_callback(action, source=source, destination=destination,
                        start_time=start_time, errors=errors,),
    ]


async def wait_for_log_futures(*args, **kwargs):
    return await asyncio.wait(
        log_file_action(*args, **kwargs),
        return_when=asyncio.ALL_COMPLETED
    )
