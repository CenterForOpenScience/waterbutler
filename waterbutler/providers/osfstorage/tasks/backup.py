import os
import json
import asyncio
from http import HTTPStatus

import aiohttp
from boto.glacier.layer2 import Layer2
from boto.glacier.exceptions import UnexpectedHTTPResponseError

from waterbutler.core import signing
from waterbutler.core.utils import async_retry
from waterbutler.providers.osfstorage import settings
from waterbutler.providers.osfstorage.tasks import utils


def get_vault(credentials, settings):
    layer2 = Layer2(
        aws_access_key_id=credentials['access_key'],
        aws_secret_access_key=credentials['secret_key'],
    )
    return layer2.get_vault(settings['vault'])


@utils.task
def _push_file_archive(self, local_path, version_id, callback_url,
                       credentials, settings):
    _, name = os.path.split(local_path)
    with utils.RetryUpload(self):
        vault = get_vault(credentials, settings)
        try:
            glacier_id = vault.upload_archive(local_path, description=name)
        except UnexpectedHTTPResponseError as error:
            # Glacier doesn't allow empty files; catch this exception but raise
            # other errors
            if error.status == 400:
                payload = json.loads(error.body.decode('utf-8'))
                if payload.get('message') == 'Invalid Content-Length: 0':
                    return
            raise
    metadata = {
        'vault': vault.name,
        'archive': glacier_id,
    }
    _push_archive_complete.delay(version_id, callback_url, metadata)


@utils.task
def _push_archive_complete(self, version_id, callback_url, metadata):
    signer = signing.Signer(settings.HMAC_SECRET, settings.HMAC_ALGORITHM)
    with utils.RetryHook(self):
        data = signing.sign_data(
            signer,
            {
                'version': version_id,
                'metadata': metadata,
            },
        )
        future = aiohttp.request(
            'PUT',
            callback_url,
            data=json.dumps(data),
            headers={'Content-Type': 'application/json'},
        )
        loop = asyncio.get_event_loop()
        response = loop.run_until_complete(future)

        if response.status != HTTPStatus.OK:
            raise Exception('Failed to report archive completion, got status code {}'.format(response.status))


@async_retry(retries=5, backoff=5)
def main(local_path, version_id, callback_url, credentials, settings):
    return _push_file_archive.delay(local_path, version_id, callback_url, credentials, settings)
