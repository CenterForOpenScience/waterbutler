import asyncio
import hashlib
import os

from waterbutler.core import streams
from waterbutler.core.utils import async_retry, make_provider
from waterbutler.providers.osfstorage import settings as osf_settings
from waterbutler.providers.osfstorage.tasks import utils


@utils.task
def _parity_create_files(self, name, version_id, callback_url, credentials, settings):
    path = os.path.join(osf_settings.FILE_PATH_COMPLETE, name)
    loop = asyncio.get_event_loop()
    with utils.RetryUpload(self):
        parity_paths = utils.create_parity_files(
            path,
            redundancy=osf_settings.PARITY_REDUNDANCY,
        )
        if not parity_paths:
            # create_parity_files will return [] for empty files
            return
        futures = [asyncio.async(_upload_parity(each, credentials, settings))
                   for each in parity_paths]
        results, _ = loop.run_until_complete(asyncio.wait(futures,
                                                          return_when=asyncio.FIRST_EXCEPTION))
        # Errors are not raised in `wait`; explicitly check results for errors
        # and raise if any found
        for each in results:
            error = each.exception()
            if error:
                raise error

    metadata = {
        'parity': {
            'redundancy': osf_settings.PARITY_REDUNDANCY,
            'files': [
                (lambda r: {'name': r[0], 'sha256': r[1]})(r.result()) for r in results
            ],
        },
    }
    _push_parity_complete.delay(version_id, callback_url, metadata)


async def _upload_parity(path, credentials, settings):
    _, name = os.path.split(path)
    provider_name = settings.get('provider')
    provider = make_provider(provider_name, {}, credentials, settings)
    with open(path, 'rb') as file_pointer:
        stream = streams.FileStreamReader(file_pointer)
        stream.add_writer('sha256', streams.HashStreamWriter(hashlib.sha256))
        await provider.upload(
            stream,
            (await provider.validate_path('/' + name))
        )
    return (name, stream.writers['sha256'].hexdigest)


@utils.task
def _push_parity_complete(self, version_id, callback_url, metadata):
    with utils.RetryHook(self):
        future = utils.push_metadata(version_id, callback_url, metadata)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(future)


@async_retry(retries=5, backoff=5)
def main(name, version_id, callback_url, credentials, settings):
    return _parity_create_files.delay(name, version_id, callback_url, credentials, settings)
