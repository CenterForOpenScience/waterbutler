import hashlib

from waterbutler import settings

config = settings.child('OSFSTORAGE_PROVIDER_CONFIG')


FILE_PATH_PENDING = config.get('FILE_PATH_PENDING', '/tmp/pending')
FILE_PATH_COMPLETE = config.get('FILE_PATH_COMPLETE', '/tmp/complete')

RUN_TASKS = config.get_bool('RUN_TASKS', False)

HMAC_ALGORITHM = getattr(hashlib, config.get('HMAC_ALGORITHM', 'sha256'))

HMAC_SECRET = config.get_nullable('HMAC_SECRET', None)

if not settings.DEBUG:
    assert HMAC_SECRET, 'HMAC_SECRET must be specified when not in debug mode'
HMAC_SECRET = (HMAC_SECRET or 'changeme').encode('utf-8')

# Retry options
UPLOAD_RETRY_ATTEMPTS = int(config.get('UPLOAD_RETRY_ATTEMPTS', 1))
UPLOAD_RETRY_INIT_DELAY = int(config.get('UPLOAD_RETRY_INIT_DELAY', 30))
UPLOAD_RETRY_MAX_DELAY = int(config.get('UPLOAD_RETRY_MAX_DELAY', 60 * 60))
UPLOAD_RETRY_BACKOFF = int(config.get('UPLOAD_RETRY_BACKOFF', 2))
UPLOAD_RETRY_WARN_IDX = int(config.get('UPLOAD_RETRY_WARN_IDX', 5))

HOOK_RETRY_ATTEMPTS = int(config.get('HOOK_RETRY_ATTEMPTS ', 1))
HOOK_RETRY_INIT_DELAY = int(config.get('HOOK_RETRY_INIT_DELAY', 30))
HOOK_RETRY_MAX_DELAY = int(config.get('HOOK_RETRY_MAX_DELAY', 60 * 60))
HOOK_RETRY_BACKOFF = int(config.get('HOOK_RETRY_BACKOFF', 2))
HOOK_RETRY_WARN_IDX = config.get_nullable('HOOK_RETRY_WARN_IDX', None)

PARITY_RETRY_ATTEMPTS = int(config.get('PARITY_RETRY_ATTEMPTS', 1))
PARITY_RETRY_INIT_DELAY = int(config.get('PARITY_RETRY_INIT_DELAY', 30))
PARITY_RETRY_MAX_DELAY = int(config.get('PARITY_RETRY_MAX_DELAY', 60 * 60))
PARITY_RETRY_BACKOFF = int(config.get('PARITY_RETRY_BACKOFF', 2))
PARITY_RETRY_WARN_IDX = config.get_nullable('PARITY_RETRY_WARN_IDX', None)

# Parity options
PARITY_CONTAINER_NAME = config.get_nullable('PARITY_CONTAINER_NAME', None)
PARITY_REDUNDANCY = int(config.get('PARITY_REDUNDANCY', 5))
PARITY_PROVIDER_NAME = config.get('PARITY_PROVIDER_NAME', 'cloudfiles')
PARITY_PROVIDER_CREDENTIALS = config.get('PARITY_PROVIDER_CREDENTIALS', {})
PARITY_PROVIDER_SETTINGS = config.get('PARITY_PROVIDER_SETTINGS', {})
