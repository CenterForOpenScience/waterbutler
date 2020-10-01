import hashlib

from waterbutler import settings

config = settings.child('OSFSTORAGE_PROVIDER_CONFIG')


HMAC_ALGORITHM = getattr(hashlib, config.get('HMAC_ALGORITHM', 'sha256'))

HMAC_SECRET = config.get_nullable('HMAC_SECRET', None)

if not settings.DEBUG:
    assert HMAC_SECRET, 'HMAC_SECRET must be specified when not in debug mode'
HMAC_SECRET = (HMAC_SECRET or 'changeme').encode('utf-8')

# number of times to try to fetch quota limits from OSF
QUOTA_RETRIES = int(config.get('QUOTA_RETRIES', 2))

# base time in seconds to wait between each quota request.  This is multiplied by the current
# number of retries attempted.
QUOTA_RETRIES_DELAY = int(config.get('QUOTA_RETRIES_DELAY', 1))
