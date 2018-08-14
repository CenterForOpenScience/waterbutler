import hashlib

from waterbutler import settings

config = settings.child('OSFSTORAGE_PROVIDER_CONFIG')


HMAC_ALGORITHM = getattr(hashlib, config.get('HMAC_ALGORITHM', 'sha256'))

HMAC_SECRET = config.get_nullable('HMAC_SECRET', None)

if not settings.DEBUG:
    assert HMAC_SECRET, 'HMAC_SECRET must be specified when not in debug mode'
HMAC_SECRET = (HMAC_SECRET or 'changeme').encode('utf-8')
