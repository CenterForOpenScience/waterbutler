import hashlib

from waterbutler import settings


config = settings.child('SERVER_CONFIG')

ADDRESS = config.get('ADDRESS', 'localhost')
PORT = config.get('PORT', 7777)
DOMAIN = config.get('DOMAIN', "http://localhost:7777")

DEBUG = config.get_bool('DEBUG', True)

SSL_CERT_FILE = config.get_nullable('SSL_CERT_FILE', None)
SSL_KEY_FILE = config.get_nullable('SSL_KEY_FILE', None)

XHEADERS = config.get_bool('XHEADERS', False)
CORS_ALLOW_ORIGIN = config.get('CORS_ALLOW_ORIGIN', '*')

CHUNK_SIZE = int(config.get('CHUNK_SIZE', 65536))  # 64KB
MAX_BODY_SIZE = int(config.get('MAX_BODY_SIZE', int(50 * (1024 ** 3))))  # 50 GB

AUTH_HANDLERS = config.get('AUTH_HANDLERS', [
    'osf',
])

HMAC_ALGORITHM = getattr(hashlib, config.get('HMAC_ALGORITHM', 'sha256'))

HMAC_SECRET = config.get('HMAC_SECRET')
if not settings.DEBUG:
    assert HMAC_SECRET, 'HMAC_SECRET must be specified when not in debug mode'
HMAC_SECRET = (HMAC_SECRET or 'changeme').encode('utf-8')
