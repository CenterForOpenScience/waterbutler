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
MAX_BODY_SIZE = int(config.get('MAX_BODY_SIZE', int(4.9 * (1024 ** 3))))  # 4.9 GB

AUTH_HANDLERS = config.get('AUTH_HANDLERS', [
    'osf',
])

HMAC_ALGORITHM = getattr(hashlib, config.get('HMAC_ALGORITHM', 'sha256'))

HMAC_SECRET = config.get('HMAC_SECRET')
if not settings.DEBUG:
    assert HMAC_SECRET, 'HMAC_SECRET must be specified when not in debug mode'
HMAC_SECRET = (HMAC_SECRET or 'changeme').encode('utf-8')


# Configs for WB API Rate-limiting with Redis
ENABLE_RATE_LIMITING = config.get_bool('ENABLE_RATE_LIMITING', False)
REDIS_HOST = config.get('REDIS_HOST', '192.168.168.167')
REDIS_PORT = config.get('REDIS_PORT', '6379')
REDIS_PASSWORD = config.get('REDIS_PASSWORD', None)

# Number of seconds until the redis key expires
RATE_LIMITING_FIXED_WINDOW_SIZE = int(config.get('RATE_LIMITING_FIXED_WINDOW_SIZE', 3600))

# number of reqests permitted while the redis key is active
RATE_LIMITING_FIXED_WINDOW_LIMIT = int(config.get('RATE_LIMITING_FIXED_WINDOW_LIMIT', 3600))
