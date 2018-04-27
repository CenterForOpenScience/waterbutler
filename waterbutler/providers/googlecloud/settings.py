from waterbutler import settings

config = settings.child('GOOGLECLOUD_PROVIDER_SETTINGS')

# BASE URL
BASE_URL = config.get('BASE_URL', 'https://storage.googleapis.com')

# The expiration time (in seconds) for a signed request
SIGNATURE_EXPIRATION = int(config.get('SIGNATURE_EXPIRATION', 60))
