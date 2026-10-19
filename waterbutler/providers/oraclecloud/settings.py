from waterbutler import settings

config = settings.child("ORACLECLOUD_PROVIDER_SETTINGS")

# slurp downloads below this threshhold (in bytes)
MAX_SLURP_SIZE = 100 * 1000

# The expiration time (in seconds) for a signed request
SIGNATURE_EXPIRATION = int(config.get('SIGNATURE_EXPIRATION', 60))
