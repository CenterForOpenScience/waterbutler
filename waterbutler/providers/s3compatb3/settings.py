from waterbutler import settings

config = settings.child('S3COMPATB3_PROVIDER_CONFIG')


TEMP_URL_SECS = int(config.get('TEMP_URL_SECS', 100))
