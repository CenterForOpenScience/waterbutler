from waterbutler import settings

config = settings.get('S3_PROVIDER_CONFIG')


TEMP_URL_SECS = config.get('TEMP_URL_SECS', 100)
