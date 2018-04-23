from waterbutler import settings

config = settings.child('S3_PROVIDER_CONFIG')


TEMP_URL_SECS = int(config.get('TEMP_URL_SECS', 100))
NONCHUNKED_UPLOAD_LIMIT = int(config.get('NONCHUNKED_UPLOAD_LIMIT', 128000000))  # 128 MB
CHUNK_SIZE = int(config.get('CHUNK_SIZE', 64000000))  # 64 MB
