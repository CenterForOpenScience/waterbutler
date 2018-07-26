from waterbutler import settings

config = settings.child('BOX_PROVIDER_CONFIG')


BASE_URL = config.get('BASE_URL', 'https://api.box.com/2.0')
BASE_UPLOAD_URL = config.get('BASE_CONTENT_URL', 'https://upload.box.com/api/2.0')
NONCHUNKED_UPLOAD_LIMIT = int(config.get('NONCHUNKED_UPLOAD_LIMIT', 50 * 1000 * 1000))  # 50 MB

# The size of the chunks read when writing the upload stream to disk
TEMP_CHUNK_SIZE = int(config.get('TEMP_CHUNK_SIZE', 32 * 1024))  # 32KiB

# Number of times to retry upload commits before giving up
UPLOAD_COMMIT_RETRIES = int(config.get('UPLOAD_COMMIT_RETRIES', 10))
