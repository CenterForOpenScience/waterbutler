from waterbutler import settings

config = settings.child('BOX_PROVIDER_CONFIG')


BASE_URL = config.get('BASE_URL', 'https://api.box.com/2.0')
BASE_UPLOAD_URL = config.get('BASE_CONTENT_URL', 'https://upload.box.com/api/2.0')
NONCHUNKED_UPLOAD_LIMIT = int(config.get('NONCHUNKED_UPLOAD_LIMIT', 50 * 1000 * 1000))  # 50 MB
