from waterbutler import settings

config = settings.child('ONEDRIVE_PROVIDER_CONFIG')


BASE_URL = config.get('BASE_URL', 'https://api.onedrive.com/v1.0/drive/items/')
BASE_CONTENT_URL = config.get('BASE_CONTENT_URL', 'https://api.onedrive.com/v1.0/drive/items/')
BASE_ROOT_URL = config.get('BASE_ROOT_URL', 'https://api.onedrive.com/v1.0')
ONEDRIVE_COPY_ITERATION_COUNT = int(config.get('ONEDRIVE_COPY_ITERATION_COUNT', 30))
ONEDRIVE_COPY_SLEEP_INTERVAL = int(config.get('ONEDRIVE_COPY_SLEEP_INTERVAL', 3))
