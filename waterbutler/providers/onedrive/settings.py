from waterbutler import settings

config = settings.child('ONEDRIVE_PROVIDER_CONFIG')


BASE_URL = config.get('BASE_URL', 'https://api.onedrive.com/v1.0')
BASE_DRIVE_URL = config.get('BASE_DRIVE_URL', 'https://api.onedrive.com/v1.0/drive')
ONEDRIVE_COPY_ITERATION_COUNT = int(config.get('ONEDRIVE_COPY_ITERATION_COUNT', 30))
ONEDRIVE_COPY_SLEEP_INTERVAL = int(config.get('ONEDRIVE_COPY_SLEEP_INTERVAL', 3))
