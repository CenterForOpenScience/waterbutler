from waterbutler import settings

config = settings.child('ONEDRIVE_PROVIDER_CONFIG')


BASE_URL = config.get('BASE_URL', 'https://graph.microsoft.com/v1.0')
ONEDRIVE_COPY_SLEEP_INTERVAL = int(config.get('ONEDRIVE_COPY_SLEEP_INTERVAL', 3))
ONEDRIVE_MAX_UPLOAD_CHUNK_SIZE = 5 * 1024 * 1024  # 5 MiB
