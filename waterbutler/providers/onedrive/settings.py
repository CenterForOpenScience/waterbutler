from waterbutler import settings

config = settings.child('ONEDRIVE_PROVIDER_CONFIG')


BASE_URL = config.get('BASE_URL', 'https://api.onedrive.com/v1.0')
BASE_DRIVE_URL = config.get('BASE_DRIVE_URL', 'https://api.onedrive.com/v1.0/drive')
ONEDRIVE_COPY_REQUEST_TIMEOUT = int(config.get('ONEDRIVE_COPY_REQUEST_TIMEOUT', 30))
ONEDRIVE_ASYNC_REQUEST_SLEEP_INTERVAL = int(config.get('ONEDRIVE_ASYNC_REQUEST_SLEEP_INTERVAL', 3))
ONEDRIVE_ABSOLUTE_ROOT_ID = config.get('ONEDRIVE_ABSOLUTE_ROOT_ID', 'root')
# 10mb
ONEDRIVE_CHUNKED_UPLOAD_CHUNK_SIZE = int(config.get('ONEDRIVE_CHUNKED_UPLOAD_CHUNK_SIZE',
                                                    1024 * 1024 * 10))
# 90mb
ONEDRIVE_CHUNKED_UPLOAD_FILE_SIZE = int(config.get('ONEDRIVE_CHUNKED_UPLOAD_FILE_SIZE',
                                                   1024 * 1024 * 90))
assert ONEDRIVE_CHUNKED_UPLOAD_FILE_SIZE > ONEDRIVE_CHUNKED_UPLOAD_CHUNK_SIZE
