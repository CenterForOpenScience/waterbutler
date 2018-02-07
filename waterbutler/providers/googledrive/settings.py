from waterbutler import settings

config = settings.child('GOOGLEDRIVE_PROVIDER_CONFIG')

BASE_URL = config.get('BASE_URL', 'https://www.googleapis.com/drive/v3')
BASE_UPLOAD_URL = config.get('BASE_UPLOAD_URL', 'https://www.googleapis.com/upload/drive/v3')

DRIVE_IGNORE_VERSION = config.get('DRIVE_IGNORE_VERSION', '0000000000000000000000000000000000000')
