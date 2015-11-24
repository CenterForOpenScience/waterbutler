try:
    from waterbutler import settings
except ImportError:
    settings = {}

config = settings.get('ONEDRIVE_PROVIDER_CONFIG', {})


BASE_URL = config.get('BASE_URL', 'https://api.onedrive.com/v1.0')
BASE_CONTENT_URL = config.get('BASE_CONTENT_URL', 'https://api.onedrive.com/v1.0')
