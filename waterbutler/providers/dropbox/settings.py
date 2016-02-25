try:
    from waterbutler import settings
except ImportError:
    settings = {}

config = settings.get('DROPBOX_PROVIDER_CONFIG', {})


BASE_URL = config.get('BASE_URL', 'https://api.dropboxapi.com/1/')
BASE_CONTENT_URL = config.get('BASE_CONTENT_URL', 'https://content.dropboxapi.com/1/')
