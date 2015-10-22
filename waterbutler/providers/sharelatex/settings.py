try:
    from waterbutler import settings
except ImportError:
    settings = {}

config = settings.get('SHARELATEX_PROVIDER_CONFIG', {})

BASE_URL = config.get('BASE_URL', 'http://sharelatex.com/api/v1')
BASE_CONTENT_URL = config.get('BASE_CONTENT_URL', 'http://sharelatex.com/raw')
VIEW_URL = config.get('VIEW_URL', 'http://sharelatex.com/')
