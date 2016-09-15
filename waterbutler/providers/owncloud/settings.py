try:
    from waterbutler import settings
except ImportError:
    settings = {}

config = settings.get('OWNCLOUD_PROVIDER_CONFIG', {})
