try:
    from waterbutler import settings
except ImportError:
    settings = {}

config = settings.get('FAKE_AUTH_CONFIG', {})

PROVIDERS = config.get('PROVIDERS')
