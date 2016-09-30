try:
    from waterbutler import settings
except ImportError:
    settings = {}

config = settings.get('DMPTOOL_PROVIDER_CONFIG', {})
