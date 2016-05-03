try:
    from waterbutler import settings
except ImportError:
    settings = {}

config = settings.get('FEDORA_PROVIDER_CONFIG', {})
