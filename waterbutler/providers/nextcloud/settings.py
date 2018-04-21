try:
    from waterbutler import settings
except ImportError:
    settings = {}  # type: ignore

config = settings.get('NEXTCLOUD_PROVIDER_CONFIG', {})  # type: ignore
