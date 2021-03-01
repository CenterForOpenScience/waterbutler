try:
    from waterbutler import settings
except ImportError:
    settings = {}  # type: ignore

config = settings.get('OCIINSTITUTIONS_PROVIDER_CONFIG', {})  # type: ignore
