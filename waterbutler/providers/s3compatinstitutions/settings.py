try:
    from waterbutler import settings
except ImportError:
    settings = {}  # type: ignore

config = settings.get('S3COMPATINSTITUTIONS_PROVIDER_CONFIG', {})  # type: ignore
