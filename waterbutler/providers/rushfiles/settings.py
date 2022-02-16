from waterbutler import settings

config = settings.child('RUSHFILES_PROVIDER_CONFIG')

CHUNK_SIZE = config.get('CHUNK_SIZE', 90 * 1024 * 1024)  # 90MB
