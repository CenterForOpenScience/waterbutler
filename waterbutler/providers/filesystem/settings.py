from waterbutler import settings

config = settings.get('FILESYSTEM_PROVIDER_CONFIG')


CHUNK_SIZE = config.get('CHUNK_SIZE', 65536)  # 64KB
