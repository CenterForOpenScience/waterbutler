from waterbutler import settings

config = settings.child('AZUREBLOBSTORAGE_PROVIDER_CONFIG')

# Azure Blob Storage settings
CHUNK_SIZE = int(config.get('CHUNK_SIZE', 4 * 1024 * 1024))  # 4MB
CONTIGUOUS_UPLOAD_SIZE_LIMIT = int(config.get('CONTIGUOUS_UPLOAD_SIZE_LIMIT', 64 * 1024 * 1024))  # 64 MB
