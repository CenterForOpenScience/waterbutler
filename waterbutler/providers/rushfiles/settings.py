from waterbutler import settings

config = settings.child('RUSHFILES_PROVIDER_CONFIG')

# RushFiles installations have a request body size limit set to 153,600,000 B (150,000 KB),
# and files need to be uploaded in chunks no larger than that.
# Set to 140MB as default to play it safe.
CHUNK_SIZE = config.get('CHUNK_SIZE', 140 * 1024 * 1024)  # 140MB

# Device ID by which file events will be trackable in RushFiles
DEVICE_ID = config.get('DEVICE_ID', 'waterbutler')
