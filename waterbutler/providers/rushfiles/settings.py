from waterbutler import settings

config = settings.child('RUSHFILES_PROVIDER_CONFIG')

#TODO Change settings
BASE_URL = config.get('BASE_URL', 'https://clientgateway.rushfiles.com/api/shares')
BASE_FILECACHE_URL = config.get('BASE_FILECACHE_URL', 'https://filecache01.rushfiles.com/api/shares/')