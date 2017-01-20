from waterbutler import settings

config = settings.child('DROPBOX_PROVIDER_CONFIG')


BASE_URL = config.get('BASE_URL', 'https://api.dropboxapi.com/2')
BASE_CONTENT_URL = config.get('BASE_CONTENT_URL', 'https://content.dropboxapi.com/2/')
