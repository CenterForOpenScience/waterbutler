from waterbutler import settings

config = settings.child('CLOUDFILES_PROVIDER_CONFIG')


TEMP_URL_SECS = int(config.get('TEMP_URL_SECS', 100))
AUTH_URL = config.get('AUTH_URL', 'https://identity.api.rackspacecloud.com/v2.0/tokens')
