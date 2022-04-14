from waterbutler import settings

config = settings.child('RUSHFILES_PROVIDER_CONFIG')

#TODO Change settings
BASE_URL = config.get('BASE_URL', 'https://clientgateway.rushfiles.com/api/')