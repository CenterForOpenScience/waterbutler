from waterbutler import settings

config = settings.child('FIGSHARE_PROVIDER_CONFIG')


BASE_URL = config.get('BASE_URL', 'http://api.figshare.com/v1/my_data')
VIEW_URL = config.get('VIEW_URL', 'http://figshare.com/')
