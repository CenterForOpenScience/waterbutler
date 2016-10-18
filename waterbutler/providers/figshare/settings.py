from waterbutler import settings

config = settings.child('FIGSHARE_PROVIDER_CONFIG')


BASE_URL = config.get('BASE_URL', 'https://api.figshare.com/v2')
VIEW_URL = config.get('VIEW_URL', 'https://figshare.com/')
DOWNLOAD_URL = config.get('VIEW_URL', 'https://ndownloader.figshare.com/')

FOLDER_TYPES = [4]

PRIVATE_IDENTIFIER = 'https://api.figshare.com/v2/account/'

# During initial testing this was set to 2 because file was not instantly ready after receiving HTTP 201
FILE_CREATE_WAIT = 0.1   # seconds passed to time.sleep
