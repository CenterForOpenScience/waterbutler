from waterbutler import settings

config = settings.child('FIGSHARE_PROVIDER_CONFIG')


BASE_URL = config.get('BASE_URL', 'https://api.figshare.com/v2')
VIEW_URL = config.get('VIEW_URL', 'https://figshare.com/')
DOWNLOAD_URL = config.get('VIEW_URL', 'https://ndownloader.figshare.com/')

VALID_CONTAINER_TYPES = ['project', 'collection', 'article', 'fileset']
FOLDER_TYPES = [4]  # Figshare ID for filesets

PRIVATE_IDENTIFIER = 'https://api.figshare.com/v2/account/'
ARTICLE_TYPE_IDENTIFIER = 'https://api.figshare.com/v2/account/articles/'

# During initial testing this was set to 2 because file was not instantly ready after receiving HTTP 201
FILE_CREATE_WAIT = 0.1   # seconds passed to time.sleep

# project/collection article listings are paginated.  Specify max number of results returned per page.
MAX_PAGE_SIZE = int(config.get('MAX_PAGE_SIZE', 100))
