from waterbutler import settings

config = settings.child('FIGSHARE_PROVIDER_CONFIG')


BASE_URL = config.get('BASE_URL', 'https://api.figshare.com/v2')
VIEW_URL = config.get('VIEW_URL', 'https://figshare.com/')
DOWNLOAD_URL = config.get('VIEW_URL', 'https://ndownloader.figshare.com/')

# TODO [SVCS-996]: expand all 3 lists below with more folder-like article types
VALID_CONTAINER_TYPES = ['project', 'collection', 'article', 'dataset', 'fileset']
ARTICLE_CONTAINER_TYPES = ['article', 'dataset', 'fileset']
FOLDER_TYPES = [3, 4]  # figshare ID for Dataset (3) and Fileset (4, deprecated)

ARTICLE_TYPE_IDENTIFIER = 'https://api.figshare.com/v2/account/articles/'

# During initial testing this was set to 2 because file was not instantly ready after receiving HTTP 201
FILE_CREATE_WAIT = 0.1   # seconds passed to time.sleep

# project/collection article listings are paginated.  Specify max number of results returned per page.
MAX_PAGE_SIZE = int(config.get('MAX_PAGE_SIZE', 100))
