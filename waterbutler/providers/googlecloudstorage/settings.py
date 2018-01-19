from waterbutler import settings

config = settings.child('GOOGLECLOUDSTORAGE_PROVIDER_SETTINGS')

NAME = 'googlecloudstorage'

CAN_DUPLICATE_NAMES = True

BASE_URL = 'https://www.googleapis.com'

BATCH_URL = BASE_URL + '/batch'

UPLOAD_URL = BASE_URL + '/upload'

COPY_ACTION = 'copyTo'

BATCH_THRESHOLD = 100

BATCH_BOUNDARY = '===============7330845974216740156=='
