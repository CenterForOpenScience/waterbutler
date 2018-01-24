from waterbutler import settings

config = settings.child('GOOGLECLOUD_PROVIDER_SETTINGS')

NAME = 'googlecloud'

CAN_DUPLICATE_NAMES = True

BASE_URL = 'https://www.googleapis.com'

BATCH_URL = BASE_URL + '/batch'

UPLOAD_URL = BASE_URL + '/upload'

COPY_ACTION = 'copyTo'  # Another option for copy is "rewriteTo"

BATCH_THRESHOLD = 100  # One batch request supports no more than 100 individual requests

BATCH_BOUNDARY = '===============7330845974216740156=='
