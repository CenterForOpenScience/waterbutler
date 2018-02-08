from waterbutler import settings

config = settings.child('GOOGLECLOUD_PROVIDER_SETTINGS')

BASE_URL = config.get('BASE_URL', 'https://www.googleapis.com')

# The action to use for copy. Options are 'copyTo' and 'rewriteTo'
COPY_ACTION = config.get('COPY_ACTION', 'copyTo')

# One batch request can support up to 100 individual requests
BATCH_THRESHOLD = config.get('BATCH_THRESHOLD', 100)

# Maximum number of retries for failed requests
BATCH_MAX_RETRIES = config.get('BATCH_MAX_RETRIES', 5)

# The delimiter between requests in one batch
BATCH_BOUNDARY = config.get('BATCH_BOUNDARY', '===============7330845974216740156==')
