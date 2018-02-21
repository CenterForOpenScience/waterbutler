from waterbutler import settings

config = settings.child('GOOGLECLOUD_PROVIDER_SETTINGS')

# Google Cloud service account credentials
CREDS_PATH = config.get('CREDS_PATH', 'changeme')

# BASE URL for JSON API, which uses oauth access token as authentication
BASE_URL_JSON = config.get('BASE_URL_JSON', 'https://www.googleapis.com')

# BASE URL for XML API, which uses signed request as authentication
BASE_URL_XML = config.get('BASE_URL_XML', 'https://storage.googleapis.com')

# The action to use for copy. Options are 'copyTo' and 'rewriteTo'
COPY_ACTION = config.get('COPY_ACTION', 'copyTo')

# One batch request can support up to 100 individual requests
BATCH_THRESHOLD = config.get('BATCH_THRESHOLD', 100)

# Maximum number of retries for failed requests
BATCH_MAX_RETRIES = config.get('BATCH_MAX_RETRIES', 5)

# The delimiter between requests in one batch
BATCH_BOUNDARY = config.get('BATCH_BOUNDARY', '===============7330845974216740156==')

# The expiration time (in seconds) for a signed request
SIGNATURE_EXPIRATION = 60
