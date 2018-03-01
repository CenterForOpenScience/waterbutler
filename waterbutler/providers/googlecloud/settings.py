from waterbutler import settings

config = settings.child('GOOGLECLOUD_PROVIDER_SETTINGS')

# Google Cloud service account credentials
CREDS_PATH = config.get(
    'CREDS_PATH',
    './tests/providers/googlecloud/fixtures/credentials/gcloud-fake-000000.json'
)

# BASE URL for JSON API, which uses oauth access token as authentication
BASE_URL_JSON = config.get('BASE_URL_JSON', 'https://www.googleapis.com')

# BASE URL for XML API, which uses signed request as authentication
BASE_URL_XML = config.get('BASE_URL_XML', 'https://storage.googleapis.com')

# The expiration time (in seconds) for a signed request
SIGNATURE_EXPIRATION = config.get('SIGNATURE_EXPIRATION', 60)

# TODO: remove this after XML API refactoring
COPY_ACTION = 'copyTo'
