from waterbutler import settings

config = settings.child('GOOGLECLOUD_PROVIDER_SETTINGS')

# Google Cloud service account credentials
CREDS_PATH = config.get(
    'CREDS_PATH',
    './tests/providers/googlecloud/fixtures/credentials/gcloud-fake-000000.json'
)

# BASE URL
BASE_URL = config.get('BASE_URL', 'https://storage.googleapis.com')

# The expiration time (in seconds) for a signed request
SIGNATURE_EXPIRATION = config.get('SIGNATURE_EXPIRATION', 60)
