from waterbutler import settings

config = settings.child('GITHUB_PROVIDER_CONFIG')


BASE_URL = config.get('BASE_URL', 'https://api.github.com/')
VIEW_URL = config.get('VIEW_URL', 'https://github.com/')

MOVE_MESSAGE = config.get('MOVE_MESSAGE', 'Moved on behalf of WaterButler')
COPY_MESSAGE = config.get('COPY_MESSAGE', 'Copied on behalf of WaterButler')
DELETE_FILE_MESSAGE = config.get('DELETE_FILE_MESSAGE', 'File deleted on behalf of WaterButler')
UPDATE_FILE_MESSAGE = config.get('UPDATE_FILE_MESSAGE', 'File updated on behalf of WaterButler')
UPLOAD_FILE_MESSAGE = config.get('UPLOAD_FILE_MESSAGE', 'File uploaded on behalf of WaterButler')
DELETE_FOLDER_MESSAGE = config.get('DELETE_FOLDER_MESSAGE', 'Folder deleted on behalf of WaterButler')

# At some point in the near(?) future git will be changing its internal hash function from SHA-1
# to SHA-256.  sha1-names are 40 hexdigits long and sha256-names are 64 hexdigits long.  At that
# point, it seems probable that GitHub will update its API to accept both sha types. When that
# happens, the following config var will need to be updated to include both sizes.
#
# Example for passing multiple length values via an envvar on the command line:
#   $ GITHUB_PROVIDER_GITHUB_SHA_LENGTHS="40 64" invoke server
#
# Example setting in a .docker-compose.env (no quotes):
#   GITHUB_PROVIDER_GITHUB_SHA_LENGTHS=40 64
#
GITHUB_SHA_LENGTHS = [int(x) for x in config.get('GITHUB_SHA_LENGTHS', '40').split(' ')]

# The time in seconds to wait before making another attempt to add more tokens
RL_TOKEN_ADD_DELAY = int(config.get('RL_TOKEN_ADD_DELAY', 1))

# The maximum number of available tokens allowed
RL_MAX_AVAILABLE_TOKENS = int(config.get('RL_MAX_AVAILABLE_TOKENS', 10))

# The minimum interval in seconds between each request rate update.
RL_REQ_RATE_UPDATE_INTERVAL = int(config.get('RL_REQ_RATE_UPDATE_INTERVAL', 60))
