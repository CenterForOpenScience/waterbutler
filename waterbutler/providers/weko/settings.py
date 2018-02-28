from waterbutler import settings

config = settings.child('WEKO_PROVIDER_CONFIG')

FILE_PATH_DRAFT = config.get('FILE_PATH_DRAFT', '/code/weko-draft/')
