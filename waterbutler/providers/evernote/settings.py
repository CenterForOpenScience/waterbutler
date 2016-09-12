try:
    from waterbutler import settings
except ImportError:
    settings = {}

config = settings.get('EVERNOTE_PROVIDER_CONFIG', {})

EVERNOTE_META_URL = 'http://api.dataevernote.org/mn/object/doi:10.5061/evernote.'
EVERNOTE_FILE_URL = "http://www.dataevernote.org/mn/meta/doi:10.5061/evernote."
