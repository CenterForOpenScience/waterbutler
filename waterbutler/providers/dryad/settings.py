try:
    from waterbutler import settings
except ImportError:
    settings = {}

config = settings.get('DRYAD_PROVIDER_CONFIG', {})

DRYAD_META_URL = 'http://api.datadryad.org/mn/object/doi:10.5061/dryad.'
DYRAD_FILE_URL = "http://www.datadryad.org/mn/meta/doi:10.5061/dryad."
