from waterbutler import settings

config = settings.child('DRYAD_PROVIDER_CONFIG')

DRYAD_META_URL = config.get('DRYAD_META_URL',
                            'http://api.datadryad.org/mn/object/doi:10.5061/dryad.')
DRYAD_FILE_URL = config.get('DRYAD_FILE_URL',
                            'http://api.datadryad.org/mn/meta/doi:10.5061/dryad.')
DRYAD_DOI_BASE = config.get('DRYAD_DOI_BASE', 'doi:10.5061/dryad.')
