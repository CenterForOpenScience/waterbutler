try:
    from waterbutler import settings
except ImportError:
    settings = {}

config = settings.get('FEDORA_PROVIDER_CONFIG', {})

LAST_MODIFIED_PROPERTY_URI = 'http://fedora.info/definitions/v4/repository#lastModified'
CONTAINS_PROPERTY_URI = 'http://www.w3.org/ns/ldp#contains'
FILENAME_PROPERTY_URI = 'http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#filename'
SIZE_PROPERTY_URI = 'http://www.loc.gov/premis/rdf/v1#hasSize'
MIME_TYPE_PROPERTY_URI = 'http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#hasMimeType'

OCTET_STREAM_MIME_TYPE = 'application/octet-stream'
RDF_MIME_TYPES = ['text/turtle', 'text/rdf+n3', 'application/n3', 'text/n3'
                   'application/rdf+xml' 'application/n-triples' 'application/ld+json']
