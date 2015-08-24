from waterbutler.server.api.v1 import provider
PREFIX = 'v1'

HANDLERS = [
    provider.ProviderHandler.as_entry()
]
