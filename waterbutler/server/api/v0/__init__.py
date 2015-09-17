from waterbutler.server.api.v0 import zip
from waterbutler.server.api.v0 import copy
from waterbutler.server.api.v0 import crud
from waterbutler.server.api.v0 import move
from waterbutler.server.api.v0 import metadata
from waterbutler.server.api.v0 import revisions


PREFIX = ''
HANDLERS = [
    (r'/ops/copy', copy.CopyHandler),
    (r'/ops/move', move.MoveHandler),
    (r'/zip', zip.ZipHandler),
    (r'/file', crud.CRUDHandler),
    (r'/data', metadata.MetadataHandler),
    (r'/revisions', revisions.RevisionHandler),
]
