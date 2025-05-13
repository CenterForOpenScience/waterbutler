import json
from kombu.serialization import register
from waterbutler.core.path import WaterButlerPath


class _WBJSONEncoder(json.JSONEncoder):
    """Encode WaterButlerPath as a lightweight dict."""
    def default(self, obj): # noqa: D401
        if isinstance(obj, WaterButlerPath):
            return {"__wb_path__": str(obj)}
        return super().default(obj)


def _dumps(obj, **_kw):
    # Kombu passes bytes around → always return `bytes`
    return json.dumps(obj, cls=_WBJSONEncoder).encode()


def _loads(s, **_kw):
    def _hook(item):
        marker = item.get("__wb_path__")
        if marker is not None:
            return WaterButlerPath(marker)
        return item
    return json.loads(s.decode() if isinstance(s, (bytes, bytearray)) else s,
                      object_hook=_hook)

register(
    name="wb_json",
    encoder=_dumps,
    decoder=_loads,
    content_type="application/x-wb-json",
    content_encoding="utf-8",
)
