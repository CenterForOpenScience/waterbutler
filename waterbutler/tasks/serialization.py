import json
import importlib
from kombu.serialization import register
from waterbutler.core.path import WaterButlerPath
from waterbutler.core.metadata import BaseMetadata

_META_REGISTRY: dict[tuple[str, str], type[BaseMetadata]] = {}


class _WBJSONEncoder(json.JSONEncoder):
    """Encode WaterButlerPath and BaseMetadata (and their subclasses) with full fidelity."""
    def default(self, obj):
        if isinstance(obj, WaterButlerPath):
            # Build a list of all raw segments + their IDs
            parts = [
                {"raw": part.original_raw, "id": part.identifier}
                for part in obj.parts
            ]
            # Capture any prepend segments
            prepend_parts = [
                {"raw": part.original_raw, "id": part.identifier}
                for part in getattr(obj, "_prepend_parts", [])
            ]
            return {
                "__wb_path__": True,
                # Record the exact class so we rehydrate subclasses
                "cls": obj.__class__.__module__ + "." + obj.__class__.__name__,
                "is_folder": obj.is_folder,
                "parts": parts,
                "prepend_parts": prepend_parts,
                # Any provider-specific bits exposed via `.extra`
                "extra": obj.extra or {},
            }

        if isinstance(obj, BaseMetadata):
            module_name = obj.__class__.__module__
            class_name = obj.__class__.__name__
            _META_REGISTRY[(module_name, class_name)] = obj.__class__

            data: dict[str, object] = {
                "__wb_meta__": True,
                "cls": f"{module_name}.{class_name}",
                "raw": obj.raw,
            }
            if hasattr(obj, "_path"):
                data["path"] = getattr(obj, "_path")
            return data

        return super().default(obj)


def _dumps(obj, **_kw):
    return json.dumps(obj, cls=_WBJSONEncoder).encode("utf-8")


def _loads(s, **_kw):
    raw = s.decode("utf-8") if isinstance(s, (bytes, bytearray)) else s

    def _hook(item):
        if item.get("__wb_path__"):
            module_name, class_name = item["cls"].rsplit(".", 1)
            module = importlib.import_module(module_name)
            path_cls = getattr(module, class_name)

            # Rebuild PathPart instances using that subclass's PART_CLASS
            Part = path_cls.PART_CLASS
            parts = [
                Part(seg["raw"], _id=seg.get("id"))
                for seg in item["parts"]
            ]
            prepend = None
            if item["prepend_parts"]:
                # join raw segments with "/" to form the original prepend
                prepend = "/".join(seg["raw"] for seg in item["prepend_parts"])

            return path_cls.from_parts(
                parts,
                folder=item["is_folder"],
                prepend=prepend,
                **item.get("extra", {})
            )

        if item.get("__wb_meta__"):
            module_name, class_name = item["cls"].rsplit(".", 1)
            key = (module_name, class_name)

            if key in _META_REGISTRY:
                meta_cls = _META_REGISTRY[key]
            else:
                module = importlib.import_module(module_name)
                meta_cls = getattr(module, class_name)

            raw_dict = item["raw"]
            if "path" in item:
                return meta_cls(item["path"], raw_dict)  # type: ignore
            return meta_cls(raw_dict)  # type: ignore

        return item

    return json.loads(raw, object_hook=_hook)


register(
    name="wb_json",
    encoder=_dumps,
    decoder=_loads,
    content_type="application/x-wb-json",
    content_encoding="utf-8",
)
