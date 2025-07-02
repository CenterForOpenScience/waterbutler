import importlib

from kombu.serialization import register
from kombu.utils.json import register_type

from waterbutler.core.path import WaterButlerPath
from waterbutler.core.metadata import BaseMetadata, BaseFileRevisionMetadata

_META_REGISTRY: dict[tuple[str, str], type[BaseMetadata]] = {}
_REV_META_REGISTRY: dict[tuple[str, str], type[BaseFileRevisionMetadata]] = {}

import logging
logger = logging.getLogger(__name__)


def dehydrate_path(obj):
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

def rehydrate_path(payload):
    module_name, class_name = payload["cls"].rsplit(".", 1)
    module = importlib.import_module(module_name)
    path_cls = getattr(module, class_name)

    # Rebuild PathPart instances using that subclass's PART_CLASS
    Part = path_cls.PART_CLASS
    parts = [
        Part(seg["raw"], _id=seg.get("id"))
        for seg in payload["parts"]
    ]
    prepend = None
    if payload["prepend_parts"]:
        # join raw segments with "/" to form the original prepend
        prepend = "/".join(seg["raw"] for seg in payload["prepend_parts"])

    return path_cls.from_parts(
        parts,
        folder=payload["is_folder"],
        prepend=prepend,
        **payload.get("extra", {})
    )


def dehydrate_metadata(obj):
    return obj.dehydrate()

def rehydrate_metadata(payload):
    module_name, class_name = payload["cls"].rsplit(".", 1)
    key = (module_name, class_name)

    if key in _META_REGISTRY:
        meta_cls = _META_REGISTRY[key]
    else:
        module = importlib.import_module(module_name)
        meta_cls = getattr(module, class_name)

    return meta_cls.rehydrate(payload)


def dehydrate_file_revision_metadata(obj):
    return obj.dehydrate()

def rehydrate_file_revision_metadata(payload):
    module_name, class_name = payload["cls"].rsplit(".", 1)
    key = (module_name, class_name)

    if key in _REV_META_REGISTRY:
        meta_cls = _REV_META_REGISTRY[key]
    else:
        module = importlib.import_module(module_name)
        meta_cls = getattr(module, class_name)

    return meta_cls.rehydrate(payload)


register_type(
    WaterButlerPath,
    "WBPath",
    dehydrate_path,
    rehydrate_path,
)

register_type(
    BaseMetadata,
    "WBMetadata",
    dehydrate_metadata,
    rehydrate_metadata,
)

register_type(
    BaseFileRevisionMetadata,
    "WBFileRevisionMetadata",
    dehydrate_file_revision_metadata,
    rehydrate_file_revision_metadata,
)
