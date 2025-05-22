import pytest
import json

from waterbutler.tasks.serialization import _dumps, _loads
from waterbutler.core.path import WaterButlerPath, WaterButlerPathPart


@pytest.mark.parametrize("path_kwargs", [
    # simple file under root, no prepend
    {"path": "/foo.txt", "ids": [None, "123"], "prepend": None, "folder": False},
    # nested file under a mountpoint, with prepend
    {"path": "/dir/sub/file.txt", "ids": [None, "1", "2", "3"], "prepend": "/mnt/data", "folder": False},
    # a folder path
    {"path": "/a/b/c/", "ids": [None, "a", "b", "c"], "prepend": "/srv/fs", "folder": True},
])
def test_wb_json_roundtrip(path_kwargs):
    """
    Full‐fidelity round‐trip via _dumps/_loads:

      – exact list of raw segments + identifiers
      – materialized_path and full_path
      – is_folder flag
      – prepend string and prepend_parts
      – correct class rehydration
    """
    orig = WaterButlerPath(
        path_kwargs["path"],
        _ids=path_kwargs["ids"],
        prepend=path_kwargs["prepend"],
        folder=path_kwargs["folder"],
    )
    blob = _dumps(orig)
    assert isinstance(blob, (bytes, bytearray))

    restored = _loads(blob)
    assert type(restored) is type(orig)

    assert restored.materialized_path == orig.materialized_path
    assert restored.full_path == orig.full_path
    assert restored.is_folder == orig.is_folder

    assert len(restored.parts) == len(orig.parts)

    for rp, op in zip(restored.parts, orig.parts):
        assert isinstance(rp, WaterButlerPathPart)
        assert rp.original_raw == op.original_raw
        assert rp.identifier   == op.identifier

    assert getattr(restored, "_prepend", None) == getattr(orig, "_prepend", None)

    orig_p = getattr(orig, "_prepend_parts", [])
    res_p  = getattr(restored, "_prepend_parts", [])
    assert len(res_p) == len(orig_p)
    for rp, op in zip(res_p, orig_p):
        assert rp.original_raw == op.original_raw
        assert rp.identifier   == op.identifier


def test_dump_schema_includes_cls_and_extra():
    """
    Ensure _dumps embeds the full schema so that _loads can
    rehydrate any subclass with its `.extra` data.
    """
    orig = WaterButlerPath("/foo/bar/", _ids=[None, "bar"], prepend="/mnt", folder=True)
    blob = _dumps(orig)
    payload = json.loads(blob.decode("utf-8"))

    assert payload["__wb_path__"] is True
    # exact class path
    expected_cls = orig.__class__.__module__ + "." + orig.__class__.__name__
    assert payload["cls"] == expected_cls

    assert payload["is_folder"] == orig.is_folder
    assert payload["extra"] == {}

    assert isinstance(payload["parts"], list)
    for seg, part in zip(payload["parts"], orig.parts):
        assert seg["raw"] == part.original_raw
        assert seg["id"]  == part.identifier

    # prepend parts
    assert isinstance(payload["prepend_parts"], list)
    expected_prep = [{"raw": p.original_raw, "id": p.identifier} for p in orig._prepend_parts]
    assert payload["prepend_parts"] == expected_prep


def test_non_path_objects_serialize_normally():
    """
    Things which are not WaterButlerPath still fall back to plain JSON.
    """
    data = {"foo": 1, "bar": ["x", "y", {"z": 3}]}
    blob = _dumps(data)
    loaded = json.loads(blob.decode("utf-8"))
    assert loaded == data
