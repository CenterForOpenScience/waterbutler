import pytest
import json

# from waterbutler.tasks.serialization import _dumps, _loads
from waterbutler.tasks.serialization import dehydrate_path, rehydrate_path, dehydrate_metadata, rehydrate_metadata
from waterbutler.core.path import WaterButlerPath, WaterButlerPathPart
from waterbutler.core.metadata import BaseMetadata


@pytest.mark.skip('update for path/metadata dehydration')
@pytest.mark.parametrize("path_kwargs", [
    # simple file under root, no prepend
    {"path": "/foo.txt", "ids": [None, "123"], "prepend": None, "folder": False},
    # nested file under a mountpoint, with prepend
    {"path": "/dir/sub/file.txt", "ids": [None, "1", "2", "3"], "prepend": "/mnt/data", "folder": False},
    # a folder path
    {"path": "/a/b/c/", "ids": [None, "a", "b", "c"], "prepend": "/srv/fs", "folder": True},
])
def test_wb_json_roundtrip(path_kwargs):
    orig = WaterButlerPath(
        path_kwargs["path"],
        _ids=path_kwargs["ids"],
        prepend=path_kwargs["prepend"],
        folder=path_kwargs["folder"],
    )
    blob = json.dumps(dehydrate_path(orig))
    # assert isinstance(blob, (bytes, bytearray))
    assert isinstance(blob, str)

    restored = rehydrate_path(blob)
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


@pytest.mark.skip('update for path/metadata dehydration')
def test_dump_schema_includes_cls_and_extra():
    orig = WaterButlerPath("/foo/bar/", _ids=[None, "bar"], prepend="/mnt", folder=True)
    blob = json.dumps(dehydrate_path(orig))
    # payload = json.loads(blob.decode("utf-8"))
    payload = json.loads(blob)

    assert payload["__wb_path__"] is True
    expected_cls = orig.__class__.__module__ + "." + orig.__class__.__name__
    assert payload["cls"] == expected_cls

    assert payload["is_folder"] == orig.is_folder
    assert payload["extra"] == {}

    for seg, part in zip(payload["parts"], orig.parts):
        assert seg["raw"] == part.original_raw
        assert seg["id"]  == part.identifier

    expected_prep = [{"raw": p.original_raw, "id": p.identifier} for p in orig._prepend_parts]
    assert payload["prepend_parts"] == expected_prep


# def test_non_path_objects_serialize_normally():
#     data = {"foo": 1, "bar": ["x", "y", {"z": 3}]}
#     blob = _dumps(data)
#     loaded = json.loads(blob.decode("utf-8"))
#     assert loaded == data


@pytest.mark.skip('update for path/metadata dehydration')
def test_simple_base_metadata_roundtrip():
    """A BaseMetadata subclass with single‐arg constructor should round-trip."""
    class DummyMeta(BaseMetadata):
        def __init__(self, raw):
            super().__init__(raw)
        @property
        def provider(self): return "dummy"
        @property
        def kind(self): return "file"
        @property
        def name(self): return "foo.txt"
        @property
        def path(self): return "/foo.txt"
        @property
        def etag(self): return "deadbeef"

    # FIXME: path should probably be a path object
    orig = DummyMeta({"foo": "bar"})
    blob = json.dumps(dehydrate_path(orig))
    # payload = json.loads(blob.decode("utf-8"))
    payload = json.loads(blob)

    assert payload["__wb_meta__"] is True
    expected_cls = orig.__class__.__module__ + "." + orig.__class__.__name__
    assert payload["cls"] == expected_cls
    assert payload["raw"] == orig.raw
    assert "path" not in payload

    restored = rehydrate_metadata(blob)
    assert isinstance(restored, DummyMeta)
    assert restored.raw == orig.raw
    assert restored.provider == orig.provider
    assert restored.path == orig.path


@pytest.mark.skip('update for path/metadata dehydration')
def test_two_arg_base_metadata_roundtrip():
    """A BaseMetadata subclass with (path, raw) constructor should round-trip."""
    class DummyMeta2(BaseMetadata):
        def __init__(self, path, raw):
            super().__init__(raw)
            self._path = path
        @property
        def provider(self): return "dummy2"
        @property
        def kind(self): return "folder"
        @property
        def name(self): return self._path.strip("/")
        @property
        def path(self): return self._path
        @property
        def etag(self): return "cafebabe"

    # FIXME: path should probably be a path object
    orig = DummyMeta2("/myfolder/", {"hello": "world"})
    blob = json.dumps(dehydrate_metadata(orig))
    # payload = json.loads(blob.decode("utf-8"))
    payload = json.loads(blob)

    assert payload["__wb_meta__"] is True
    expected_cls = orig.__class__.__module__ + "." + orig.__class__.__name__
    assert payload["cls"] == expected_cls
    assert payload["raw"] == orig.raw
    assert payload["path"] == orig.path

    restored = rehydrate_metadata(blob)
    assert isinstance(restored, DummyMeta2)
    assert restored.raw == orig.raw
    assert restored.path == orig.path
    assert restored.provider == orig.provider
    assert restored.kind == orig.kind


@pytest.mark.skip('may not be needed anymore?')
def test_metadata_and_path_in_same_structure():
    """Ensure mixed structures of paths and metadata serialize/deserialize."""
    class DummyMeta(BaseMetadata):
        def __init__(self, raw):
            super().__init__(raw)
        @property
        def provider(self): return "mix"
        @property
        def kind(self): return "file"
        @property
        def name(self): return "mix.txt"
        @property
        def path(self): return "/mix.txt"
        @property
        def etag(self): return "ff00ff"

    orig_path = WaterButlerPath("/mix.txt", _ids=[None, "m"], prepend=None, folder=False)
    orig_meta = DummyMeta({"k": "v"})
    struct = {"p": dehydrate_path(orig_path), "m": dehydrate_metadata(orig_meta)}
    blob = json.dumps(struct)
    restored = rehydrate_metadata(blob)
    assert isinstance(restored["p"], WaterButlerPath)
    assert restored["p"].materialized_path == orig_path.materialized_path
    assert isinstance(restored["m"], DummyMeta)
    assert restored["m"].raw == orig_meta.raw
