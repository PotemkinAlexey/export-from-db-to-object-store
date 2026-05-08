"""Manifest builder/writer tests (no Airflow needed)."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from airflow_export_to_object_store.manifest import (
    MANIFEST_VERSION,
    build_manifest,
    common_remote_dir,
    resolve_manifest_path,
    write_manifest_local,
)
from airflow_export_to_object_store.options import ShardResult


def _r(idx: int, *, rows: int = 100, bytes_: int = 200, skipped: bool = False, uri: str | None = None) -> ShardResult:
    return ShardResult(
        shard_index=idx,
        remote_uri=uri or f"s3://bucket/path/data_{idx}.parquet",
        rows=rows,
        bytes=bytes_,
        md5=None,
        elapsed_s=0.1,
        skipped=skipped,
    )


def test_common_remote_dir_basic():
    assert common_remote_dir(["a/b/x.parquet", "a/b/y.parquet"]) == "a/b"
    assert common_remote_dir(["a/x.parquet", "b/y.parquet"]) == ""
    assert common_remote_dir([]) == ""


def test_common_remote_dir_partial_filename_match():
    """Common prefix that doesn't end on a slash must be trimmed back."""
    assert common_remote_dir(["a/b/data_001.parquet", "a/b/data_002.parquet"]) == "a/b"


def test_resolve_manifest_path_explicit_wins():
    assert resolve_manifest_path("custom/path.json", [_r(0)], ["a/b.parquet"]) == "custom/path.json"


def test_resolve_manifest_path_default_uses_common_dir():
    paths = ["exports/2026/data_000.parquet", "exports/2026/data_001.parquet"]
    assert resolve_manifest_path(None, [_r(0), _r(1)], paths) == "exports/2026/_manifest.json"


def test_resolve_manifest_path_no_remote_paths_returns_none():
    assert resolve_manifest_path(None, [], []) is None


def test_build_manifest_aggregates_totals_and_sorts():
    when = datetime(2026, 5, 8, 12, 0, 0, tzinfo=timezone.utc)
    m = build_manifest(
        [_r(2, rows=300, bytes_=600), _r(0, rows=100, bytes_=200), _r(1, rows=200, bytes_=400)],
        exported_at=when,
    )
    assert m["version"] == MANIFEST_VERSION
    assert m["exported_at"] == "2026-05-08T12:00:00+00:00"
    assert m["total_rows"] == 600
    assert m["total_bytes"] == 1200
    # Sorted by shard_index for deterministic downstream consumers.
    assert [f["shard_index"] for f in m["files"]] == [0, 1, 2]


def test_build_manifest_includes_skipped_flag():
    m = build_manifest([_r(0, skipped=True), _r(1, skipped=False)])
    assert [f["skipped"] for f in m["files"]] == [True, False]


def test_write_manifest_local_round_trip(tmp_path):
    manifest = build_manifest([_r(0)])
    target = tmp_path / "_manifest.json"
    size = write_manifest_local(manifest, str(target))
    assert size == target.stat().st_size
    parsed = json.loads(target.read_text())
    assert parsed["version"] == MANIFEST_VERSION
    assert parsed["files"][0]["shard_index"] == 0


def test_write_manifest_local_atomic(tmp_path, monkeypatch):
    """A failure during write must not leave a corrupt or empty target."""
    target = tmp_path / "out.json"
    target.write_text("PRE-EXISTING")

    manifest = build_manifest([_r(0)])

    def boom(_src, _dst):
        raise OSError("disk full simulated")

    monkeypatch.setattr("os.replace", boom)
    with pytest.raises(OSError, match="disk full"):
        write_manifest_local(manifest, str(target))

    # Original content is untouched.
    assert target.read_text() == "PRE-EXISTING"
    # No leftover temp manifest files in the directory.
    leftover = [p for p in tmp_path.iterdir() if p.name.startswith("manifest-")]
    assert leftover == []
