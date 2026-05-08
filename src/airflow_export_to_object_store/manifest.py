"""Build and upload an export manifest.

The manifest is a single JSON object listing every file written by the
operator, enough metadata for downstream consumers (Athena, Trino, Spark,
schema registries) to act on the export atomically without listing the
bucket. Schema is intentionally minimal and forward-compatible:

    {
      "version": 1,
      "exported_at": "2026-05-08T12:34:56+00:00",
      "total_rows": 1500000,
      "total_bytes": 1234567890,
      "files": [
        {
          "shard_index": 0,
          "remote_uri": "s3://bucket/exports/2026-05-08/data_000.parquet",
          "remote_path": "exports/2026-05-08/data_000.parquet",
          "rows": 250000,
          "bytes": 205678123,
          "md5": "...",
          "skipped": false
        }
      ]
    }
"""

from __future__ import annotations

import json
import os
import posixpath
import tempfile
from datetime import datetime, timezone
from typing import Any

from .options import ShardResult

MANIFEST_VERSION = 1


def common_remote_dir(remote_paths: list[str]) -> str:
    """Greatest common prefix of remote paths, truncated at the last ``/``.

    Used to choose a default location for the manifest when the user did
    not specify ``manifest_path``.
    """
    if not remote_paths:
        return ""
    common = os.path.commonprefix(remote_paths)
    # Trim back to the last slash so we don't generate something like
    # ``exports/2026-05-08/data_`` and then append ``_manifest.json``.
    if "/" in common:
        return common.rsplit("/", 1)[0]
    return ""


def build_manifest(
    results: list[ShardResult],
    *,
    exported_at: datetime | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Render the manifest as a plain dict (caller serialises)."""
    when = exported_at or datetime.now(timezone.utc)
    files: list[dict[str, Any]] = []
    for r in sorted(results, key=lambda x: x.shard_index):
        files.append(
            {
                "shard_index": r.shard_index,
                "remote_uri": r.remote_uri,
                "rows": r.rows,
                "bytes": r.bytes,
                "md5": r.md5,
                "skipped": r.skipped,
            }
        )
    manifest: dict[str, Any] = {
        "version": MANIFEST_VERSION,
        "exported_at": when.isoformat(),
        "total_rows": sum(r.rows for r in results),
        "total_bytes": sum(r.bytes for r in results),
        "files": files,
    }
    if extra:
        manifest["extra"] = extra
    return manifest


def resolve_manifest_path(
    explicit_path: str | None,
    results: list[ShardResult],
    remote_paths: list[str],
) -> str | None:
    """Decide where to write the manifest.

    1. If ``explicit_path`` was provided, use it as-is.
    2. Else compute the common directory of all shard remote paths and
       append ``_manifest.json``.
    3. If neither is available (no shards), return ``None`` and the caller
       skips writing.
    """
    if explicit_path:
        return explicit_path
    if not remote_paths:
        return None
    base = common_remote_dir(remote_paths)
    if not base:
        return "_manifest.json"
    return posixpath.join(base, "_manifest.json")


def write_manifest_local(manifest: dict[str, Any], path: str) -> int:
    """Atomically write manifest JSON to a local path. Returns bytes written."""
    payload = json.dumps(manifest, indent=2, sort_keys=False).encode("utf-8")
    fd, tmp = tempfile.mkstemp(prefix="manifest-", suffix=".json", dir=os.path.dirname(path) or None)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(payload)
        os.replace(tmp, path)
    except Exception:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise
    return len(payload)
