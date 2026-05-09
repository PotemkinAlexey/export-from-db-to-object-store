"""BigQuery ``EXPORT DATA`` native unload.

BigQuery can write Parquet directly to Google Cloud Storage with a
single statement::

    EXPORT DATA OPTIONS(
        uri='gs://bucket/path/*.parquet',
        format='PARQUET',
        compression='ZSTD',
        overwrite=true
    ) AS
    SELECT * FROM dataset.table WHERE date = '2026-05-08'

The strategy issues that statement against the user's BigQuery hook,
then lists the destination prefix on GCS to discover what landed —
``EXPORT DATA``'s result set is empty, so there is no per-file row
count from BigQuery itself. Total rows are read from the SELECT's
preceding count when available; otherwise we leave per-file
``rows = 0`` and let the manifest's bucket-side ``bytes`` carry the
signal downstream.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

try:
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
except ImportError:
    BigQueryHook = None  # type: ignore[assignment]

try:
    from airflow.providers.google.cloud.hooks.gcs import GCSHook
except ImportError:
    GCSHook = None  # type: ignore[assignment]

from ..options import ShardResult


@dataclass(frozen=True)
class BigQueryUnloadOptions:
    """Options for the ``EXPORT DATA OPTIONS(...)`` clause.

    Defaults match a "Parquet, zstd, multi-file with auto-shard" setup
    that downstream readers (Athena, Trino, Spark, BigQuery itself)
    consume happily.
    """

    file_format: str = "PARQUET"
    compression: str = "ZSTD"  # PARQUET-only: NONE | SNAPPY | ZSTD | GZIP
    overwrite: bool = True
    # ``EXPORT DATA`` always auto-shards; the wildcard ``*`` is required for
    # any output > 1 GB. We default to a wildcard suffix so users get
    # the parallelisation by default.
    file_pattern: str = "*.parquet"
    extra_options: dict[str, str] = field(default_factory=dict)


class BigQueryUnloadStrategy:
    """Bulk export from BigQuery into GCS via ``EXPORT DATA``."""

    name = "bigquery"

    def __init__(self, options: BigQueryUnloadOptions | None = None):
        self.options = options or BigQueryUnloadOptions()

    # ------------------------------------------------------------------
    # Protocol
    # ------------------------------------------------------------------
    def matches(self, db_hook: Any, storage_hook: Any) -> bool:
        if BigQueryHook is None or not isinstance(db_hook, BigQueryHook):
            return False
        return GCSHook is not None and isinstance(storage_hook, GCSHook)

    def unload(
        self,
        *,
        db_hook: Any,
        storage_hook: Any,
        sql: str,
        remote_dir: str,
        container: str | None,
        bucket: str | None,
        log: logging.Logger,
    ) -> list[ShardResult]:
        if not bucket:
            raise ValueError("bucket must be set for BigQuery → GCS unload")

        prefix = remote_dir.lstrip("/").rstrip("/") + "/" if remote_dir.strip("/") else ""
        target_uri = f"gs://{bucket}/{prefix}{self.options.file_pattern}"
        export_sql = self._build_export_sql(target_uri=target_uri, select_sql=sql)

        log.info("BigQuery EXPORT DATA → %s", target_uri)
        log.debug("EXPORT DATA SQL:\n%s", export_sql)

        # ``EXPORT DATA`` returns an empty result set; we just wait for it to
        # complete by calling get_records (which executes + drains).
        db_hook.get_records(export_sql)

        # Discover what BigQuery actually wrote. ``EXPORT DATA`` doesn't
        # surface destination URIs so we list the prefix.
        return _list_gcs_results(storage_hook, bucket=bucket, prefix=prefix, log=log)

    # ------------------------------------------------------------------
    # SQL building
    # ------------------------------------------------------------------
    def _build_export_sql(self, *, target_uri: str, select_sql: str) -> str:
        opts = self.options
        clauses = [
            f"uri='{target_uri}'",
            f"format='{opts.file_format}'",
            f"compression='{opts.compression}'",
            f"overwrite={'true' if opts.overwrite else 'false'}",
        ]
        for k, v in opts.extra_options.items():
            clauses.append(f"{k}={v}")
        select_clean = select_sql.strip().rstrip(";")
        joined = ",\n    ".join(clauses)
        return f"EXPORT DATA OPTIONS(\n    {joined}\n) AS\n{select_clean}"


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def _list_gcs_results(storage_hook: Any, *, bucket: str, prefix: str, log: logging.Logger) -> list[ShardResult]:
    """Translate a GCS prefix listing into ``ShardResult``s."""
    keys = list(storage_hook.list(bucket_name=bucket, prefix=prefix))
    keys = [k for k in keys if not k.endswith("/")]
    log.info("EXPORT DATA produced %d file(s) under gs://%s/%s", len(keys), bucket, prefix)

    results: list[ShardResult] = []
    for idx, key in enumerate(sorted(keys)):
        size = _safe_get_size(storage_hook, bucket, key)
        results.append(
            ShardResult(
                shard_index=idx,
                remote_uri=f"gs://{bucket}/{key}",
                rows=0,  # EXPORT DATA does not emit per-file row counts
                bytes=size,
                md5=None,
                elapsed_s=0.0,
                skipped=False,
            )
        )
    return results


def _safe_get_size(storage_hook: Any, bucket: str, object_name: str) -> int:
    """Best-effort byte size lookup; returns 0 when the API is unavailable."""
    try:
        size = storage_hook.get_size(bucket_name=bucket, object_name=object_name)
        return int(size or 0)
    except Exception:
        return 0
