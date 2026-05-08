"""Snowflake ``COPY INTO`` native unload.

Snowflake can write Parquet files **directly** to S3, Azure or GCS via
``COPY INTO '<external_location>' FROM (SELECT ...)``. Two auth modes
are supported:

* **Storage integration** (recommended): pre-configured by a Snowflake
  admin, the strategy refers to it by name and zero credentials cross
  the SQL boundary.
* **Inline credentials**: the SQL embeds an ``AWS_KEY_ID`` /
  ``AWS_SECRET_KEY`` etc. block. Convenient for ad-hoc testing, less so
  for production.

Snowflake auto-shards the output across its compute nodes: with
``MAX_FILE_SIZE`` it produces ``data_<thread>_<chunk>_0.snappy.parquet``
style filenames, all under the prefix we passed.

The strategy parses the result set Snowflake returns from ``COPY INTO``
(one row per file) into :class:`ShardResult` objects so the manifest
writer downstream sees the unload exactly like a sharded fetch run.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

try:
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
except ImportError:
    SnowflakeHook = None  # type: ignore[assignment]

try:
    from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
except ImportError:
    WasbHook = None  # type: ignore[assignment]

try:
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
except ImportError:
    S3Hook = None  # type: ignore[assignment]

try:
    from airflow.providers.google.cloud.hooks.gcs import GCSHook
except ImportError:
    GCSHook = None  # type: ignore[assignment]

from ..options import ShardResult


@dataclass(frozen=True)
class SnowflakeUnloadOptions:
    """Server-side ``COPY INTO`` options exposed to the user.

    Fields map 1:1 onto Snowflake clauses; defaults match the reasonable
    "Parquet, zstd, multi-file under MAX_FILE_SIZE" production setup.
    """

    # Auth (exactly one must be set).
    storage_integration: str | None = None
    credentials: dict[str, str] | None = None

    # COPY INTO knobs.
    file_format: str = "PARQUET"
    compression: str = "ZSTD"  # PARQUET-only: NONE | SNAPPY | LZO | BROTLI | LZ4 | ZSTD | GZIP
    max_file_size: int = 256 * 1024 * 1024  # 256 MiB chunks → many small files, parallel reads downstream
    single: bool = False  # True writes one large file
    overwrite: bool = True
    header: bool = False  # Parquet ignores HEADER, kept for CSV/JSON future use
    extra_options: dict[str, str] = field(default_factory=dict)


class SnowflakeUnloadStrategy:
    """Bulk export from Snowflake into S3 / Azure / GCS via COPY INTO."""

    name = "snowflake"

    def __init__(self, options: SnowflakeUnloadOptions):
        self.options = options

    # ------------------------------------------------------------------
    # Protocol
    # ------------------------------------------------------------------
    def matches(self, db_hook: Any, storage_hook: Any) -> bool:
        if SnowflakeHook is None or not isinstance(db_hook, SnowflakeHook):
            return False
        return _backend(storage_hook) is not None

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
        backend = _backend(storage_hook)
        if backend is None:  # pragma: no cover - guarded by matches()
            raise RuntimeError(f"Unsupported storage hook for Snowflake unload: {type(storage_hook)}")

        target = self._build_target(backend, container=container, bucket=bucket, remote_dir=remote_dir)
        copy_sql = self._build_copy_sql(target=target, select_sql=sql)

        log.info("Snowflake unload → %s", target)
        log.debug("COPY INTO SQL:\n%s", copy_sql)

        rows = db_hook.get_records(copy_sql)
        return _rows_to_shard_results(rows, target=target, log=log)

    # ------------------------------------------------------------------
    # SQL building
    # ------------------------------------------------------------------
    def _build_target(self, backend: str, *, container: str | None, bucket: str | None, remote_dir: str) -> str:
        if backend == "s3":
            if not bucket:
                raise ValueError("bucket must be set for Snowflake → S3 unload")
            return f"s3://{bucket}/{remote_dir.lstrip('/')}"
        if backend == "azure":
            if not container:
                raise ValueError("container must be set for Snowflake → Azure unload")
            account = _azure_account_from_hook_or_raise()
            return f"azure://{account}.blob.core.windows.net/{container}/{remote_dir.lstrip('/')}"
        if backend == "gcs":
            if not bucket:
                raise ValueError("bucket must be set for Snowflake → GCS unload")
            return f"gcs://{bucket}/{remote_dir.lstrip('/')}"
        raise RuntimeError(f"Unknown backend: {backend}")  # pragma: no cover

    def _build_copy_sql(self, *, target: str, select_sql: str) -> str:
        opts = self.options
        if opts.storage_integration and opts.credentials:
            raise ValueError("Set either storage_integration or credentials, not both")
        if not opts.storage_integration and not opts.credentials:
            raise ValueError("One of storage_integration / credentials must be set for Snowflake unload")

        clauses: list[str] = []
        if opts.storage_integration:
            clauses.append(f"STORAGE_INTEGRATION = {opts.storage_integration}")
        if opts.credentials:
            kv = " ".join(f"{k}='{v}'" for k, v in opts.credentials.items())
            clauses.append(f"CREDENTIALS = ({kv})")

        clauses.append(f"FILE_FORMAT = (TYPE = {opts.file_format} COMPRESSION = {opts.compression})")
        clauses.append(f"MAX_FILE_SIZE = {opts.max_file_size}")
        clauses.append(f"SINGLE = {'TRUE' if opts.single else 'FALSE'}")
        clauses.append(f"OVERWRITE = {'TRUE' if opts.overwrite else 'FALSE'}")
        if opts.header:
            clauses.append("HEADER = TRUE")
        for k, v in opts.extra_options.items():
            clauses.append(f"{k} = {v}")

        # Ensure we have a tidy single-line copy statement; the SELECT is wrapped
        # in parens so callers can pass any rendered SQL (with WHERE / JOIN / etc).
        select_clean = select_sql.strip().rstrip(";")
        return f"COPY INTO '{target}'\nFROM ({select_clean})\n" + "\n".join(clauses)


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def _backend(storage_hook: Any) -> str | None:
    if S3Hook is not None and isinstance(storage_hook, S3Hook):
        return "s3"
    if WasbHook is not None and isinstance(storage_hook, WasbHook):
        return "azure"
    if GCSHook is not None and isinstance(storage_hook, GCSHook):
        return "gcs"
    return None


def _azure_account_from_hook_or_raise() -> str:
    """Snowflake's azure:// URL needs the storage account name. We don't
    have a clean way to introspect WasbHook for it across Airflow versions,
    so for now require the caller to encode it via container or fall back
    to an explicit error. v1 leaves Azure unload off the happy path."""
    raise NotImplementedError(
        "Snowflake → Azure unload requires the storage account name; pass it via "
        "SnowflakeUnloadOptions.extra_options or open an issue with your setup."
    )


def _rows_to_shard_results(rows: list[Any], *, target: str, log: logging.Logger) -> list[ShardResult]:
    """Snowflake COPY INTO returns one row per produced file.

    Column order (Snowflake docs):
        rows_unloaded, input_bytes, output_bytes, file_name, ...

    Some driver versions use slightly different orderings or include
    additional columns. We index defensively by name when available and
    fall back to positions otherwise.
    """
    results: list[ShardResult] = []
    for idx, row in enumerate(rows or []):
        rows_unloaded, output_bytes, file_name = _extract_unload_columns(row)
        # Snowflake returns relative file names like 'data_0_0_0.snappy.parquet'.
        # The canonical URI is target dir + the produced file name.
        remote_uri = target.rstrip("/") + "/" + (file_name or "").lstrip("/")
        results.append(
            ShardResult(
                shard_index=idx,
                remote_uri=remote_uri,
                rows=int(rows_unloaded or 0),
                bytes=int(output_bytes or 0),
                md5=None,  # Snowflake does not return MD5 in COPY INTO
                elapsed_s=0.0,  # Server-side — no per-file client timings.
                skipped=False,
            )
        )
    log.info("Snowflake produced %d file(s)", len(results))
    return results


def _extract_unload_columns(row: Any) -> tuple[int, int, str | None]:
    """Best-effort extraction of (rows_unloaded, output_bytes, file_name) from
    a Snowflake COPY INTO result row, regardless of driver row shape."""
    if isinstance(row, dict):
        return (
            int(row.get("rows_unloaded") or row.get("ROWS_UNLOADED") or 0),
            int(row.get("output_bytes") or row.get("OUTPUT_BYTES") or 0),
            row.get("file_name") or row.get("FILE_NAME"),
        )
    # Tuple / list — column order per Snowflake docs:
    # (file_name, rows_unloaded, input_bytes, output_bytes, ...)
    # but legacy variants put rows_unloaded first; fall back gracefully.
    if isinstance(row, (list, tuple)):
        if len(row) >= 4 and isinstance(row[0], str):
            file_name, rows_unloaded, _input_bytes, output_bytes = row[0], row[1], row[2], row[3]
        elif len(row) >= 4:
            rows_unloaded, _input_bytes, output_bytes, file_name = row[0], row[1], row[2], row[3]
        else:
            rows_unloaded = row[0] if len(row) > 0 else 0
            output_bytes = row[2] if len(row) > 2 else 0
            file_name = row[3] if len(row) > 3 else None
        return int(rows_unloaded or 0), int(output_bytes or 0), file_name
    return 0, 0, None
