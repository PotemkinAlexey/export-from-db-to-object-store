"""Redshift ``UNLOAD`` native unload.

Redshift writes Parquet directly to S3 in parallel from every compute
slice::

    UNLOAD ('SELECT * FROM events WHERE date = ''2026-05-08''')
    TO 's3://bucket/path/'
    IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftUnload'
    FORMAT AS PARQUET
    PARALLEL ON
    MAXFILESIZE 256 MB
    CLEANPATH

The strategy escapes the user's SELECT (single-quotes inside ``UNLOAD``
must be doubled), assembles the SQL, runs it via the Redshift hook,
and lists the resulting S3 prefix to build :class:`ShardResult`
objects. Per-file row counts are not in the immediate result; we
leave them at ``0`` and downstream consumers should rely on the
manifest's bytes / file list (or query ``STL_UNLOAD_LOG`` if they
need authoritative row counts).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

try:
    from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
except ImportError:
    RedshiftSQLHook = None  # type: ignore[assignment]

try:
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
except ImportError:
    S3Hook = None  # type: ignore[assignment]

from ..options import ShardResult


@dataclass(frozen=True)
class RedshiftUnloadOptions:
    """Options for the ``UNLOAD`` clause.

    Either ``iam_role`` (recommended) or ``credentials`` must be set.
    Defaults match a "Parquet, parallel, 256 MB chunks, OVERWRITE-style
    cleanpath" production setup.
    """

    # Auth (exactly one).
    iam_role: str | None = None
    credentials: str | None = None  # raw "ACCESS_KEY_ID=...;SECRET_ACCESS_KEY=..."

    # UNLOAD knobs.
    file_format: str = "PARQUET"  # PARQUET | CSV | JSON
    parallel: bool = True  # ON: one file per slice, OFF: single sorted file
    max_file_size_mb: int = 256
    cleanpath: bool = True  # delete pre-existing files at the prefix
    manifest: bool = False  # also write Redshift's own manifest.json
    extra_options: list[str] = field(default_factory=list)  # raw clauses appended verbatim


class RedshiftUnloadStrategy:
    """Bulk export from Redshift into S3 via ``UNLOAD``."""

    name = "redshift"

    def __init__(self, options: RedshiftUnloadOptions):
        self.options = options

    # ------------------------------------------------------------------
    # Protocol
    # ------------------------------------------------------------------
    def matches(self, db_hook: Any, storage_hook: Any) -> bool:
        if RedshiftSQLHook is None or not isinstance(db_hook, RedshiftSQLHook):
            return False
        return S3Hook is not None and isinstance(storage_hook, S3Hook)

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
            raise ValueError("bucket must be set for Redshift → S3 unload")

        prefix = remote_dir.lstrip("/").rstrip("/") + "/" if remote_dir.strip("/") else ""
        target = f"s3://{bucket}/{prefix}"
        unload_sql = self._build_unload_sql(target=target, select_sql=sql)

        log.info("Redshift UNLOAD → %s", target)
        log.debug("UNLOAD SQL:\n%s", unload_sql)

        db_hook.get_records(unload_sql)

        return _list_s3_results(storage_hook, bucket=bucket, prefix=prefix, log=log)

    # ------------------------------------------------------------------
    # SQL building
    # ------------------------------------------------------------------
    def _build_unload_sql(self, *, target: str, select_sql: str) -> str:
        opts = self.options
        if opts.iam_role and opts.credentials:
            raise ValueError("Set either iam_role or credentials, not both")
        if not opts.iam_role and not opts.credentials:
            raise ValueError("One of iam_role / credentials must be set for Redshift unload")

        # UNLOAD takes the SELECT as a quoted string — single quotes inside
        # the SELECT must be doubled.
        select_clean = select_sql.strip().rstrip(";")
        select_escaped = select_clean.replace("'", "''")

        clauses: list[str] = []
        if opts.iam_role:
            clauses.append(f"IAM_ROLE '{opts.iam_role}'")
        if opts.credentials:
            clauses.append(f"CREDENTIALS '{opts.credentials}'")

        clauses.append(f"FORMAT AS {opts.file_format}")
        clauses.append("PARALLEL " + ("ON" if opts.parallel else "OFF"))
        clauses.append(f"MAXFILESIZE {opts.max_file_size_mb} MB")
        if opts.cleanpath:
            clauses.append("CLEANPATH")
        if opts.manifest:
            clauses.append("MANIFEST")
        clauses.extend(opts.extra_options)

        return f"UNLOAD ('{select_escaped}')\nTO '{target}'\n" + "\n".join(clauses)


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def _list_s3_results(storage_hook: Any, *, bucket: str, prefix: str, log: logging.Logger) -> list[ShardResult]:
    """Translate an S3 prefix listing into ``ShardResult``s.

    Skips the auxiliary ``manifest`` file that ``UNLOAD ... MANIFEST``
    produces — that's catalog metadata, not data.
    """
    keys = storage_hook.list_keys(bucket_name=bucket, prefix=prefix) or []
    data_keys = [k for k in keys if not k.endswith("manifest") and not k.endswith("/")]
    log.info("UNLOAD produced %d file(s) under s3://%s/%s", len(data_keys), bucket, prefix)

    results: list[ShardResult] = []
    for idx, key in enumerate(sorted(data_keys)):
        size = _safe_get_size(storage_hook, bucket, key)
        results.append(
            ShardResult(
                shard_index=idx,
                remote_uri=f"s3://{bucket}/{key}",
                rows=0,  # not surfaced by UNLOAD; query STL_UNLOAD_LOG if needed
                bytes=size,
                md5=None,
                elapsed_s=0.0,
                skipped=False,
            )
        )
    return results


def _safe_get_size(storage_hook: Any, bucket: str, key: str) -> int:
    try:
        meta = storage_hook.head_object(key=key, bucket_name=bucket)
        return int(meta.get("ContentLength", 0))
    except Exception:
        try:
            obj = storage_hook.get_key(key, bucket_name=bucket)
            return int(getattr(obj, "content_length", 0) or 0)
        except Exception:
            return 0
