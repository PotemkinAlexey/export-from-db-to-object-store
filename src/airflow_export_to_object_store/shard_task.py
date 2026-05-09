"""Module-level shard entry point.

Both ``ThreadPoolExecutor`` and ``ProcessPoolExecutor`` require their
target callable to be importable. The operator's ``_run_single_shard``
method captures ``self`` and isn't safe to pickle across process
boundaries (Airflow operators carry hooks, loggers and DB context that
are not all picklable).

This module provides a small frozen-dataclass parameter object plus a
plain function that re-resolves the storage hook, builds a thin retry
wrapper around the uploader, runs the shard and returns
``(ShardResult, shard_metric_dict)``. The caller (the operator) merges
the metric dicts into its own ``ExportMetrics`` after each shard
completes so the global summary is correct under both modes.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from typing import Any

from airflow.hooks.base import BaseHook

from .encryption import EncryptionOptions
from .metrics import ExportMetrics
from .options import ParquetOptions, RetryOptions, ShardOptions, ShardResult
from .parquet_io import ShardWorker, TransformFn
from .retry import with_retries
from .uploaders import resolve_uploader

_SCHEMES = {"azure": "azure", "s3": "s3", "gcs": "gs"}


def _canonical_uri(uploader_name: str, params: ShardTaskParams) -> str:
    """Produce the same ``scheme://container_or_bucket/remote_path`` string
    each backend's ``upload()`` returns, without touching the network."""
    scheme = _SCHEMES.get(uploader_name, uploader_name)
    bucket_or_container = params.container if uploader_name == "azure" else params.bucket
    if not bucket_or_container:
        return ""
    return f"{scheme}://{bucket_or_container}/{params.remote_path}"


@dataclass(frozen=True)
class ShardTaskParams:
    shard_index: int
    sql_text: str
    filename: str
    remote_path: str
    db_hook_id: str
    storage_hook_id: str
    tmp_dir: str | None
    container: str | None
    bucket: str | None
    overwrite: bool
    compute_md5: bool
    validate_parquet: bool
    parquet_options: ParquetOptions
    shard_options: ShardOptions
    retry_options: RetryOptions
    skip_if_exists: bool = False
    # Optional row-level transform applied to every Arrow batch before write.
    # MUST be a top-level callable (importable by name) when running with
    # ``execution_mode='processes'`` — closures and lambdas cannot be pickled.
    transform_fn: TransformFn | None = None
    encryption: EncryptionOptions | None = None
    tags: dict[str, str] | None = None


class _UploadHost:
    """Tiny holder used by the @with_retries decorator (it reads ``self.log``
    and ``self.retry_options``). Pickling it is straightforward — only the
    module-level function reaches it."""

    def __init__(self, log: logging.Logger, retry_options: RetryOptions) -> None:
        self.log = log
        self.retry_options = retry_options

    @with_retries
    def upload(
        self,
        storage_hook: Any,
        local_path: str,
        remote_path: str,
        *,
        container: str | None,
        bucket: str | None,
        overwrite: bool,
        storage_hook_id: str,
        encryption: EncryptionOptions | None = None,
        tags: dict[str, str] | None = None,
    ) -> str:
        uploader = resolve_uploader(storage_hook)
        return uploader.upload(
            storage_hook,
            local_path,
            remote_path,
            container=container,
            bucket=bucket,
            overwrite=overwrite,
            storage_hook_id=storage_hook_id,
            log=self.log,
            encryption=encryption,
            tags=tags,
        )


def execute_shard(params: ShardTaskParams, cancel: threading.Event | None = None) -> tuple[ShardResult, dict]:
    """Run one shard end-to-end.

    ``cancel`` is honoured only when running in the same process — i.e.
    in ``execution_mode="threads"``. ``ProcessPoolExecutor`` cannot share
    a ``threading.Event`` with subprocesses, so the operator passes
    ``None`` in that mode (running shards complete naturally; only
    not-yet-started futures are cancelled).
    """
    log = logging.getLogger(f"export.shard.{params.shard_index}")

    storage_hook = BaseHook.get_hook(params.storage_hook_id)
    uploader = resolve_uploader(storage_hook)

    # Idempotency short-circuit: if the destination already has an object,
    # don't even open a DB cursor. Cheaper than fetching+writing+comparing.
    if params.skip_if_exists:
        try:
            exists = uploader.exists(
                storage_hook,
                container=params.container,
                bucket=params.bucket,
                remote_path=params.remote_path,
            )
        except Exception as e:  # pragma: no cover - defensive
            log.warning("exists() probe failed (%s) — proceeding with upload", e)
            exists = False
        if exists:
            log.info("Already present at %s — skipping shard %d", params.remote_path, params.shard_index)
            remote_uri = _canonical_uri(uploader.name, params)
            elapsed = 0.0
            shard_metric = {
                "shard_index": params.shard_index,
                "rows": 0,
                "bytes": 0,
                "bytes_mb": 0.0,
                "duration_s": elapsed,
                "throughput_rows_s": 0.0,
                "throughput_mb_s": 0.0,
                "skipped": True,
            }
            return (
                ShardResult(
                    shard_index=params.shard_index,
                    remote_uri=remote_uri,
                    rows=0,
                    bytes=0,
                    md5=None,
                    elapsed_s=elapsed,
                    skipped=True,
                ),
                shard_metric,
            )

    host = _UploadHost(log=log, retry_options=params.retry_options)

    def _upload_fn(hook: Any, local_path: str, remote_path: str) -> str:
        return host.upload(
            hook,
            local_path,
            remote_path,
            container=params.container,
            bucket=params.bucket,
            overwrite=params.overwrite,
            storage_hook_id=params.storage_hook_id,
            encryption=params.encryption,
            tags=params.tags,
        )

    # A throwaway metrics object scoped to this shard. The parent operator
    # merges the resulting per-shard dict into its own ExportMetrics.
    shard_metrics = ExportMetrics(host)
    shard_metrics.start()

    worker = ShardWorker(
        shard_index=params.shard_index,
        sql_text=params.sql_text,
        filename=params.filename,
        remote_path=params.remote_path,
        db_hook_id=params.db_hook_id,
        storage_hook=storage_hook,
        tmp_dir_root=params.tmp_dir,
        parquet_options=params.parquet_options,
        shard_options=params.shard_options,
        compute_md5=params.compute_md5,
        validate_parquet=params.validate_parquet,
        upload_fn=_upload_fn,
        metrics=shard_metrics,
        log=log,
        cancel=cancel,
        transform_fn=params.transform_fn,
    )
    result = worker.run()
    shard_metric = shard_metrics.shards[0] if shard_metrics.shards else {}
    return result, shard_metric
