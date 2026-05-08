# -*- coding: utf-8 -*-
"""
Universal streaming export operator:
Execute SQL via any DB hook (PEP-249 / Airflow Connection) → stream Arrow batches
→ write Parquet → upload to object storage (Azure Blob or AWS S3).
"""
from __future__ import annotations

import os
import shutil
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

import pyarrow as pa
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator

from .metrics import ExportMetrics
from .options import ParquetOptions, RetryOptions, ShardOptions, ShardResult
from .parquet_io import ShardWorker
from .parquet_validator import validate_parquet_schema
from .retry import with_retries
from .templating import (
    flatten_and_render_params as _flatten_and_render_params,
    render_path_template,
    render_template,
)
from .uploaders import resolve_uploader
from .utils import coerce_ts_table, compute_md5_eff


class StreamingExportOperator(BaseOperator):
    """
    High-throughput streaming export from any SQL database to Parquet, uploaded to Azure Blob or AWS S3.

    Features:
        • Arrow-native or DB-API fallback fetching
        • Memory-safe incremental Parquet writing
        • Automatic batch tuning per shard
        • Parallel shards using ThreadPoolExecutor
        • Azure (Wasb) and S3 (multipart) uploads with retries
        • Per-shard metrics and full summary returned via XCom

    Parameters:
        db_hook_id: Airflow connection to SQL source.
        storage_hook_id: Airflow connection to Azure/S3.
        query/sql_template: SQL text or Jinja template.
        shards: List of shard dicts used in SQL templating.
        filename_template: Local Parquet filename pattern.
        remote_path_template: Cloud path pattern.
        parquet_options: Parquet compression/row groups/etc.
        retry_options: Retry/backoff settings for uploads.
        shard_options: Parallelism, chunk size, timeouts."""

    def __init__(
        self,
        *,
        task_id: str,
        db_hook_id: str,
        storage_hook_id: str,
        query: Optional[str] = None,
        sql_template: Optional[str] = None,
        sql_params: Optional[Dict[str, Any]] = None,
        shards: Optional[List[Dict[str, Any]]] = None,
        filename_template: str = "data_{{ '%03d' | format(shard_index) }}.parquet",
        remote_path_template: str = "{{ ds }}/data_{{ '%03d' | format(shard_index) }}.parquet",
        container: Optional[str] = None,
        bucket: Optional[str] = None,
        parquet_options: Optional[ParquetOptions] = None,
        retry_options: Optional[RetryOptions] = None,
        shard_options: Optional[ShardOptions] = None,
        tmp_dir: Optional[str] = None,
        compute_md5: bool = False,
        overwrite: bool = True,
        log_timings: bool = True,
        validate_parquet: bool = True,
        **kwargs,
    ):

        super().__init__(task_id=task_id, **kwargs)
        self.tmp_dir = tmp_dir

        # Basic validation
        if bool(query) == bool(sql_template):
            raise ValueError("Provide exactly one: query OR sql_template")

        self.db_hook_id = db_hook_id
        self.storage_hook_id = storage_hook_id
        self.query = query
        self.sql_template = sql_template
        self.sql_params = sql_params or {}
        self.shards = shards or [{}]

        self.filename_template = filename_template
        self.remote_path_template = remote_path_template
        self.container = container
        self.bucket = bucket

        self.parquet_options = parquet_options or ParquetOptions()
        self.retry_options = retry_options or RetryOptions()
        self.shard_options = shard_options or ShardOptions()
        self.compute_md5 = compute_md5
        self.overwrite = overwrite
        self.log_timings = log_timings
        self.validate_parquet = validate_parquet

        # === NEW: metrics object ===
        self._metrics = ExportMetrics(self)

    # -------------------------------------------------------
    # Execute
    # -------------------------------------------------------
    def execute(self, context):

        """
        Execute sharded export:
            • Render SQL and paths per shard
            • Run shard in ThreadPoolExecutor
            • Graceful shutdown on error (cancel all futures)
            • Collect metrics into final summary"""
        t0 = time.time()

        self._clean_old_tmp_dirs()

        # Pre-flight checks
        self._health_checks(context)

        self._metrics.start()
        tasks = []

        # Prepare each shard
        for idx, shard in enumerate(self.shards):
            shard_ctx = {**context, **self.sql_params, **shard, "shard_index": idx}

            sql_text = self.query or self._render_template(self.sql_template, shard_ctx, "SQL")

            filename = self._render_template_str(self.filename_template, shard_ctx)
            remote_path = self._render_template_str(self.remote_path_template, shard_ctx)

            tasks.append((idx, sql_text, filename, remote_path))

        from concurrent.futures import FIRST_EXCEPTION, wait

        with ThreadPoolExecutor(max_workers=self.shard_options.max_workers) as pool:
            futures = [pool.submit(self._run_single_shard_task, *task) for task in tasks]

            done, not_done = wait(futures, return_when=FIRST_EXCEPTION)

            for future in done:
                if future.exception():
                    for f in not_done:
                        f.cancel()
                    raise future.exception()

            results = [f.result() for f in futures]

        elapsed = time.time() - t0

        summary = self._metrics.summary()

        self.log.info(
            "Export completed in %.2fs — rows=%d bytes=%.1fMB",
            elapsed,
            summary["total_rows"],
            summary["total_bytes_mb"],
        )

        return {
            "shards": [r.__dict__ for r in sorted(results, key=lambda x: x.shard_index)],
            "metrics": summary,
            "total_rows": summary["total_rows"],
            "total_bytes": summary["total_bytes"],
            "elapsed_s": elapsed,
        }

    @staticmethod
    def flatten_and_render_params(data: dict, ctx: dict) -> dict:
        """Backwards-compatible delegate; see :mod:`templating`."""
        return _flatten_and_render_params(data, ctx)

    # ------------------------------------------------------------------
    # Run a single shard
    # ------------------------------------------------------------------

    def _run_single_shard_task(self, shard_index, sql_text, filename, remote_path):
        storage_hook = BaseHook.get_hook(self.storage_hook_id)
        return self._run_single_shard(shard_index, storage_hook, sql_text, filename, remote_path)

    def _run_single_shard(
        self,
        shard_index: int,
        storage_hook: Any,
        sql_text: str,
        filename: str,
        remote_path: str,
    ) -> ShardResult:
        worker = ShardWorker(
            shard_index=shard_index,
            sql_text=sql_text,
            filename=filename,
            remote_path=remote_path,
            db_hook_id=self.db_hook_id,
            storage_hook=storage_hook,
            tmp_dir_root=self.tmp_dir,
            parquet_options=self.parquet_options,
            shard_options=self.shard_options,
            compute_md5=self.compute_md5,
            validate_parquet=self.validate_parquet,
            upload_fn=self._upload,
            metrics=self._metrics,
            log=self.log,
        )
        return worker.run()

    # ------------------------------------------------------------------
    # UPLOAD WITH RETRIES
    # ------------------------------------------------------------------
    @with_retries
    def _upload(self, storage_hook: Any, local_path: str, remote_path: str) -> str:
        uploader = resolve_uploader(storage_hook)
        return uploader.upload(
            storage_hook,
            local_path,
            remote_path,
            container=self.container,
            bucket=self.bucket,
            overwrite=self.overwrite,
            storage_hook_id=self.storage_hook_id,
            log=self.log,
        )

    # ------------------------------------------------------------------
    # HEALTH CHECKS
    # ------------------------------------------------------------------
    def _network_health_check(self, uploader) -> None:
        """Probe TCP reachability for the resolved uploader's known endpoints."""
        import socket

        targets = list(uploader.network_targets())
        if not targets:
            self.log.info("No network targets for %s → skipping network health checks.", uploader.name)
            return

        for host, port in targets:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(3)
                result = sock.connect_ex((host, port))
                sock.close()
                if result == 0:
                    self.log.info("🌐 Network OK → %s (%s:%d)", uploader.name, host, port)
                else:
                    self.log.warning("⚠ Network may be limited for %s (%s:%d)", uploader.name, host, port)
            except Exception as e:
                self.log.warning("Network check failed for %s (%s): %s", uploader.name, host, e)

    def _memory_health_check(self):
        """Check available memory before starting export."""
        try:
            import psutil

            mem = psutil.virtual_memory()
            available_gb = mem.available / (1024**3)

            self.log.info(
                f"Memory: total={mem.total / (1024**3):.1f}GB, " f"available={available_gb:.1f}GB, " f"used={mem.percent}%"
            )

            if available_gb < 2:
                self.log.warning("Low available memory for export!")

            swap = psutil.swap_memory()
            if swap.percent > 80:
                self.log.warning(f"High swap usage: {swap.percent}%")

        except ImportError:
            self.log.debug("psutil not available, skipping memory check")
        except Exception as e:
            self.log.warning(f"Memory check failed: {e}")

    def _clean_old_tmp_dirs(self) -> None:
        """Remove this operator's leftover tmp dirs older than 24h. Runs once per execute()."""
        if not self.tmp_dir or not os.path.exists(self.tmp_dir):
            return
        try:
            now = time.time()
            for item in os.listdir(self.tmp_dir):
                item_path = os.path.join(self.tmp_dir, item)
                if os.path.isdir(item_path):
                    age = now - os.path.getmtime(item_path)
                    if age > 24 * 3600:
                        shutil.rmtree(item_path, ignore_errors=True)
                        self.log.info("🧹 Cleaned old temp directory: %s", item)
        except Exception as e:
            self.log.warning("Temp cleanup failed: %s", e)

    def _health_checks(self, context):
        """Validate storage reachability, backend permissions, and local disk space."""
        try:
            storage_hook = BaseHook.get_hook(self.storage_hook_id)
            uploader = resolve_uploader(storage_hook)

            self._network_health_check(uploader)
            self._memory_health_check()
            self.log.info("Performing %s health checks...", uploader.name)

            try:
                uploader.health_check(
                    storage_hook,
                    container=self.container,
                    bucket=self.bucket,
                    log=self.log,
                )
            except Exception as e:
                self.log.error("%s health check FAILED: %s", uploader.name, e)
                raise
        except Exception as e:
            self.log.error("Storage health check FAILED: %s", e)
            raise

        tmp_dir = self.tmp_dir or tempfile.gettempdir()
        try:
            stat = shutil.disk_usage(tmp_dir)
            free_gb = stat.free / (1024**3)
            if free_gb < 1.0:
                self.log.warning("Low disk space in %s: %.2f GB free", tmp_dir, free_gb)
        except Exception as e:
            self.log.warning("Could not check disk space: %s", e)

        self.log.info("Health checks OK ✓")

    # ------------------------------------------------------------------
    # TEMPLATE RENDERING (delegates to templating module)
    # ------------------------------------------------------------------
    def _render_template(self, template_str: str, ctx: Dict[str, Any], label: str = "template") -> str:
        return render_template(template_str, ctx, self.sql_params, self.log, label=label)

    def _render_template_str(self, template_str: str, ctx: Dict[str, Any]) -> str:
        return render_path_template(template_str, ctx, self.sql_params, self.log)

    # ------------------------------------------------------------------
    # Backwards-compatible delegates to extracted modules.
    # ------------------------------------------------------------------
    def _coerce_ts_table(self, tbl: pa.Table, target_unit: str) -> pa.Table:
        return coerce_ts_table(tbl, target_unit)

    @staticmethod
    def compute_md5_eff(file_path: str, *, log_fn=None, skip_threshold_gb: int = 10):
        return compute_md5_eff(file_path, log_fn=log_fn, skip_threshold_gb=skip_threshold_gb)

    def _validate_parquet_schema(self, file_path: str) -> bool:
        return validate_parquet_schema(file_path, self.log)
