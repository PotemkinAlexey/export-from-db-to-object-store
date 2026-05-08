"""Universal streaming export operator.

Execute SQL via any DB hook (PEP-249 / Airflow Connection) → stream Arrow
batches → write Parquet → upload to object storage (Azure Blob, AWS S3,
or Google Cloud Storage).
"""
from __future__ import annotations

import os
import shutil
import tempfile
import threading
import time
from concurrent.futures import FIRST_EXCEPTION, ProcessPoolExecutor, ThreadPoolExecutor, wait
from typing import Any

import pyarrow as pa
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator

from .manifest import build_manifest, resolve_manifest_path, write_manifest_local
from .metrics import ExportMetrics
from .options import ParquetOptions, RetryOptions, ShardOptions, ShardResult
from .parquet_validator import validate_parquet_schema
from .shard_task import ShardTaskParams, execute_shard
from .templating import (
    flatten_and_render_params as _flatten_and_render_params,
)
from .templating import render_path_template, render_template
from .tracing import span as _span
from .unload import UnloadStrategy
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
        query: str | None = None,
        sql_template: str | None = None,
        sql_params: dict[str, Any] | None = None,
        shards: list[dict[str, Any]] | None = None,
        filename_template: str = "data_{{ '%03d' | format(shard_index) }}.parquet",
        remote_path_template: str = "{{ ds }}/data_{{ '%03d' | format(shard_index) }}.parquet",
        container: str | None = None,
        bucket: str | None = None,
        parquet_options: ParquetOptions | None = None,
        retry_options: RetryOptions | None = None,
        shard_options: ShardOptions | None = None,
        tmp_dir: str | None = None,
        compute_md5: bool = False,
        overwrite: bool = True,
        log_timings: bool = True,
        validate_parquet: bool = True,
        skip_if_exists: bool = False,
        write_manifest: bool = False,
        manifest_path: str | None = None,
        unload_strategy: UnloadStrategy | None = None,
        unload_dir_template: str = "{{ ds }}/",
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
        self.skip_if_exists = skip_if_exists
        self.write_manifest = write_manifest
        self.manifest_path = manifest_path
        self.unload_strategy = unload_strategy
        self.unload_dir_template = unload_dir_template

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

        with _span(
            "export.execute",
            **{
                "export.task_id": self.task_id,
                "export.db_hook_id": self.db_hook_id,
                "export.storage_hook_id": self.storage_hook_id,
                "export.mode": "unload" if self.unload_strategy is not None else "stream",
                "export.shards": len(self.shards),
            },
        ):
            return self._execute_inner(context, t0)

    def _execute_inner(self, context, t0):
        self._clean_old_tmp_dirs()

        # Pre-flight checks
        self._health_checks(context)

        self._metrics.start()

        # Native unload bypass: ask the warehouse to write directly to the
        # bucket instead of streaming through this process.
        if self.unload_strategy is not None:
            results, unload_dir = self._run_unload(context)
            if self.write_manifest:
                self._write_manifest_at(results, unload_dir)
            elapsed = time.time() - t0
            summary = self._metrics.summary()
            self.log.info(
                "Native unload completed in %.2fs — rows=%d bytes=%.1fMB",
                elapsed,
                summary.get("total_rows", 0),
                summary.get("total_bytes_mb", 0),
            )
            return {
                "shards": [r.__dict__ for r in sorted(results, key=lambda x: x.shard_index)],
                "metrics": summary,
                "total_rows": summary.get("total_rows", 0),
                "total_bytes": summary.get("total_bytes", 0),
                "elapsed_s": elapsed,
                "mode": "unload",
            }

        shard_params: list[ShardTaskParams] = []

        # Prepare each shard
        for idx, shard in enumerate(self.shards):
            shard_ctx = {**context, **self.sql_params, **shard, "shard_index": idx}

            sql_text = self.query or self._render_template(self.sql_template, shard_ctx, "SQL")
            filename = self._render_template_str(self.filename_template, shard_ctx)
            remote_path = self._render_template_str(self.remote_path_template, shard_ctx)

            shard_params.append(
                ShardTaskParams(
                    shard_index=idx,
                    sql_text=sql_text,
                    filename=filename,
                    remote_path=remote_path,
                    db_hook_id=self.db_hook_id,
                    storage_hook_id=self.storage_hook_id,
                    tmp_dir=self.tmp_dir,
                    container=self.container,
                    bucket=self.bucket,
                    overwrite=self.overwrite,
                    compute_md5=self.compute_md5,
                    validate_parquet=self.validate_parquet,
                    parquet_options=self.parquet_options,
                    shard_options=self.shard_options,
                    retry_options=self.retry_options,
                    skip_if_exists=self.skip_if_exists,
                )
            )

        results = self._run_shards(shard_params)

        if self.write_manifest:
            self._write_manifest(results, [p.remote_path for p in shard_params])

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
    # Shard orchestration: pool selection + cancellation propagation.
    # ------------------------------------------------------------------
    def _run_shards(self, shard_params: list[ShardTaskParams]) -> list[ShardResult]:
        mode = self.shard_options.execution_mode
        max_workers = self.shard_options.max_workers

        if mode == "threads":
            cancel = threading.Event()
            pool = ThreadPoolExecutor(max_workers=max_workers)
        elif mode == "processes":
            cancel = None  # threading.Event cannot cross process boundaries
            pool = ProcessPoolExecutor(max_workers=max_workers)
        else:  # pragma: no cover - guarded by Literal
            raise ValueError(f"Unknown execution_mode: {mode}")

        self.log.info("Running %d shard(s) on %s pool (max_workers=%d)", len(shard_params), mode, max_workers)

        futures_to_index: dict[Any, int] = {}
        results: list[ShardResult | None] = [None] * len(shard_params)

        try:
            for params in shard_params:
                if mode == "threads":
                    future = pool.submit(execute_shard, params, cancel)
                else:
                    future = pool.submit(execute_shard, params)
                futures_to_index[future] = params.shard_index

            done, not_done = wait(list(futures_to_index), return_when=FIRST_EXCEPTION)

            first_exc: BaseException | None = None
            for future in done:
                exc = future.exception()
                if exc is not None and first_exc is None:
                    first_exc = exc

            if first_exc is not None:
                self.log.warning("Shard failure detected — cancelling siblings: %s", first_exc)
                if cancel is not None:
                    cancel.set()
                for f in not_done:
                    f.cancel()
                # Wait for already-running shards to finish (cleanly, since cancel
                # is set in threads mode; in processes mode they run to completion).
                wait(not_done)
                raise first_exc

            for future, idx in futures_to_index.items():
                shard_result, shard_metric = future.result()
                results[idx] = shard_result
                if shard_metric:
                    self._metrics.shards.append(shard_metric)
        finally:
            pool.shutdown(wait=True)

        # All slots are now non-None.
        return [r for r in results if r is not None]

    # ------------------------------------------------------------------
    # NATIVE UNLOAD
    # ------------------------------------------------------------------
    def _run_unload(self, context: dict[str, Any]) -> tuple[list[ShardResult], str]:  # noqa: D401
        with _span("export.unload", **{"unload.strategy": type(self.unload_strategy).__name__}):
            return self._run_unload_inner(context)

    def _run_unload_inner(self, context: dict[str, Any]) -> tuple[list[ShardResult], str]:
        """Delegate to the warehouse-native bulk export strategy.

        We render a single SQL (no shards: the warehouse parallelises)
        and a single remote directory, then ask the strategy to produce
        ``ShardResult``\\s for each file the warehouse wrote. Returns the
        results plus the resolved remote directory (used by the manifest
        writer in unload mode).
        """
        sql = self.query or self._render_template(self.sql_template, dict(context), "SQL")
        remote_dir = self._render_template_str(self.unload_dir_template, dict(context))

        db_hook = BaseHook.get_hook(self.db_hook_id)
        storage_hook = BaseHook.get_hook(self.storage_hook_id)

        if not self.unload_strategy.matches(db_hook, storage_hook):
            raise ValueError(
                f"unload_strategy {type(self.unload_strategy).__name__} does not match "
                f"the configured db_hook ({type(db_hook).__name__}) and storage_hook "
                f"({type(storage_hook).__name__})."
            )

        results = self.unload_strategy.unload(
            db_hook=db_hook,
            storage_hook=storage_hook,
            sql=sql,
            remote_dir=remote_dir,
            container=self.container,
            bucket=self.bucket,
            log=self.log,
        )

        # Backfill the global metrics so summary() stays consistent across
        # streaming and unload modes.
        for r in results:
            self._metrics.record_shard(r.shard_index, r.rows, r.bytes, r.elapsed_s)

        return results, remote_dir

    def _write_manifest_at(self, results: list[ShardResult], remote_dir: str) -> None:
        """Manifest writer for native unload: directory is known up-front."""
        if not results:
            self.log.info("Skipping manifest: unload produced no files.")
            return
        manifest_remote = self.manifest_path or (remote_dir.rstrip("/") + "/_manifest.json").lstrip("/")
        manifest = build_manifest(results)
        with tempfile.TemporaryDirectory() as td:
            local = os.path.join(td, "_manifest.json")
            size = write_manifest_local(manifest, local)
            self.log.info("Manifest %d bytes → %s", size, manifest_remote)

            storage_hook = BaseHook.get_hook(self.storage_hook_id)
            uploader = resolve_uploader(storage_hook)
            uri = uploader.upload(
                storage_hook,
                local,
                manifest_remote,
                container=self.container,
                bucket=self.bucket,
                overwrite=True,
                storage_hook_id=self.storage_hook_id,
                log=self.log,
            )
            self.log.info("Manifest uploaded → %s", uri)

    # ------------------------------------------------------------------
    # MANIFEST
    # ------------------------------------------------------------------
    def _write_manifest(self, results: list[ShardResult], remote_paths: list[str]) -> None:
        manifest_remote = resolve_manifest_path(self.manifest_path, results, remote_paths)
        if not manifest_remote:
            self.log.info("Skipping manifest: no shards produced output.")
            return

        manifest = build_manifest(results)
        with tempfile.TemporaryDirectory() as td:
            local = os.path.join(td, "_manifest.json")
            size = write_manifest_local(manifest, local)
            self.log.info("Manifest %d bytes → %s", size, manifest_remote)

            storage_hook = BaseHook.get_hook(self.storage_hook_id)
            uploader = resolve_uploader(storage_hook)
            uri = uploader.upload(
                storage_hook,
                local,
                manifest_remote,
                container=self.container,
                bucket=self.bucket,
                overwrite=True,
                storage_hook_id=self.storage_hook_id,
                log=self.log,
            )
            self.log.info("Manifest uploaded → %s", uri)

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
    def _render_template(self, template_str: str, ctx: dict[str, Any], label: str = "template") -> str:
        return render_template(template_str, ctx, self.sql_params, self.log, label=label)

    def _render_template_str(self, template_str: str, ctx: dict[str, Any]) -> str:
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
