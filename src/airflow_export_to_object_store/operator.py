# -*- coding: utf-8 -*-
"""
Universal streaming export operator:
Execute SQL via any DB hook (PEP-249 / Airflow Connection) → stream Arrow batches
→ write Parquet → upload to object storage (Azure Blob or AWS S3).
"""
from __future__ import annotations

import os
import queue
import shutil
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from typing import Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator

from .db_adapter import UniversalDbAdapter
from .metrics import ExportMetrics
from .options import ParquetOptions, RetryOptions, ShardOptions, ShardResult
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
        parquet_options: ParquetOptions = ParquetOptions(),
        retry_options: RetryOptions = RetryOptions(),
        shard_options: ShardOptions = ShardOptions(),
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

        self.parquet_options = parquet_options
        self.retry_options = retry_options
        self.shard_options = shard_options
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

        start = time.time()
        prefix = f"[Shard {shard_index}]"

        q = queue.Queue(maxsize=2)
        stop_event = threading.Event()
        errors: List[Exception] = []

        tmp_dir = tempfile.mkdtemp(dir=self.tmp_dir)
        temp_path = os.path.join(tmp_dir, filename)

        with ExitStack() as stack:
            stack.callback(lambda: shutil.rmtree(tmp_dir, ignore_errors=True))

            adapter = UniversalDbAdapter(self.db_hook_id)
            cursor = adapter.cursor()
            stack.callback(adapter.close)

            rows_written = 0

            # -----------------------------
            # Memory budget per shard
            # -----------------------------
            total_mem = self.shard_options.memory_limit_mb  # MB
            workers = max(1, self.shard_options.max_workers)
            safe_total = total_mem * 0.80  # 80% safety cap
            per_shard_mem = max(safe_total / workers, 128)  # Minimum 128 MB

            self.log.info(
                "%s Memory budget: total=%dMB workers=%d → per_shard=%.1fMB", prefix, total_mem, workers, per_shard_mem
            )

            # --------------------------------------------------
            # HEARTBEAT THREAD — reports progress every N sec
            # --------------------------------------------------
            rows_lock = threading.Lock()

            def heartbeat():
                hb_interval = 30
                last_log = time.time()
                while not stop_event.is_set():
                    now = time.time()
                    if now - last_log >= hb_interval:
                        size_mb = os.path.getsize(temp_path) / 1024 / 1024 if os.path.exists(temp_path) else 0.0

                        with rows_lock:
                            current_rows = rows_written

                        self.log.info("%s Heartbeat: %d rows written (%.1fMB)", prefix, current_rows, size_mb)
                        last_log = now

                    # stop_event.wait stops immediately if stop_event is set
                    stop_event.wait(1)

            t_heartbeat = threading.Thread(target=heartbeat, daemon=True, name=f"heartbeat-{shard_index}")
            t_heartbeat.start()

            # ------------------------------------------------------------------
            # FETCH THREAD — reads from DB and pushes Arrow batches to queue
            # ------------------------------------------------------------------
            def fetch():
                try:
                    cursor.execute(sql_text)

                    chunk_rows = self.shard_options.chunk_rows
                    tuned = False

                    is_arrow_native = hasattr(cursor, "fetchmany_arrow")

                    while not stop_event.is_set():
                        # --------------------------------------
                        # FETCH LOGIC
                        # --------------------------------------
                        if is_arrow_native:
                            tbl = cursor.fetchmany_arrow(chunk_rows)
                        else:
                            rows = cursor.fetchmany(chunk_rows)
                            if not rows:
                                tbl = None
                            else:
                                # Column names
                                if not cursor.description:
                                    raise RuntimeError("Cursor returned no description")

                                columns = [d[0] for d in cursor.description]

                                # Fast path — zip(*rows)
                                try:
                                    cols_data = list(zip(*rows))
                                    if len(cols_data) != len(columns):
                                        raise ValueError(f"Column mismatch: got {len(cols_data)}, expected {len(columns)}")
                                    arrays = [pa.array(col, from_pandas=False) for col in cols_data]

                                except Exception as e:
                                    self.log.warning("%s Fast Arrow conversion failed (%s), fallback mode", prefix, e)
                                    arrays = []
                                    for col_idx, col_name in enumerate(columns):
                                        col_values = [row[col_idx] for row in rows]
                                        arrays.append(pa.array(col_values))

                                tbl = pa.Table.from_arrays(arrays, names=columns)

                        if not tbl or tbl.num_rows == 0:
                            break

                        # --------------------------------------
                        # DICTIONARY DECODE (PyArrow < 12 compat)
                        # --------------------------------------
                        try:
                            decoded_cols = []
                            for name in tbl.schema.names:
                                col = tbl[name]
                                if pa.types.is_dictionary(col.type):
                                    col = col.dictionary_decode()
                                decoded_cols.append(col)
                            tbl = pa.table(decoded_cols, names=tbl.schema.names)
                        except Exception as e:
                            self.log.warning("%s Dict decode failed: %s", prefix, e)

                        # --------------------------------------
                        # AUTO-TUNING CHUNK SIZE (first batch only)
                        # --------------------------------------
                        if not tuned:
                            try:
                                # Estimate row size
                                total_bytes = tbl.get_total_size() if hasattr(tbl, "get_total_size") else tbl.nbytes
                                avg_row = max(total_bytes / max(tbl.num_rows, 1), 32)

                                per_shard_bytes = int(per_shard_mem * 1024 * 1024)
                                target_batch_bytes = per_shard_bytes * 0.25  # 25% of budget

                                new_chunk = int(target_batch_bytes / avg_row)

                                # Hard bounds
                                chunk_rows = max(10_000, min(new_chunk, 500_000))

                                self.log.info(
                                    "%s Auto-tuned chunk_rows=%d " "(avg_row=%.1fB, est_batch=%.1fMB, mem_limit=%.1fMB)",
                                    prefix,
                                    chunk_rows,
                                    avg_row,
                                    (avg_row * chunk_rows) / 1048576,
                                    per_shard_mem,
                                )

                            except Exception as e:
                                self.log.warning("%s Auto-tuning failed: %s", prefix, e)

                            tuned = True

                        # --------------------------------------
                        # QUEUE PUT WITH TIMEOUT (deadlock-safe)
                        # --------------------------------------
                        try:
                            q.put(tbl, timeout=2)
                        except queue.Full:
                            if stop_event.is_set():
                                break
                            # Reduce chunk size dynamically if queue is full
                            chunk_rows = max(1000, int(chunk_rows * 0.7))
                            continue

                    # Send termination marker
                    try:
                        q.put_nowait(None)
                    except Exception:
                        pass

                except Exception as e:
                    errors.append(e)
                    stop_event.set()
                    # Ensure writer does not block on full queue
                    while True:
                        try:
                            q.get_nowait()
                        except Exception:
                            break
                    try:
                        q.put_nowait(None)
                    except Exception:
                        pass

            # ------------------------------------------------------------------
            # WRITE THREAD — consumes Arrow batches and writes .parquet
            # ------------------------------------------------------------------
            def write():
                nonlocal rows_written
                writer = None

                try:
                    while True:
                        try:
                            tbl = q.get(timeout=5)
                        except queue.Empty:
                            if stop_event.is_set():
                                break
                            continue

                        # END marker
                        if tbl is None:
                            q.task_done()
                            break

                        # Init ParquetWriter on first batch
                        if writer is None:
                            writer = pq.ParquetWriter(
                                temp_path,
                                tbl.schema,
                                compression=self.parquet_options.compression,
                                write_statistics=self.parquet_options.write_statistics,
                                use_dictionary=self.parquet_options.use_dictionary,
                            )

                        # Coerce timestamp units if required
                        tbl = self._coerce_ts_table(tbl, self.parquet_options.coerce_timestamps or "ms")

                        # Schema alignment
                        if not tbl.schema.equals(writer.schema):
                            try:
                                tbl = tbl.cast(writer.schema)
                            except Exception:
                                self.log.warning("%s Schema mismatch — padding null columns", prefix)
                                tbl = pa.Table.from_arrays(
                                    [
                                        tbl.column(n) if n in tbl.schema.names else pa.nulls(tbl.num_rows)
                                        for n in writer.schema.names
                                    ],
                                    names=writer.schema.names,
                                )

                        writer.write_table(tbl, row_group_size=self.parquet_options.row_group_size)
                        with rows_lock:
                            rows_written += tbl.num_rows
                        q.task_done()

                except Exception as e:
                    errors.append(e)
                    stop_event.set()
                finally:
                    if writer:
                        writer.close()

            # --------------------------------------
            # Start both threads
            # --------------------------------------
            t_fetch = threading.Thread(target=fetch, daemon=True, name=f"fetch-{shard_index}")
            t_write = threading.Thread(target=write, daemon=True, name=f"write-{shard_index}")

            t_fetch.start()
            t_write.start()
            t_fetch.join()
            t_write.join()
            stop_event.set()
            t_heartbeat.join(timeout=2)

            # Deadlock detection
            if t_fetch.is_alive() or t_write.is_alive():
                stop_event.set()
                raise RuntimeError(f"Deadlock detected in shard {shard_index}")

            # DB / writer errors
            if errors:
                raise errors[0]

            # Validate data presence
            if not os.path.exists(temp_path) or os.path.getsize(temp_path) == 0:
                elapsed = time.time() - start
                self.log.warning("%s No data — skipping upload", prefix)

                # Record empty shard
                self._metrics.record_shard(shard_index, 0, 0, elapsed)

                return ShardResult(shard_index, "", 0, 0, None, elapsed)

            bytes_written = os.path.getsize(temp_path)

            # Optional MD5
            md5_hex = None
            if self.compute_md5:
                md5_hex = self.compute_md5_eff(temp_path, log_fn=self.log.info)

            elapsed = time.time() - start

            # Record shard metrics
            self._metrics.record_shard(shard_index, rows_written, bytes_written, elapsed)

            self.log.info(
                "%s Completed: rows=%d size=%.1fMB time=%.2fs %s",
                prefix,
                rows_written,
                bytes_written / 1048576,
                elapsed,
                f"[MD5 {md5_hex}]" if md5_hex else "",
            )
            # --------------------------------------------------
            # OPTIONAL PARQUET VALIDATION BEFORE UPLOAD
            # --------------------------------------------------
            if self.validate_parquet:
                ok = self._validate_parquet_schema(temp_path)
                if not ok:
                    raise ValueError(f"{prefix} Parquet validation failed for {temp_path}")
                else:
                    self.log.info(f"{prefix} Parquet validation ✓ passed")

            # Final step: upload to storage
            remote_uri = self._upload(storage_hook, temp_path, remote_path)

            return ShardResult(
                shard_index=shard_index,
                remote_uri=remote_uri,
                rows=rows_written,
                bytes=bytes_written,
                md5=md5_hex,
                elapsed_s=elapsed,
            )

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
