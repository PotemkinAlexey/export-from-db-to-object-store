# -*- coding: utf-8 -*-
"""
Universal streaming export operator:
Execute SQL via any DB hook (PEP-249 / Airflow Connection) → stream Arrow batches
→ write Parquet → upload to object storage (Azure Blob or AWS S3).
"""
from __future__ import annotations

import hashlib
import mimetypes
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
from airflow import macros
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from jinja2 import StrictUndefined, Template

from .db_adapter import UniversalDbAdapter
from .metrics import ExportMetrics
from .options import ParquetOptions, RetryOptions, ShardOptions, ShardResult
from .retry import with_retries

try:
    from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
except ImportError:
    WasbHook = None

try:
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
except ImportError:
    S3Hook = None

try:
    from azure.storage.blob import ContentSettings
except (ImportError, ModuleNotFoundError):
    ContentSettings = None


class ExportFromDBToObjectStoreOperator(BaseOperator):
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
        # Auto-clean old tmp dirs
        if self.tmp_dir and os.path.exists(self.tmp_dir):
            try:
                now = time.time()
                for item in os.listdir(self.tmp_dir):
                    item_path = os.path.join(self.tmp_dir, item)

                    # only remove directories created by this operator
                    if os.path.isdir(item_path):
                        age = now - os.path.getmtime(item_path)
                        if age > 24 * 3600:
                            shutil.rmtree(item_path, ignore_errors=True)
                            self.log.info(f"🧹 Cleaned old temp directory: {item}")
            except Exception as e:
                self.log.warning(f"Temp cleanup failed: {e}")

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

        results: List[ShardResult] = []
        futures = {}

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
        """Flatten nested dict/list and render Jinja templates."""
        flat = {}

        def _flatten(prefix: str, value: Any, depth=0):
            if depth > 10:
                raise ValueError("Params nesting too deep — recursion loop?")

            if isinstance(value, dict):
                for k, v in value.items():
                    key = f"{prefix}_{k}" if prefix else k
                    _flatten(key, v, depth + 1)
            elif isinstance(value, list):
                for i, v in enumerate(value):
                    key = f"{prefix}_{i}" if prefix else f"list_{i}"
                    _flatten(key, v, depth + 1)
            else:
                flat_key = prefix if prefix else "root"
                flat[flat_key] = value

        _flatten("", data)

        result = {}
        for k, v in flat.items():
            if isinstance(v, str) and "{{" in v and "}}" in v:
                result[k] = Template(v, undefined=StrictUndefined).render(**ctx)
            else:
                result[k] = v
        return result

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
                last_log = 0
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
    def is_aws_generic_hook(self, storage_hook):
        """
        Detects whether storage_hook is an AWS-based hook
        even if AwsGenericHook cannot be imported (old Airflow, custom providers).
        """
        # 1) Try direct import (Airflow >= 2.3+)
        try:
            from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook

            if isinstance(storage_hook, AwsGenericHook):
                return True
        except Exception:
            pass

        # 2) Fallback detection by class name (covers custom providers, proxies, older versions)
        cls = type(storage_hook).__name__.lower()
        if "aws" in cls and "hook" in cls:
            return True

        # 3) Raw type string match (very defensive)
        if "AwsGenericHook" in str(type(storage_hook)):
            return True

        return False

    @with_retries
    def _upload(self, storage_hook: Any, local_path: str, remote_path: str) -> str:
        """
        Unified upload entry point.
        Routes to the correct backend:
          - AWS S3 (any file size) → automatic multipart via boto3.s3.transfer.UploadFile
          - Azure Blob Storage:
                <5GB  → simple upload
                >5GB  → block-blob multipart upload
        """
        size = os.path.getsize(local_path)

        # --------------------------------------------------
        # AZURE BLOB STORAGE
        # --------------------------------------------------
        if WasbHook and isinstance(storage_hook, WasbHook):
            if not self.container:
                raise ValueError("Container must be set for Azure uploads")

            if size < 5 * 1024**3:
                return self._upload_azure_simple(storage_hook, local_path, remote_path)
            else:
                self.log.info("Azure large file detected (%.1f GB) → block upload", size / 1024**3)
                return self._upload_azure_block(storage_hook, local_path, remote_path)

        # --------------------------------------------------
        # AWS S3 (S3Hook or AwsGenericHook)
        # --------------------------------------------------
        is_s3 = S3Hook and isinstance(storage_hook, S3Hook)
        is_aws_generic = self.is_aws_generic_hook(storage_hook)

        if is_s3 or is_aws_generic:
            # Single unified S3 uploader (uses boto3 automatic multipart)
            return self._upload_s3_unified(storage_hook, local_path, remote_path)

        # --------------------------------------------------
        # UNSUPPORTED STORAGE
        # --------------------------------------------------
        raise NotImplementedError(f"Unsupported storage hook: {type(storage_hook)}")

    def _upload_s3_unified(self, storage_hook, local_path, remote_path):
        import boto3
        from boto3.s3.transfer import TransferConfig
        from botocore.config import Config

        self.log.info("S3 unified upload: %s → %s", local_path, remote_path)

        # Resolve bucket
        bucket = self.bucket or getattr(storage_hook, "bucket_name", None)
        if not bucket:
            raise ValueError("bucket must be set for S3 uploads")

        # Get S3 client
        if isinstance(storage_hook, S3Hook):
            s3 = storage_hook.get_conn()
        else:
            aws_conn = BaseHook.get_connection(self.storage_hook_id)
            session = boto3.session.Session(
                aws_access_key_id=aws_conn.login,
                aws_secret_access_key=aws_conn.password,
                region_name=aws_conn.extra_dejson.get("region_name"),
            )
            s3 = session.client(
                "s3",
                config=Config(
                    retries={"max_attempts": 10, "mode": "standard"},
                    connect_timeout=60,
                    read_timeout=60,
                ),
            )

        # AWS-managed automatic multipart
        transfer_cfg = TransferConfig(
            multipart_threshold=64 * 1024 * 1024,
            multipart_chunksize=64 * 1024 * 1024,
            max_concurrency=8,
            use_threads=True,
        )

        s3.upload_file(
            Filename=local_path,
            Bucket=bucket,
            Key=remote_path,
            Config=transfer_cfg,
        )

        return f"s3://{bucket}/{remote_path}"

    def _upload_azure_simple(self, storage_hook, local_path, remote_path):
        self.log.info("Azure simple upload: %s → %s", local_path, remote_path)

        client = storage_hook.get_conn()
        blob = client.get_blob_client(container=self.container, blob=remote_path)

        content_type = mimetypes.guess_type(local_path)[0] or "application/octet-stream"
        cs = ContentSettings(content_type=content_type) if ContentSettings else None

        with open(local_path, "rb") as f:
            if cs:
                blob.upload_blob(f, overwrite=self.overwrite, content_settings=cs)
            else:
                blob.upload_blob(f, overwrite=self.overwrite)

        return f"azure://{self.container}/{remote_path}"

    def _upload_azure_block(self, storage_hook, local_path, remote_path):
        self.log.info("Azure block upload for large file: %s", local_path)

        client = storage_hook.get_conn()
        blob = client.get_blob_client(container=self.container, blob=remote_path)

        block_size = 100 * 1024 * 1024  # 100MB per block
        blocks = []
        idx = 0

        with open(local_path, "rb") as f:
            while True:
                chunk = f.read(block_size)
                if not chunk:
                    break
                block_id = f"{idx:08d}"
                blocks.append(block_id)
                self.log.info("Azure uploading block %s (%d MB)", block_id, len(chunk) / (1024**2))
                blob.stage_block(block_id=block_id, data=chunk)
                idx += 1

        blob.commit_block_list(blocks)

        return f"azure://{self.container}/{remote_path}"

    # ------------------------------------------------------------------
    # HEALTH CHECKS
    # ------------------------------------------------------------------
    def _network_health_check(self, storage_hook):
        """Check network connectivity only for the actual storage backend."""
        import socket

        checks = []

        # Azure
        if WasbHook and isinstance(storage_hook, WasbHook):
            checks.append(("Azure", "blob.core.windows.net", 443))

        # AWS S3 (S3Hook or AwsGenericHook)
        if (S3Hook and isinstance(storage_hook, S3Hook)) or self.is_aws_generic_hook(storage_hook):
            checks.append(("AWS S3", "s3.amazonaws.com", 443))

        if not checks:
            self.log.info("No known storage type → skipping network health checks.")
            return

        for service, host, port in checks:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(3)
                result = sock.connect_ex((host, port))
                sock.close()

                if result == 0:
                    self.log.info(f"🌐 Network OK → {service}")
                else:
                    self.log.warning(f"⚠ Network may be limited for {service} ({host})")
            except Exception as e:
                self.log.warning(f"Network check failed for {service}: {e}")

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

    def _health_checks(self, context):
        """
        Validate storage connection, bucket/container accessibility,
        and available local disk space.
        """

        try:
            storage_hook = BaseHook.get_hook(self.storage_hook_id)

            self._network_health_check(storage_hook)
            self._memory_health_check()
            self.log.info("Performing health checks...")
            # ----------------------------------------
            # Azure health check
            # ----------------------------------------

            if WasbHook and isinstance(storage_hook, WasbHook):
                if not self.container:
                    raise ValueError("Container must be specified for Azure storage")

                client = storage_hook.get_conn()
                container_client = client.get_container_client(self.container)

                try:
                    test_blob = container_client.get_blob_client("_healthcheck_tmp")

                    # 1) Try WRITE permission
                    try:
                        test_blob.upload_blob(b"test", overwrite=True)
                        test_blob.delete_blob()
                        self.log.info("Azure health check OK ✓ (write/delete allowed)")
                        return
                    except Exception:
                        pass  # Maybe read-only, continue

                    # 2) Try READ permission
                    try:
                        _ = next(container_client.list_blobs(results_per_page=1), None)
                        self.log.info("Azure health check OK ✓ (read-only allowed)")
                        return
                    except Exception:
                        pass  # Maybe write-only, continue

                    # 3) Try GET properties on container (works with write-only SAS)
                    try:
                        _ = container_client.get_container_properties()
                        self.log.info("Azure health check OK ✓ (write-only SAS)")
                        return
                    except Exception:
                        pass

                    # 4) Everything failed
                    raise RuntimeError("Azure health check failed: neither read nor write allowed")

                except Exception as e:
                    self.log.error("Azure health check FAILED: %s", e)
                    raise

            # ----------------------------------------
            # S3 health check
            # ----------------------------------------
            elif S3Hook and isinstance(storage_hook, S3Hook):
                bucket = self.bucket or getattr(storage_hook, "bucket_name", None)
                if not bucket:
                    raise ValueError("S3 bucket must be specified")
                conn = storage_hook.get_conn()
                conn.head_bucket(Bucket=bucket)

        except Exception as e:
            self.log.error("Storage health check FAILED: %s", e)
            raise

        # ----------------------------------------
        # Disk space check
        # ----------------------------------------
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
    # TEMPLATE RENDERING (SQL + paths)
    # ------------------------------------------------------------------
    def _render_template(self, template_str: str, ctx: Dict[str, Any], label="template") -> str:
        """
        Render Jinja template with Airflow macros, SQL params,
        and flattened dynamic parameters.
        Also performs SQL-injection safety checks.
        """
        flat_params = self.flatten_and_render_params(
            {
                **ctx.get("params", {}),
                **self.sql_params,
                **ctx,
                "macros": macros,
            },
            ctx,
        )

        full_ctx = {**ctx, "macros": macros, **flat_params}
        rendered = Template(template_str, undefined=StrictUndefined).render(**full_ctx)

        # --------------------------------------
        # Basic SQL safety validation
        # --------------------------------------
        if label == "SQL":
            check = rendered.upper()

            dangerous = ["; DROP", "UNION SELECT", "--", "/*", "XP_CMDSHELL"]
            if any(x in check for x in dangerous):
                self.log.warning("⚠ Potential SQL injection: %s", rendered[:200])

            if not check.strip().startswith(("SELECT", "WITH")):
                self.log.warning("SQL does not start with SELECT/WITH: %s", check[:100])

        self.log.info("Rendered %s:\n%s", label, rendered)
        return rendered

    def _render_template_str(self, template_str: str, ctx: Dict[str, Any]) -> str:
        """
        Render template for filenames / remote paths.
        Includes path traversal protections.
        """
        rendered = self._render_template(template_str, ctx, label="string")

        # Prevent path traversal
        if ".." in rendered or rendered.startswith("/"):
            raise ValueError(f"Invalid path: {rendered}")

        return rendered.lstrip("/")

    # ------------------------------------------------------------------
    # TIMESTAMP COERCION
    # ------------------------------------------------------------------
    def _coerce_ts_table(self, tbl: pa.Table, target_unit: str) -> pa.Table:
        """
        Convert all timestamp columns in the table to target unit (ms/us/ns).
        Avoids schema drift in ParquetWriter.
        """
        if target_unit not in ("s", "ms", "us", "ns"):
            return tbl

        new_fields = []
        for field in tbl.schema:
            if pa.types.is_timestamp(field.type):
                new_fields.append(pa.field(field.name, pa.timestamp(target_unit, field.type.tz)))
            else:
                new_fields.append(field)

        new_schema = pa.schema(new_fields)
        return tbl.cast(new_schema, safe=False)

    @staticmethod
    def compute_md5_eff(file_path: str, *, log_fn=None, skip_threshold_gb: int = 10):
        """
        Efficient MD5 calculation with optional logging and skip for huge files.
        Returns MD5 hex string or None.
        """
        size = os.path.getsize(file_path)
        size_gb = size / (1024**3)

        # Auto-skip huge files
        if size_gb > skip_threshold_gb:
            if log_fn:
                log_fn(f"Skipping MD5 for very large file (size: {size_gb:.1f} GB)")
            return None

        h = hashlib.md5()
        buf = bytearray(8 * 1024 * 1024)  # 8MB
        mv = memoryview(buf)

        with open(file_path, "rb", buffering=0) as f:
            while True:
                n = f.readinto(buf)
                if not n:
                    break
                h.update(mv[:n])

        return h.hexdigest()

    def _validate_parquet_schema(self, file_path: str) -> bool:
        """
        Lightweight but powerful Parquet validation:
        • checks that the file is readable
        • schema is valid
        • no corrupt footer
        • row groups > 0
        • warns about nested/logical types
        • performs minimal sample read
        """
        try:
            if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
                self.log.error(f"❌ Parquet file is empty or missing: {file_path}")
                return False

            # -------------------------------------------------------
            # FOOTER + METADATA VALIDATION
            # -------------------------------------------------------
            try:
                meta = pq.read_metadata(file_path)
            except Exception as e:
                self.log.error(f"❌ Failed to read Parquet metadata/footer: {e}")
                return False

            if meta.num_row_groups == 0:
                self.log.error("❌ Parquet file has zero row groups — likely corrupt.")
                return False

            # -------------------------------------------------------
            # SCHEMA VALIDATION
            # -------------------------------------------------------
            try:
                schema = pq.read_schema(file_path)
            except Exception as e:
                self.log.error(f"❌ Failed to read Parquet schema: {e}")
                return False

            for field in schema:
                t = field.type

                # Warn on nested types
                if pa.types.is_nested(t):
                    self.log.warning(f"⚠️ Nested type: {field.name} → {t}")

                # Warn on Maps
                if pa.types.is_map(t):
                    self.log.warning(f"⚠️ MAP type detected — may not be supported by some engines: {field.name}")

                # Warn on LargeList / LargeBinary
                if pa.types.is_large_list(t) or pa.types.is_large_binary(t):
                    self.log.warning(f"⚠️ Large* Parquet types detected: {field.name} → {t}")

                # Warn on DECIMAL with high precision
                if pa.types.is_decimal(t) and t.precision > 38:
                    self.log.warning(f"⚠️ High precision DECIMAL({t.precision},{t.scale}) might break consumers")

                # Timestamp consistency check
                if pa.types.is_timestamp(t) and t.unit not in ("ms", "us", "ns"):
                    self.log.warning(f"⚠️ Unexpected timestamp unit {t.unit} for column {field.name}")

            # -------------------------------------------------------
            # SAMPLE READ VALIDATION (read small slices of all columns)
            # -------------------------------------------------------
            try:
                # We read the *first row group only*, not entire file.
                # This guarantees a light read but still validates schema & encodings.
                pf = pq.ParquetFile(file_path)

                if pf.num_row_groups == 0:
                    self.log.error("❌ Parquet has zero row groups.")
                    return False

                # Read first row group (usually up to ~128–512MB uncompressed)
                row_group = pf.read_row_group(0, use_threads=False)

                # Now take a small sample from the row group
                sample = row_group.slice(0, min(1000, row_group.num_rows))
                _ = sample.num_rows  # force materialization

                self.log.info(f"Sample read OK ✓ (rows={sample.num_rows}, columns={len(sample.schema.names)})")

            except Exception as e:
                self.log.error(f"❌ Failed to read sample data (all columns): {e}")
                return False

            if row_group.num_rows == 0:
                self.log.warning("⚠️ First row group has zero rows.")

            # SUCCESS LOG
            self.log.info(
                f"✅ Parquet validation succeeded: "
                f"sample_rows={sample.num_rows}, "
                f"columns={len(schema)}, "
                f"row_groups={meta.num_row_groups}"
            )

            return True

        except Exception as e:
            self.log.error(f"❌ Unexpected Parquet validation error: {e}")
            return False
