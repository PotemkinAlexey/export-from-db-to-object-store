"""Per-shard fetch → Arrow → Parquet pipeline.

The :class:`ShardWorker` owns three threads:

* **fetch** — pulls rows from a DB cursor (Arrow-native if available, else
  DB-API + fast ``zip(*rows)`` conversion) and pushes :class:`pyarrow.Table`
  batches onto a bounded queue.
* **write** — drains the queue, opens a :class:`pyarrow.parquet.ParquetWriter`
  on the first batch and aligns subsequent batches' schemas to it.
* **heartbeat** — emits a periodic INFO log with rows-written and on-disk size.

It is intentionally decoupled from the Airflow operator: callers inject an
``upload`` callable, the storage hook, the DB adapter factory, options and a
logger.
"""
from __future__ import annotations

import logging
import os
import queue
import shutil
import tempfile
import threading
import time
from collections.abc import Callable
from contextlib import ExitStack, suppress
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from .db_adapter import UniversalDbAdapter
from .metrics import ExportMetrics
from .options import ParquetOptions, ShardOptions, ShardResult
from .parquet_validator import validate_parquet_schema
from .utils import coerce_ts_table, compute_md5_eff

UploadFn = Callable[[Any, str, str], str]
"""(storage_hook, local_path, remote_path) -> remote URI"""

DbAdapterFactory = Callable[[str], Any]
"""(db_hook_id) -> object exposing cursor() and close()."""


def _default_adapter_factory(db_hook_id: str) -> Any:
    return UniversalDbAdapter(db_hook_id)


class ShardWorker:
    """Run a single shard end-to-end. Stateless across runs; one ``run()`` per instance."""

    def __init__(
        self,
        *,
        shard_index: int,
        sql_text: str,
        filename: str,
        remote_path: str,
        db_hook_id: str,
        storage_hook: Any,
        tmp_dir_root: str | None,
        parquet_options: ParquetOptions,
        shard_options: ShardOptions,
        compute_md5: bool,
        validate_parquet: bool,
        upload_fn: UploadFn,
        metrics: ExportMetrics,
        log: logging.Logger,
        db_adapter_factory: DbAdapterFactory = _default_adapter_factory,
        cancel: threading.Event | None = None,
    ) -> None:
        self.shard_index = shard_index
        self.sql_text = sql_text
        self.filename = filename
        self.remote_path = remote_path
        self.db_hook_id = db_hook_id
        self.storage_hook = storage_hook
        self.tmp_dir_root = tmp_dir_root
        self.parquet_options = parquet_options
        self.shard_options = shard_options
        self.compute_md5 = compute_md5
        self.validate_parquet = validate_parquet
        self.upload_fn = upload_fn
        self.metrics = metrics
        self.log = log
        self._adapter_factory = db_adapter_factory

        self._prefix = f"[Shard {shard_index}]"
        self._queue: queue.Queue[pa.Table | None] = queue.Queue(maxsize=2)
        self._stop = threading.Event()
        # External cancellation signal shared across shards (set by the operator
        # when any other shard fails). The shard treats it like its own _stop.
        self._cancel = cancel
        self._errors: list[Exception] = []
        self._rows_lock = threading.Lock()
        self._rows_written = 0
        self._temp_path: str = ""

    def _should_stop(self) -> bool:
        """True when this shard's local stop or the external cancel was raised."""
        return self._stop.is_set() or (self._cancel is not None and self._cancel.is_set())

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------
    def run(self) -> ShardResult:
        start = time.time()
        tmp_dir = tempfile.mkdtemp(dir=self.tmp_dir_root)
        self._temp_path = os.path.join(tmp_dir, self.filename)

        with ExitStack() as stack:
            stack.callback(lambda: shutil.rmtree(tmp_dir, ignore_errors=True))

            adapter = self._adapter_factory(self.db_hook_id)
            cursor = adapter.cursor()
            stack.callback(adapter.close)

            per_shard_mem = self._memory_budget()

            t_hb = threading.Thread(target=self._heartbeat, daemon=True, name=f"heartbeat-{self.shard_index}")
            t_fetch = threading.Thread(
                target=self._fetch, args=(cursor, per_shard_mem), daemon=True, name=f"fetch-{self.shard_index}"
            )
            t_write = threading.Thread(target=self._write, daemon=True, name=f"write-{self.shard_index}")

            t_hb.start()
            t_fetch.start()
            t_write.start()
            t_fetch.join()
            t_write.join()
            self._stop.set()
            t_hb.join(timeout=2)

            if t_fetch.is_alive() or t_write.is_alive():
                raise RuntimeError(f"Deadlock detected in shard {self.shard_index}")

            if self._errors:
                raise self._errors[0]

            # If the operator cancelled this shard mid-flight, exit early without
            # validating/uploading a partial file.
            if self._cancel is not None and self._cancel.is_set():
                elapsed = time.time() - start
                self.log.info("%s Cancelled — skipping upload", self._prefix)
                self.metrics.record_shard(self.shard_index, self._rows_written, 0, elapsed)
                return ShardResult(self.shard_index, "", self._rows_written, 0, None, elapsed)

            return self._finalize(start)

    # ------------------------------------------------------------------
    # Memory planning
    # ------------------------------------------------------------------
    def _memory_budget(self) -> float:
        total = self.shard_options.memory_limit_mb
        workers = max(1, self.shard_options.max_workers)
        per_shard = max((total * 0.80) / workers, 128)
        self.log.info(
            "%s Memory budget: total=%dMB workers=%d → per_shard=%.1fMB",
            self._prefix,
            total,
            workers,
            per_shard,
        )
        return per_shard

    # ------------------------------------------------------------------
    # Threads
    # ------------------------------------------------------------------
    def _heartbeat(self) -> None:
        hb_interval = 30
        last_log = time.time()
        while not self._should_stop():
            now = time.time()
            if now - last_log >= hb_interval:
                size_mb = (
                    os.path.getsize(self._temp_path) / 1024 / 1024 if os.path.exists(self._temp_path) else 0.0
                )
                with self._rows_lock:
                    current = self._rows_written
                self.log.info("%s Heartbeat: %d rows written (%.1fMB)", self._prefix, current, size_mb)
                last_log = now
            self._stop.wait(1)

    def _fetch(self, cursor: Any, per_shard_mem_mb: float) -> None:
        try:
            cursor.execute(self.sql_text)
            chunk_rows = self.shard_options.chunk_rows
            tuned = False
            arrow_native = hasattr(cursor, "fetchmany_arrow")

            while not self._should_stop():
                tbl = self._fetch_one_batch(cursor, chunk_rows, arrow_native)
                if tbl is None or tbl.num_rows == 0:
                    break

                tbl = self._decode_dictionary(tbl)

                if not tuned:
                    chunk_rows = self._auto_tune_chunk(tbl, chunk_rows, per_shard_mem_mb)
                    tuned = True

                try:
                    self._queue.put(tbl, timeout=2)
                except queue.Full:
                    if self._should_stop():
                        break
                    chunk_rows = max(1000, int(chunk_rows * 0.7))
                    continue

            self._safe_put_sentinel()

        except Exception as e:
            self._errors.append(e)
            self._stop.set()
            self._drain_queue()
            self._safe_put_sentinel()

    def _write(self) -> None:
        writer: pq.ParquetWriter | None = None
        try:
            while True:
                try:
                    tbl = self._queue.get(timeout=5)
                except queue.Empty:
                    if self._should_stop():
                        break
                    continue

                if tbl is None:
                    self._queue.task_done()
                    break

                if writer is None:
                    writer = pq.ParquetWriter(
                        self._temp_path,
                        tbl.schema,
                        compression=self.parquet_options.compression,
                        write_statistics=self.parquet_options.write_statistics,
                        use_dictionary=self.parquet_options.use_dictionary,
                    )

                tbl = coerce_ts_table(tbl, self.parquet_options.coerce_timestamps or "ms")
                tbl = self._align_schema(tbl, writer.schema)
                writer.write_table(tbl, row_group_size=self.parquet_options.row_group_size)

                with self._rows_lock:
                    self._rows_written += tbl.num_rows
                self._queue.task_done()

        except Exception as e:
            self._errors.append(e)
            self._stop.set()
        finally:
            if writer is not None:
                writer.close()

    # ------------------------------------------------------------------
    # Fetch helpers
    # ------------------------------------------------------------------
    def _fetch_one_batch(self, cursor: Any, chunk_rows: int, arrow_native: bool) -> pa.Table | None:
        if arrow_native:
            return cursor.fetchmany_arrow(chunk_rows)

        rows = cursor.fetchmany(chunk_rows)
        if not rows:
            return None
        if not cursor.description:
            raise RuntimeError("Cursor returned no description")

        columns = [d[0] for d in cursor.description]
        try:
            cols_data = list(zip(*rows))
            if len(cols_data) != len(columns):
                raise ValueError(f"Column mismatch: got {len(cols_data)}, expected {len(columns)}")
            arrays = [pa.array(col, from_pandas=False) for col in cols_data]
        except Exception as e:
            self.log.warning("%s Fast Arrow conversion failed (%s), fallback mode", self._prefix, e)
            arrays = []
            for col_idx, _ in enumerate(columns):
                arrays.append(pa.array([row[col_idx] for row in rows]))
        return pa.Table.from_arrays(arrays, names=columns)

    def _decode_dictionary(self, tbl: pa.Table) -> pa.Table:
        try:
            decoded = []
            for name in tbl.schema.names:
                col = tbl[name]
                if pa.types.is_dictionary(col.type):
                    col = col.dictionary_decode()
                decoded.append(col)
            return pa.table(decoded, names=tbl.schema.names)
        except Exception as e:
            self.log.warning("%s Dict decode failed: %s", self._prefix, e)
            return tbl

    def _auto_tune_chunk(self, tbl: pa.Table, current_chunk: int, per_shard_mem_mb: float) -> int:
        try:
            total_bytes = tbl.get_total_size() if hasattr(tbl, "get_total_size") else tbl.nbytes
            avg_row = max(total_bytes / max(tbl.num_rows, 1), 32)
            target_batch_bytes = int(per_shard_mem_mb * 1024 * 1024) * 0.25
            new_chunk = max(10_000, min(int(target_batch_bytes / avg_row), 500_000))
            self.log.info(
                "%s Auto-tuned chunk_rows=%d (avg_row=%.1fB, est_batch=%.1fMB, mem_limit=%.1fMB)",
                self._prefix,
                new_chunk,
                avg_row,
                (avg_row * new_chunk) / 1048576,
                per_shard_mem_mb,
            )
            return new_chunk
        except Exception as e:
            self.log.warning("%s Auto-tuning failed: %s", self._prefix, e)
            return current_chunk

    def _safe_put_sentinel(self) -> None:
        with suppress(Exception):
            self._queue.put_nowait(None)

    def _drain_queue(self) -> None:
        while True:
            try:
                self._queue.get_nowait()
            except Exception:
                break

    # ------------------------------------------------------------------
    # Write helpers
    # ------------------------------------------------------------------
    def _align_schema(self, tbl: pa.Table, target: pa.Schema) -> pa.Table:
        if tbl.schema.equals(target):
            return tbl
        try:
            return tbl.cast(target)
        except Exception:
            self.log.warning("%s Schema mismatch — padding null columns", self._prefix)
            return pa.Table.from_arrays(
                [
                    tbl.column(n) if n in tbl.schema.names else pa.nulls(tbl.num_rows)
                    for n in target.names
                ],
                names=target.names,
            )

    # ------------------------------------------------------------------
    # Finalize: validate, MD5, upload, build result
    # ------------------------------------------------------------------
    def _finalize(self, start: float) -> ShardResult:
        if not os.path.exists(self._temp_path) or os.path.getsize(self._temp_path) == 0:
            elapsed = time.time() - start
            self.log.warning("%s No data — skipping upload", self._prefix)
            self.metrics.record_shard(self.shard_index, 0, 0, elapsed)
            return ShardResult(self.shard_index, "", 0, 0, None, elapsed)

        bytes_written = os.path.getsize(self._temp_path)

        md5_hex: str | None = None
        if self.compute_md5:
            md5_hex = compute_md5_eff(self._temp_path, log_fn=self.log.info)

        elapsed = time.time() - start
        rows = self._rows_written
        self.metrics.record_shard(self.shard_index, rows, bytes_written, elapsed)

        self.log.info(
            "%s Completed: rows=%d size=%.1fMB time=%.2fs %s",
            self._prefix,
            rows,
            bytes_written / 1048576,
            elapsed,
            f"[MD5 {md5_hex}]" if md5_hex else "",
        )

        if self.validate_parquet:
            if not validate_parquet_schema(self._temp_path, self.log):
                raise ValueError(f"{self._prefix} Parquet validation failed for {self._temp_path}")
            self.log.info("%s Parquet validation ✓ passed", self._prefix)

        remote_uri = self.upload_fn(self.storage_hook, self._temp_path, self.remote_path)

        return ShardResult(
            shard_index=self.shard_index,
            remote_uri=remote_uri,
            rows=rows,
            bytes=bytes_written,
            md5=md5_hex,
            elapsed_s=elapsed,
        )
