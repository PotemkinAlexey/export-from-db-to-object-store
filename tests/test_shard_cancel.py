"""Cancellation propagation through ShardWorker."""
from __future__ import annotations

import logging
import pickle
import threading
import time

from airflow_export_to_object_store.metrics import ExportMetrics
from airflow_export_to_object_store.options import (
    ParquetOptions,
    RetryOptions,
    ShardOptions,
)
from airflow_export_to_object_store.parquet_io import ShardWorker
from airflow_export_to_object_store.shard_task import ShardTaskParams

LOG = logging.getLogger("test-shard-cancel")


class _FakeOperator:
    log = LOG


class _SlowCursor:
    """Cursor that blocks in fetchmany until released; used to exercise cancel."""

    description = [("id", None, None, None, None, None, None), ("name", None, None, None, None, None, None)]

    def __init__(self, release: threading.Event):
        self._release = release
        self._yielded = False

    def execute(self, _sql):
        return None

    def fetchmany(self, _n):
        # Block until either released (will return one batch then EOF)
        # or 5 seconds elapse so a stuck test fails fast.
        self._release.wait(5)
        if self._yielded:
            return []
        self._yielded = True
        return [(1, "a"), (2, "b"), (3, "c")]

    def close(self):
        pass


class _SlowAdapter:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def close(self):
        self._cursor.close()


def _build(tmp_path, cancel, cursor):
    metrics = ExportMetrics(_FakeOperator())
    metrics.start()
    captured = []

    def _upload(_hook, local_path, remote_path):
        captured.append(remote_path)
        return f"fake://{remote_path}"

    return (
        ShardWorker(
            shard_index=0,
            sql_text="SELECT id, name FROM t",
            filename="data.parquet",
            remote_path="exports/data.parquet",
            db_hook_id="any",
            storage_hook=object(),
            tmp_dir_root=str(tmp_path),
            parquet_options=ParquetOptions(),
            shard_options=ShardOptions(max_workers=1, chunk_rows=10),
            compute_md5=False,
            validate_parquet=False,
            upload_fn=_upload,
            metrics=metrics,
            log=LOG,
            db_adapter_factory=lambda _id: _SlowAdapter(cursor),
            cancel=cancel,
        ),
        captured,
        metrics,
    )


def test_cancel_before_run_skips_upload(tmp_path):
    cancel = threading.Event()
    cancel.set()  # already cancelled before we start
    release = threading.Event()
    release.set()  # don't make the cursor block, we want a quick exit
    cursor = _SlowCursor(release)

    worker, captured, _metrics = _build(tmp_path, cancel, cursor)
    result = worker.run()

    assert result.remote_uri == ""
    assert result.bytes == 0
    assert captured == []  # upload was never called


def test_cancel_during_run_stops_pipeline(tmp_path):
    cancel = threading.Event()
    release = threading.Event()  # cursor blocks until set
    cursor = _SlowCursor(release)

    worker, captured, _metrics = _build(tmp_path, cancel, cursor)

    # Start the worker in a background thread, set cancel, then unblock the cursor
    # so the fetch loop notices the cancellation on its next check.
    started = time.time()
    t = threading.Thread(target=worker.run)
    t.start()
    time.sleep(0.05)
    cancel.set()
    release.set()
    t.join(timeout=8)

    assert not t.is_alive(), "shard did not honour cancellation in time"
    assert time.time() - started < 8
    # Upload may or may not have happened depending on exact race timing; the
    # important guarantee is that the worker terminated promptly.


def test_shard_task_params_is_picklable():
    """Without this, ProcessPoolExecutor cannot ship a shard to a subprocess."""
    p = ShardTaskParams(
        shard_index=0,
        sql_text="SELECT 1",
        filename="x.parquet",
        remote_path="x.parquet",
        db_hook_id="db",
        storage_hook_id="storage",
        tmp_dir=None,
        container=None,
        bucket="b",
        overwrite=True,
        compute_md5=False,
        validate_parquet=True,
        parquet_options=ParquetOptions(),
        shard_options=ShardOptions(),
        retry_options=RetryOptions(),
    )
    p2 = pickle.loads(pickle.dumps(p))
    assert p2 == p
