"""Per-shard timeout: a slow cursor must not run forever.

Real DB drivers either honour socket-level timeouts and return errors
when the deadline hits, or yield rows in small batches between which
the fetch loop polls the stop event. Either way the watchdog catches
them within ``timeout`` seconds of firing. The test models the
batched-yield case (the more common one) — between batches the fetch
loop calls :meth:`ShardWorker._should_stop`, sees the timer-set stop
event, exits, and ``run()`` raises ``TimeoutError``.

If a driver is genuinely stuck inside a single uninterruptible C call
(``time.sleep(60)`` from Python, or a poorly-written native code that
ignores signals), neither this watchdog nor any thread-based scheme
can preempt it — the user's escape hatch is Airflow's own
``execution_timeout`` which kills the task process.
"""

from __future__ import annotations

import logging
import sqlite3
import time

import pytest

from airflow_export_to_object_store.metrics import ExportMetrics
from airflow_export_to_object_store.options import ParquetOptions, ShardOptions
from airflow_export_to_object_store.parquet_io import ShardWorker

LOG = logging.getLogger("test-shard-timeout")


class _FakeOp:
    log = LOG


class _SlowCursor:
    """Yields one row every 50 ms forever (until the loop is told to stop).

    Models a DB that's actively producing data but slowly, so the fetch
    loop polls ``_should_stop`` between batches.
    """

    description = [("id", None, None, None, None, None, None)]

    def __init__(self):
        self.calls = 0

    def execute(self, _sql):
        return None

    def fetchmany(self, _n):
        time.sleep(0.05)
        self.calls += 1
        return [(self.calls,)]

    def close(self):
        pass


class _SlowAdapter:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def close(self):
        self._cursor.close()


def test_timeout_aborts_stuck_shard(tmp_path):
    metrics = ExportMetrics(_FakeOp())
    metrics.start()

    cursor = _SlowCursor()
    worker = ShardWorker(
        shard_index=0,
        sql_text="SELECT 1",
        filename="f.parquet",
        remote_path="f.parquet",
        db_hook_id="any",
        storage_hook=object(),
        tmp_dir_root=str(tmp_path),
        parquet_options=ParquetOptions(),
        shard_options=ShardOptions(timeout=0.5, max_workers=1, chunk_rows=10),
        compute_md5=False,
        validate_parquet=False,
        upload_fn=lambda *_a: "fake://x",
        metrics=metrics,
        log=LOG,
        db_adapter_factory=lambda _id: _SlowAdapter(cursor),
    )

    started = time.time()
    with pytest.raises(TimeoutError, match="timeout"):
        worker.run()
    elapsed = time.time() - started
    # Timer at 0.5 s + one batch (50 ms) + thread join overhead.
    assert elapsed < 5.0, f"shard took {elapsed:.2f}s to honour timeout"
    # We must have spent at least the timeout — otherwise the test wasn't
    # really exercising the watchdog path.
    assert elapsed >= 0.5


def test_no_timeout_leaves_normal_shard_alone(tmp_path):
    """ShardOptions.timeout=None must not interfere with a healthy shard."""
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.execute("CREATE TABLE t (x INTEGER)")
    conn.executemany("INSERT INTO t VALUES (?)", [(i,) for i in range(5)])
    conn.commit()

    class _A:
        def __init__(self, c):
            self._c = c
            self._cur = None

        def cursor(self):
            if self._cur is None:
                self._cur = self._c.cursor()
            return self._cur

        def close(self):
            self._c.close()

    metrics = ExportMetrics(_FakeOp())
    metrics.start()

    worker = ShardWorker(
        shard_index=0,
        sql_text="SELECT x FROM t",
        filename="f.parquet",
        remote_path="f.parquet",
        db_hook_id="any",
        storage_hook=object(),
        tmp_dir_root=str(tmp_path),
        parquet_options=ParquetOptions(),
        shard_options=ShardOptions(timeout=None, max_workers=1, chunk_rows=10),
        compute_md5=False,
        validate_parquet=False,
        upload_fn=lambda *_a: "fake://x",
        metrics=metrics,
        log=LOG,
        db_adapter_factory=lambda _id: _A(conn),
    )

    result = worker.run()
    assert result.rows == 5
