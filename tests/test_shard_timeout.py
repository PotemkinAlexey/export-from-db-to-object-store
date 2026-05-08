"""Per-shard timeout: a slow cursor must not run forever."""

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


class _StuckCursor:
    """A cursor whose fetchmany blocks much longer than the test timeout."""

    description = [("id", None, None, None, None, None, None)]

    def execute(self, _sql):
        return None

    def fetchmany(self, _n):
        time.sleep(60)
        return []

    def close(self):
        pass


class _StuckAdapter:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def close(self):
        self._cursor.close()


def test_timeout_aborts_stuck_shard(tmp_path):
    metrics = ExportMetrics(_FakeOp())
    metrics.start()

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
        db_adapter_factory=lambda _id: _StuckAdapter(_StuckCursor()),
    )

    started = time.time()
    with pytest.raises(TimeoutError, match="timeout"):
        worker.run()
    # ShardWorker's timer fires at 0.5s; threads notice on next stop poll.
    assert time.time() - started < 5.0


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
