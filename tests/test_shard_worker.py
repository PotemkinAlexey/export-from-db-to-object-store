"""End-to-end ShardWorker tests using sqlite as a real PEP-249 source."""
from __future__ import annotations

import logging
import os
import sqlite3

import pyarrow.parquet as pq
import pytest

from airflow_export_to_object_store.metrics import ExportMetrics
from airflow_export_to_object_store.options import ParquetOptions, ShardOptions
from airflow_export_to_object_store.parquet_io import ShardWorker

LOG = logging.getLogger("test-shard-worker")


class _FakeOperator:
    log = LOG


class _SqliteAdapter:
    """Adapter shaped like UniversalDbAdapter, backed by a sqlite connection."""

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn
        self._cursor = None

    def cursor(self):
        if self._cursor is None:
            self._cursor = self._conn.cursor()
        return self._cursor

    def close(self):
        if self._cursor is not None:
            self._cursor.close()
            self._cursor = None
        self._conn.close()


def _make_factory(rows):
    # ShardWorker drives the cursor from a fetch thread, so we must allow
    # cross-thread use of the sqlite connection.
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.execute("CREATE TABLE t (id INTEGER, name TEXT)")
    conn.executemany("INSERT INTO t VALUES (?, ?)", rows)
    conn.commit()
    return lambda _hook_id: _SqliteAdapter(conn)


def _build_worker(tmp_path, factory, *, validate=True, compute_md5=False, sql="SELECT id, name FROM t"):
    metrics = ExportMetrics(_FakeOperator())
    metrics.start()

    captured = {}

    def fake_upload(storage_hook, local_path, remote_path):
        captured["local_path"] = local_path
        captured["remote_path"] = remote_path
        captured["bytes"] = os.path.getsize(local_path)
        return f"fake://{remote_path}"

    worker = ShardWorker(
        shard_index=0,
        sql_text=sql,
        filename="data.parquet",
        remote_path="exports/data.parquet",
        db_hook_id="any",
        storage_hook=object(),
        tmp_dir_root=str(tmp_path),
        parquet_options=ParquetOptions(),
        shard_options=ShardOptions(max_workers=1, chunk_rows=2),
        compute_md5=compute_md5,
        validate_parquet=validate,
        upload_fn=fake_upload,
        metrics=metrics,
        log=LOG,
        db_adapter_factory=factory,
    )
    return worker, captured, metrics


def test_basic_export_writes_valid_parquet(tmp_path):
    factory = _make_factory([(i, f"name-{i}") for i in range(7)])
    worker, captured, metrics = _build_worker(tmp_path, factory)

    result = worker.run()

    assert result.shard_index == 0
    assert result.rows == 7
    assert result.bytes > 0
    assert result.remote_uri == "fake://exports/data.parquet"
    assert result.elapsed_s >= 0
    assert metrics.shards[0]["rows"] == 7

    # Local file is cleaned up by ExitStack — but during fake_upload we recorded its size.
    assert captured["bytes"] == result.bytes
    assert captured["remote_path"] == "exports/data.parquet"


def test_empty_result_skips_upload(tmp_path):
    factory = _make_factory([])
    worker, captured, metrics = _build_worker(tmp_path, factory, validate=False)

    result = worker.run()

    assert result.rows == 0
    assert result.bytes == 0
    assert result.remote_uri == ""
    assert "remote_path" not in captured  # upload was not called
    assert metrics.shards[0]["rows"] == 0


def test_md5_computed_when_requested(tmp_path):
    factory = _make_factory([(1, "a"), (2, "b")])
    worker, _captured, _metrics = _build_worker(tmp_path, factory, compute_md5=True)

    result = worker.run()

    assert result.md5 is not None
    assert len(result.md5) == 32  # hex digest


def test_parquet_is_readable_after_export(tmp_path):
    factory = _make_factory([(i, f"x{i}") for i in range(5)])
    captured_files = []

    def fake_upload(storage_hook, local_path, remote_path):
        # Snapshot the file contents before ExitStack tears down the tmp dir.
        with open(local_path, "rb") as f:
            captured_files.append((remote_path, f.read()))
        return f"fake://{remote_path}"

    worker, _captured, _metrics = _build_worker(tmp_path, factory)
    worker.upload_fn = fake_upload  # type: ignore[assignment]
    worker.run()

    snapshot_path = tmp_path / "snapshot.parquet"
    snapshot_path.write_bytes(captured_files[0][1])
    tbl = pq.read_table(str(snapshot_path))
    assert tbl.num_rows == 5
    assert tbl.column_names == ["id", "name"]


def test_validation_failure_raises(tmp_path, monkeypatch):
    factory = _make_factory([(1, "a")])
    worker, _captured, _metrics = _build_worker(tmp_path, factory, validate=True)

    monkeypatch.setattr(
        "airflow_export_to_object_store.parquet_io.validate_parquet_schema",
        lambda *_a, **_kw: False,
    )

    with pytest.raises(ValueError, match="Parquet validation failed"):
        worker.run()
