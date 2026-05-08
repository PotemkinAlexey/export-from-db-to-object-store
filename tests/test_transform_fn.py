"""Row-level transform_fn applied to every Arrow batch before write."""
from __future__ import annotations

import logging
import sqlite3

import pyarrow as pa
import pyarrow.parquet as pq

from airflow_export_to_object_store.metrics import ExportMetrics
from airflow_export_to_object_store.options import ParquetOptions, ShardOptions
from airflow_export_to_object_store.parquet_io import ShardWorker

LOG = logging.getLogger("test-transform-fn")


class _FakeOp:
    log = LOG


def _sqlite_factory(rows):
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.execute("CREATE TABLE t (id INTEGER, email TEXT)")
    conn.executemany("INSERT INTO t VALUES (?, ?)", rows)
    conn.commit()

    class _A:
        def __init__(self):
            self._cur = None

        def cursor(self):
            if self._cur is None:
                self._cur = conn.cursor()
            return self._cur

        def close(self):
            conn.close()

    return lambda _id: _A()


def _build(tmp_path, factory, *, transform_fn):
    metrics = ExportMetrics(_FakeOp())
    metrics.start()
    captured = {}

    def _upload(_hook, local_path, remote_path):
        captured["local"] = local_path
        captured["bytes"] = open(local_path, "rb").read()
        return f"fake://{remote_path}"

    return (
        ShardWorker(
            shard_index=0,
            sql_text="SELECT id, email FROM t",
            filename="data.parquet",
            remote_path="exports/data.parquet",
            db_hook_id="any",
            storage_hook=object(),
            tmp_dir_root=str(tmp_path),
            parquet_options=ParquetOptions(),
            shard_options=ShardOptions(max_workers=1, chunk_rows=2),
            compute_md5=False,
            validate_parquet=False,
            upload_fn=_upload,
            metrics=metrics,
            log=LOG,
            db_adapter_factory=factory,
            transform_fn=transform_fn,
        ),
        captured,
    )


def _mask_email_column(tbl: pa.Table) -> pa.Table:
    """Replace the ``email`` column with literal ``"<redacted>"``."""
    idx = tbl.schema.get_field_index("email")
    masked = pa.array(["<redacted>"] * tbl.num_rows, type=pa.string())
    return tbl.set_column(idx, "email", masked)


def test_transform_fn_modifies_batches_before_write(tmp_path):
    factory = _sqlite_factory([(1, "a@x.com"), (2, "b@y.com"), (3, "c@z.com")])
    worker, captured = _build(tmp_path, factory, transform_fn=_mask_email_column)
    worker.run()

    snap = tmp_path / "snap.parquet"
    snap.write_bytes(captured["bytes"])
    out = pq.read_table(str(snap))
    assert out.column("email").to_pylist() == ["<redacted>"] * 3
    assert out.column("id").to_pylist() == [1, 2, 3]


def test_transform_fn_failure_surfaces_clean_error(tmp_path):
    factory = _sqlite_factory([(1, "a@x.com")])

    def _broken(_tbl):
        raise ValueError("boom")

    worker, _ = _build(tmp_path, factory, transform_fn=_broken)
    with __import__("pytest").raises(RuntimeError, match="transform_fn raised"):
        worker.run()


def test_transform_fn_can_filter_to_empty(tmp_path):
    """A transform that drops all rows from a batch shouldn't crash the
    pipeline — the shard finishes with whatever did get through."""
    factory = _sqlite_factory([(i, f"e{i}") for i in range(20)])

    def _drop_all(tbl):
        return tbl.slice(0, 0)

    worker, captured = _build(tmp_path, factory, transform_fn=_drop_all)
    result = worker.run()
    # No batches survived the transform → empty shard, no upload.
    assert result.rows == 0
    assert "local" not in captured  # upload was never called
