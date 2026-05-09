"""End-to-end tests for ``StreamingExportOperator.execute``.

These exercise the orchestration layer that the per-component tests
don't reach: the full health-checks → render → run_shards → manifest →
watermark-commit pipeline, with sqlite as the source DB and a
filesystem-backed fake uploader as the destination.
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from airflow_export_to_object_store import (
    EncryptionOptions,
    IncrementalConfig,
    RetryOptions,
    ShardOptions,
    StreamingExportOperator,
)

from .conftest_e2e import (
    FakeTI,
    install_fakes,
    make_context,
    make_orders_rows,
)


# ----------------------------------------------------------------------
# Streaming
# ----------------------------------------------------------------------
def test_basic_streaming_export(tmp_path, monkeypatch):
    """Single-shard export from sqlite to a filesystem-backed fake bucket."""
    storage_root = tmp_path / "bucket"
    storage_root.mkdir()
    install_fakes(monkeypatch, rows=make_orders_rows(7), storage_root=storage_root)

    op = StreamingExportOperator(
        task_id="t",
        db_hook_id="test_db",
        storage_hook_id="test_storage",
        bucket="test-bucket",
        sql_template="SELECT id, amount FROM orders",
        remote_path_template="exports/{{ ds }}/data.parquet",
        tmp_dir=str(tmp_path / "tmp"),
    )
    Path(tmp_path / "tmp").mkdir()

    result = op.execute(make_context())

    assert result["total_rows"] == 7
    assert result["total_bytes"] > 0
    assert len(result["shards"]) == 1

    landed = storage_root / "exports/2026-05-08/data.parquet"
    assert landed.exists()
    tbl = pq.read_table(str(landed))
    assert tbl.num_rows == 7
    assert tbl.column_names == ["id", "amount"]


def test_sharded_export_with_manifest_and_md5(tmp_path, monkeypatch):
    storage_root = tmp_path / "bucket"
    storage_root.mkdir()
    install_fakes(monkeypatch, rows=make_orders_rows(20), storage_root=storage_root)

    op = StreamingExportOperator(
        task_id="t",
        db_hook_id="test_db",
        storage_hook_id="test_storage",
        bucket="test-bucket",
        sql_template="""
            SELECT id, amount FROM orders
            WHERE id % {{ shards_total }} = {{ shard_id }}
        """,
        shards=[{"shard_id": i, "shards_total": 4} for i in range(4)],
        remote_path_template="exports/{{ ds }}/part_{{ '%03d' | format(shard_index) }}.parquet",
        shard_options=ShardOptions(max_workers=4, chunk_rows=10),
        compute_md5=True,
        write_manifest=True,
        tmp_dir=str(tmp_path / "tmp"),
    )
    Path(tmp_path / "tmp").mkdir()

    result = op.execute(make_context())

    assert result["total_rows"] == 20
    assert len(result["shards"]) == 4
    assert all(s["md5"] is not None for s in result["shards"])

    # All four shards on disk.
    for i in range(4):
        assert (storage_root / f"exports/2026-05-08/part_{i:03d}.parquet").exists()

    # Manifest at the common prefix, listing every shard with a non-None MD5.
    manifest_path = storage_root / "exports/2026-05-08/_manifest.json"
    assert manifest_path.exists()
    manifest = json.loads(manifest_path.read_text())
    assert manifest["total_rows"] == 20
    assert {f["shard_index"] for f in manifest["files"]} == {0, 1, 2, 3}
    assert all(f["md5"] for f in manifest["files"])


# ----------------------------------------------------------------------
# Idempotency
# ----------------------------------------------------------------------
def test_skip_if_exists_short_circuits_in_operator_path(tmp_path, monkeypatch):
    storage_root = tmp_path / "bucket"
    storage_root.mkdir()
    db_hook, _storage, uploader = install_fakes(monkeypatch, rows=make_orders_rows(5), storage_root=storage_root)

    # Pretend the destination already has all the shards — they should
    # all short-circuit without uploading anything.
    pre_existing = "exports/2026-05-08/data.parquet"
    uploader.exists_paths.add(pre_existing)

    op = StreamingExportOperator(
        task_id="t",
        db_hook_id="test_db",
        storage_hook_id="test_storage",
        bucket="test-bucket",
        sql_template="SELECT id, amount FROM orders",
        remote_path_template="exports/{{ ds }}/data.parquet",
        skip_if_exists=True,
        tmp_dir=str(tmp_path / "tmp"),
    )
    Path(tmp_path / "tmp").mkdir()

    result = op.execute(make_context())

    assert len(result["shards"]) == 1
    assert result["shards"][0]["skipped"] is True
    # The fake uploader's upload was never called.
    assert uploader.uploads == []


# ----------------------------------------------------------------------
# Watermark
# ----------------------------------------------------------------------
def test_incremental_watermark_round_trip(tmp_path, monkeypatch):
    """First run picks up default_value, runs the watermark_query, pushes
    MAX(updated_at) back to XCom. The next run would pick that up via
    xcom_pull(include_prior_dates=True)."""
    storage_root = tmp_path / "bucket"
    storage_root.mkdir()
    install_fakes(monkeypatch, rows=make_orders_rows(5), storage_root=storage_root)

    ti = FakeTI()  # no prior watermark — first run

    op = StreamingExportOperator(
        task_id="t",
        db_hook_id="test_db",
        storage_hook_id="test_storage",
        bucket="test-bucket",
        sql_template="""
            SELECT id, amount, updated_at FROM orders
            WHERE updated_at > '{{ watermark_prev }}'
              AND updated_at <= '{{ watermark_now }}'
        """,
        remote_path_template="exports/{{ ds }}/data.parquet",
        incremental=IncrementalConfig(
            watermark_query="SELECT MAX(updated_at) FROM orders",
            xcom_key="last_watermark",
            default_value="1970-01-01 00:00:00",
        ),
        tmp_dir=str(tmp_path / "tmp"),
    )
    Path(tmp_path / "tmp").mkdir()

    result = op.execute(make_context(ti=ti))

    # MAX(updated_at) for our 5 rows is "2026-05-08 04:00:00".
    assert result["watermark"] == "2026-05-08 04:00:00"
    assert ti.pushed["last_watermark"] == "2026-05-08 04:00:00"

    # All 5 rows landed (default_value was 1970, so the WHERE matches all).
    landed = storage_root / "exports/2026-05-08/data.parquet"
    assert pq.read_table(str(landed)).num_rows == 5


def test_incremental_second_run_uses_prior_watermark(tmp_path, monkeypatch):
    storage_root = tmp_path / "bucket"
    storage_root.mkdir()
    install_fakes(monkeypatch, rows=make_orders_rows(5), storage_root=storage_root)

    # Pretend the previous run pushed a watermark of "2026-05-08 02:00:00":
    # only rows with updated_at > 02:00 should land this time (rows 3 and 4).
    ti = FakeTI(prior={("t", "last_watermark"): "2026-05-08 02:00:00"})

    op = StreamingExportOperator(
        task_id="t",
        db_hook_id="test_db",
        storage_hook_id="test_storage",
        bucket="test-bucket",
        sql_template="""
            SELECT id, amount, updated_at FROM orders
            WHERE updated_at > '{{ watermark_prev }}'
              AND updated_at <= '{{ watermark_now }}'
        """,
        remote_path_template="exports/{{ ds }}/data.parquet",
        incremental=IncrementalConfig(
            watermark_query="SELECT MAX(updated_at) FROM orders",
            xcom_key="last_watermark",
            default_value="1970-01-01 00:00:00",
        ),
        tmp_dir=str(tmp_path / "tmp"),
    )
    Path(tmp_path / "tmp").mkdir()

    result = op.execute(make_context(ti=ti))

    landed = storage_root / "exports/2026-05-08/data.parquet"
    tbl = pq.read_table(str(landed))
    # Rows 3 and 4 (updated_at "03:00:00" and "04:00:00").
    assert tbl.num_rows == 2
    assert sorted(tbl.column("id").to_pylist()) == [3, 4]
    assert result["watermark"] == "2026-05-08 04:00:00"


# ----------------------------------------------------------------------
# Transform + encryption + tags
# ----------------------------------------------------------------------
def _double_amount(tbl):
    import pyarrow as pa
    import pyarrow.compute as pc

    idx = tbl.schema.get_field_index("amount")
    doubled = pc.multiply(tbl.column("amount"), pa.scalar(2.0))
    return tbl.set_column(idx, "amount", doubled)


def test_transform_encryption_and_tags_reach_uploader(tmp_path, monkeypatch):
    storage_root = tmp_path / "bucket"
    storage_root.mkdir()
    _db, _storage, uploader = install_fakes(monkeypatch, rows=make_orders_rows(3), storage_root=storage_root)

    enc = EncryptionOptions(kms_key_id="arn:aws:kms:us-east-1:1:key/k", sse_algorithm="aws:kms")
    op = StreamingExportOperator(
        task_id="t",
        db_hook_id="test_db",
        storage_hook_id="test_storage",
        bucket="test-bucket",
        sql_template="SELECT id, amount FROM orders",
        remote_path_template="exports/{{ ds }}/data.parquet",
        transform_fn=_double_amount,
        encryption=enc,
        tags={"env": "prod", "team": "data"},
        write_manifest=True,
        tmp_dir=str(tmp_path / "tmp"),
    )
    Path(tmp_path / "tmp").mkdir()

    op.execute(make_context())

    # Two uploads: the data shard and the manifest. Both must carry the
    # operator's encryption + tags.
    assert len(uploader.uploads) == 2
    for call in uploader.uploads:
        assert call.encryption is enc
        assert call.tags == {"env": "prod", "team": "data"}

    # Transform actually ran: amount column is doubled.
    landed = storage_root / "exports/2026-05-08/data.parquet"
    tbl = pq.read_table(str(landed))
    assert tbl.column("amount").to_pylist() == [0.0, 20.0, 40.0]


# ----------------------------------------------------------------------
# Failure cancellation
# ----------------------------------------------------------------------
def test_failure_in_one_shard_cancels_siblings(tmp_path, monkeypatch):
    """When shard 0 fails its upload, surviving shards should observe
    the cross-shard cancel and exit promptly. The operator re-raises
    the original exception."""
    storage_root = tmp_path / "bucket"
    storage_root.mkdir()
    _db, _storage, uploader = install_fakes(monkeypatch, rows=make_orders_rows(40), storage_root=storage_root)
    # Force the upload of shard 0 to fail.
    uploader.fail_remote_paths.add("exports/2026-05-08/part_000.parquet")

    op = StreamingExportOperator(
        task_id="t",
        db_hook_id="test_db",
        storage_hook_id="test_storage",
        bucket="test-bucket",
        sql_template="""
            SELECT id, amount FROM orders
            WHERE id % {{ shards_total }} = {{ shard_id }}
        """,
        shards=[{"shard_id": i, "shards_total": 4} for i in range(4)],
        remote_path_template="exports/{{ ds }}/part_{{ '%03d' | format(shard_index) }}.parquet",
        shard_options=ShardOptions(max_workers=4, chunk_rows=10),
        retry_options=RetryOptions(upload_retries=0, backoff_base=1.0, backoff_cap=0.0),
        tmp_dir=str(tmp_path / "tmp"),
    )
    Path(tmp_path / "tmp").mkdir()

    with pytest.raises(RuntimeError, match="forced upload failure"):
        op.execute(make_context())

    # Operator does NOT write a partial manifest on failure.
    assert not (storage_root / "exports/2026-05-08/_manifest.json").exists()
