"""Tests for utils.compute_md5_eff and utils.coerce_ts_table."""
from __future__ import annotations

import hashlib
import os

import pyarrow as pa

from airflow_export_to_object_store.utils import coerce_ts_table, compute_md5_eff


def test_md5_matches_hashlib(tmp_path):
    p = tmp_path / "data.bin"
    payload = os.urandom(123_456)
    p.write_bytes(payload)
    assert compute_md5_eff(str(p)) == hashlib.md5(payload).hexdigest()


def test_md5_skip_when_above_threshold(tmp_path):
    p = tmp_path / "small.bin"
    p.write_bytes(b"hello")
    # threshold of 0 GB → any non-empty file is skipped
    assert compute_md5_eff(str(p), skip_threshold_gb=0) is None


def test_md5_calls_log_fn_on_skip(tmp_path):
    p = tmp_path / "small.bin"
    p.write_bytes(b"hello")
    captured = []
    compute_md5_eff(str(p), log_fn=captured.append, skip_threshold_gb=0)
    assert captured and "Skipping MD5" in captured[0]


def test_coerce_ts_no_op_for_unknown_unit():
    tbl = pa.table({"ts": pa.array([1, 2], type=pa.timestamp("ns"))})
    assert coerce_ts_table(tbl, "bogus").schema.field("ts").type.unit == "ns"


def test_coerce_ts_changes_unit():
    tbl = pa.table({"ts": pa.array([1_000_000_000], type=pa.timestamp("ns"))})
    out = coerce_ts_table(tbl, "ms")
    assert out.schema.field("ts").type.unit == "ms"


def test_coerce_ts_preserves_non_timestamp_columns():
    tbl = pa.table(
        {
            "ts": pa.array([1], type=pa.timestamp("ns")),
            "n": pa.array([42], type=pa.int64()),
            "s": pa.array(["x"]),
        }
    )
    out = coerce_ts_table(tbl, "us")
    assert out.schema.field("ts").type.unit == "us"
    assert out.schema.field("n").type == pa.int64()
    assert out.schema.field("s").type == pa.string()
