"""Tests for parquet_validator.validate_parquet_schema."""
from __future__ import annotations

import logging

import pyarrow as pa
import pyarrow.parquet as pq

from airflow_export_to_object_store.parquet_validator import validate_parquet_schema

LOG = logging.getLogger("test-parquet-validator")


def _write_parquet(path, tbl):
    pq.write_table(tbl, path)


def test_valid_parquet_passes(tmp_path):
    p = tmp_path / "ok.parquet"
    _write_parquet(str(p), pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]}))
    assert validate_parquet_schema(str(p), LOG) is True


def test_missing_file_fails(tmp_path):
    assert validate_parquet_schema(str(tmp_path / "nope.parquet"), LOG) is False


def test_empty_file_fails(tmp_path):
    p = tmp_path / "empty.parquet"
    p.write_bytes(b"")
    assert validate_parquet_schema(str(p), LOG) is False


def test_corrupt_footer_fails(tmp_path):
    p = tmp_path / "bad.parquet"
    p.write_bytes(b"not a parquet file at all")
    assert validate_parquet_schema(str(p), LOG) is False
