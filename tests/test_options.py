"""Defaults and immutability of option dataclasses."""
from __future__ import annotations

import dataclasses

import pytest

from airflow_export_to_object_store.options import (
    ParquetOptions,
    RetryOptions,
    ShardOptions,
    ShardResult,
)


def test_parquet_defaults():
    p = ParquetOptions()
    assert p.compression == "zstd"
    assert p.row_group_size == 512_000
    assert p.coerce_timestamps == "ms"
    assert p.write_statistics is False
    assert p.use_dictionary is True


def test_retry_defaults():
    r = RetryOptions()
    assert r.upload_retries == 3
    assert r.backoff_base == 1.5
    assert r.backoff_cap == 20.0


def test_shard_defaults():
    s = ShardOptions()
    assert s.max_workers == 6
    assert s.chunk_rows == 50_000
    assert s.memory_limit_mb == 1024
    assert s.timeout is None
    assert s.execution_mode == "threads"


def test_shard_execution_mode_processes():
    s = ShardOptions(execution_mode="processes")
    assert s.execution_mode == "processes"


def test_options_are_frozen():
    p = ParquetOptions()
    with pytest.raises(dataclasses.FrozenInstanceError):
        p.compression = "snappy"  # type: ignore[misc]


def test_shard_result_constructs():
    r = ShardResult(shard_index=0, remote_uri="s3://x/y", rows=10, bytes=1024, md5=None, elapsed_s=1.5)
    assert r.shard_index == 0
    assert r.remote_uri == "s3://x/y"
