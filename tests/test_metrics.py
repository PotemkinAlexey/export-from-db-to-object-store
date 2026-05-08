"""Smoke tests for ExportMetrics."""
from __future__ import annotations

import logging

from airflow_export_to_object_store.metrics import ExportMetrics


class _FakeOperator:
    log = logging.getLogger("fake-op")


def test_summary_with_recorded_shards():
    m = ExportMetrics(_FakeOperator())
    m.start()
    m.record_shard(0, rows=1000, bytes_=2_000_000, duration=1.0)
    m.record_shard(1, rows=500, bytes_=1_000_000, duration=0.5)

    s = m.summary()

    assert s["total_rows"] == 1500
    assert s["total_bytes"] == 3_000_000
    assert s["total_bytes_mb"] > 0
    assert len(s["shards"]) == 2
    assert s["shards"][0]["throughput_rows_s"] > 0


def test_summary_without_start_returns_empty():
    m = ExportMetrics(_FakeOperator())
    assert m.summary() == {}
