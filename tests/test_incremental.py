"""IncrementalConfig dataclass + watermark coercion."""
from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest

from airflow_export_to_object_store.incremental import IncrementalConfig, coerce_watermark


def test_requires_exactly_one_source():
    with pytest.raises(ValueError, match="exactly one"):
        IncrementalConfig()
    with pytest.raises(ValueError, match="exactly one"):
        IncrementalConfig(
            watermark_query="SELECT MAX(updated_at) FROM t",
            watermark_now_template="{{ ts }}",
        )


def test_accepts_query_alone():
    cfg = IncrementalConfig(watermark_query="SELECT MAX(updated_at) FROM t")
    assert cfg.xcom_key == "watermark"
    assert cfg.default_value == "1970-01-01 00:00:00"


def test_accepts_template_alone():
    cfg = IncrementalConfig(watermark_now_template="{{ ts }}")
    assert cfg.watermark_query is None
    assert cfg.watermark_now_template == "{{ ts }}"


def test_coerce_watermark_handles_common_db_types():
    assert coerce_watermark(None) == ""
    assert coerce_watermark("2026-05-08") == "2026-05-08"
    assert coerce_watermark(123) == "123"
    assert coerce_watermark(Decimal("99.5")) == "99.5"
    assert coerce_watermark(dt.datetime(2026, 5, 8, 12, 0)) == "2026-05-08 12:00:00"
    assert coerce_watermark(dt.date(2026, 5, 8)) == "2026-05-08"


def test_options_are_frozen():
    cfg = IncrementalConfig(watermark_now_template="x")
    with pytest.raises(Exception):
        cfg.xcom_key = "other"  # type: ignore[misc]
