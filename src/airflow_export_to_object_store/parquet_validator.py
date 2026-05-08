"""Lightweight pre-upload Parquet validation."""
from __future__ import annotations

import logging
import os

import pyarrow as pa
import pyarrow.parquet as pq


def validate_parquet_schema(file_path: str, log: logging.Logger) -> bool:
    """Check that a Parquet file is readable, has row groups, and a sane schema.

    Reads only the first row group (and a small sample slice from it) to keep
    validation cost bounded even for multi-GB files. Returns ``True`` on
    success, ``False`` on any structural problem (with details logged).
    """
    try:
        if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
            log.error("❌ Parquet file is empty or missing: %s", file_path)
            return False

        try:
            meta = pq.read_metadata(file_path)
        except Exception as e:
            log.error("❌ Failed to read Parquet metadata/footer: %s", e)
            return False

        if meta.num_row_groups == 0:
            log.error("❌ Parquet file has zero row groups — likely corrupt.")
            return False

        try:
            schema = pq.read_schema(file_path)
        except Exception as e:
            log.error("❌ Failed to read Parquet schema: %s", e)
            return False

        for field in schema:
            t = field.type
            if pa.types.is_nested(t):
                log.warning("⚠️ Nested type: %s → %s", field.name, t)
            if pa.types.is_map(t):
                log.warning("⚠️ MAP type detected — may not be supported by some engines: %s", field.name)
            if pa.types.is_large_list(t) or pa.types.is_large_binary(t):
                log.warning("⚠️ Large* Parquet types detected: %s → %s", field.name, t)
            if pa.types.is_decimal(t) and t.precision > 38:
                log.warning("⚠️ High precision DECIMAL(%d,%d) might break consumers", t.precision, t.scale)
            if pa.types.is_timestamp(t) and t.unit not in ("ms", "us", "ns"):
                log.warning("⚠️ Unexpected timestamp unit %s for column %s", t.unit, field.name)

        try:
            pf = pq.ParquetFile(file_path)
            if pf.num_row_groups == 0:
                log.error("❌ Parquet has zero row groups.")
                return False
            row_group = pf.read_row_group(0, use_threads=False)
            sample = row_group.slice(0, min(1000, row_group.num_rows))
            _ = sample.num_rows  # force materialization
            log.info("Sample read OK ✓ (rows=%d, columns=%d)", sample.num_rows, len(sample.schema.names))
        except Exception as e:
            log.error("❌ Failed to read sample data (all columns): %s", e)
            return False

        if row_group.num_rows == 0:
            log.warning("⚠️ First row group has zero rows.")

        log.info(
            "✅ Parquet validation succeeded: sample_rows=%d, columns=%d, row_groups=%d",
            sample.num_rows,
            len(schema),
            meta.num_row_groups,
        )
        return True

    except Exception as e:
        log.error("❌ Unexpected Parquet validation error: %s", e)
        return False
