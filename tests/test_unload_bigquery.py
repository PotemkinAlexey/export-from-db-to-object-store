"""BigQueryUnloadStrategy unit tests.

Same shape as the Snowflake tests: SQL building, dispatch, and an
end-to-end with a fake hook + fake GCS hook for the post-export
listing. Real BigQuery and GCS are not touched.
"""

from __future__ import annotations

import logging

import pytest

pytest.importorskip("airflow.providers.google.cloud.hooks.bigquery")
pytest.importorskip("airflow.providers.google.cloud.hooks.gcs")

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook  # noqa: E402
from airflow.providers.google.cloud.hooks.gcs import GCSHook  # noqa: E402

from airflow_export_to_object_store.unload.bigquery import (  # noqa: E402
    BigQueryUnloadOptions,
    BigQueryUnloadStrategy,
)

LOG = logging.getLogger("test-unload-bigquery")


class _FakeBQ(BigQueryHook):
    def __init__(self, rows=None):  # type: ignore[override]
        self._rows = rows or []
        self.executed: list[str] = []

    def get_records(self, sql):  # type: ignore[override]
        self.executed.append(sql)
        return self._rows


class _FakeGCS(GCSHook):
    def __init__(self, files):  # type: ignore[override]
        # files: list of (object_name, size)
        self._files = files

    def list(self, bucket_name, prefix=None, **kwargs):  # type: ignore[override]
        return [name for name, _size in self._files if not prefix or name.startswith(prefix)]

    def get_size(self, bucket_name, object_name):  # type: ignore[override]
        for name, size in self._files:
            if name == object_name:
                return size
        return 0


# matches() ------------------------------------------------------------
def test_matches_bq_to_gcs():
    s = BigQueryUnloadStrategy()
    assert s.matches(_FakeBQ(), _FakeGCS([])) is True


def test_does_not_match_non_bq_db():
    s = BigQueryUnloadStrategy()

    class _NotBQ:
        pass

    assert s.matches(_NotBQ(), _FakeGCS([])) is False


def test_does_not_match_non_gcs_storage():
    s = BigQueryUnloadStrategy()

    class _NotGCS:
        pass

    assert s.matches(_FakeBQ(), _NotGCS()) is False


# SQL building ---------------------------------------------------------
def test_build_export_sql_default_options():
    s = BigQueryUnloadStrategy()
    sql = s._build_export_sql(target_uri="gs://b/path/*.parquet", select_sql="SELECT * FROM t WHERE 1=1;")
    assert sql.startswith("EXPORT DATA OPTIONS(")
    assert "uri='gs://b/path/*.parquet'" in sql
    assert "format='PARQUET'" in sql
    assert "compression='ZSTD'" in sql
    assert "overwrite=true" in sql
    # Trailing semicolon was stripped from the SELECT.
    assert "AS\nSELECT * FROM t WHERE 1=1" in sql


def test_build_export_sql_overwrite_false():
    s = BigQueryUnloadStrategy(BigQueryUnloadOptions(overwrite=False))
    sql = s._build_export_sql(target_uri="gs://b/p/*.parquet", select_sql="SELECT 1")
    assert "overwrite=false" in sql


def test_build_export_sql_includes_extra_options():
    s = BigQueryUnloadStrategy(BigQueryUnloadOptions(extra_options={"field_delimiter": "'|'"}))
    sql = s._build_export_sql(target_uri="gs://b/p/*.parquet", select_sql="SELECT 1")
    assert "field_delimiter='|'" in sql


# unload() end-to-end ---------------------------------------------------
def test_unload_runs_export_and_lists_results():
    bq = _FakeBQ()
    gcs = _FakeGCS(
        [
            ("exports/2026/000000000000.parquet", 1024),
            ("exports/2026/000000000001.parquet", 2048),
        ]
    )
    s = BigQueryUnloadStrategy()

    results = s.unload(
        db_hook=bq,
        storage_hook=gcs,
        sql="SELECT * FROM t",
        remote_dir="exports/2026",
        container=None,
        bucket="my-bucket",
        log=LOG,
    )

    assert len(bq.executed) == 1
    assert "EXPORT DATA OPTIONS(" in bq.executed[0]
    assert "gs://my-bucket/exports/2026/*.parquet" in bq.executed[0]

    assert len(results) == 2
    # Sorted by key for deterministic output.
    assert results[0].remote_uri == "gs://my-bucket/exports/2026/000000000000.parquet"
    assert results[0].bytes == 1024
    assert results[1].bytes == 2048
    # EXPORT DATA doesn't surface per-file row counts.
    assert all(r.rows == 0 for r in results)


def test_unload_requires_bucket():
    s = BigQueryUnloadStrategy()
    with pytest.raises(ValueError, match="bucket must be set"):
        s.unload(
            db_hook=_FakeBQ(),
            storage_hook=_FakeGCS([]),
            sql="SELECT 1",
            remote_dir="x/",
            container=None,
            bucket=None,
            log=LOG,
        )


def test_unload_filters_directory_markers():
    """GCS list may return zero-byte ``foo/`` directory markers; ignore them."""
    bq = _FakeBQ()
    gcs = _FakeGCS([("p/", 0), ("p/data_000.parquet", 100)])
    s = BigQueryUnloadStrategy()
    results = s.unload(
        db_hook=bq, storage_hook=gcs, sql="SELECT 1", remote_dir="p", container=None, bucket="b", log=LOG
    )
    assert len(results) == 1
    assert results[0].remote_uri == "gs://b/p/data_000.parquet"
