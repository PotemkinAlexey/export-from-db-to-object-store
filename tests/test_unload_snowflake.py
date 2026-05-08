"""SnowflakeUnloadStrategy unit tests.

We don't spin up Snowflake — we only test the value-add boundaries:

* SQL building (storage_integration vs credentials, mutual exclusion,
  every COPY INTO clause comes through).
* Backend dispatch (S3 / Azure / GCS recognition; rejection of unknown
  storage hooks).
* Result-row parsing (dict, tuple file-name-first, tuple
  rows-unloaded-first).
* Operator integration boundary (matches() False if db hook is not
  Snowflake).
"""

from __future__ import annotations

import logging

import pytest

pytest.importorskip("airflow.providers.snowflake.hooks.snowflake")
pytest.importorskip("airflow.providers.amazon.aws.hooks.s3")

from airflow.providers.amazon.aws.hooks.s3 import S3Hook  # noqa: E402
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook  # noqa: E402

from airflow_export_to_object_store.unload.snowflake import (  # noqa: E402
    SnowflakeUnloadOptions,
    SnowflakeUnloadStrategy,
    _extract_unload_columns,
    _rows_to_shard_results,
)

LOG = logging.getLogger("test-unload-snowflake")


class _FakeSnowflake(SnowflakeHook):
    def __init__(self, rows):  # type: ignore[override]
        self._rows = rows
        self.executed: list[str] = []

    def get_records(self, sql):  # type: ignore[override]
        self.executed.append(sql)
        return self._rows


class _FakeS3(S3Hook):
    def __init__(self):  # type: ignore[override]
        pass


# ----------------------------------------------------------------------
# matches()
# ----------------------------------------------------------------------
def test_matches_snowflake_to_s3():
    s = SnowflakeUnloadStrategy(SnowflakeUnloadOptions(storage_integration="MY_INT"))
    assert s.matches(_FakeSnowflake([]), _FakeS3()) is True


def test_does_not_match_non_snowflake_db():
    s = SnowflakeUnloadStrategy(SnowflakeUnloadOptions(storage_integration="MY_INT"))

    class _NotSnowflake:
        pass

    assert s.matches(_NotSnowflake(), _FakeS3()) is False


def test_does_not_match_unknown_storage():
    s = SnowflakeUnloadStrategy(SnowflakeUnloadOptions(storage_integration="MY_INT"))

    class _UnknownStorage:
        pass

    assert s.matches(_FakeSnowflake([]), _UnknownStorage()) is False


# ----------------------------------------------------------------------
# SQL building
# ----------------------------------------------------------------------
def test_build_copy_sql_with_integration_uses_clauses():
    s = SnowflakeUnloadStrategy(SnowflakeUnloadOptions(storage_integration="MY_INT"))
    sql = s._build_copy_sql(target="s3://b/path/", select_sql="SELECT * FROM t WHERE 1=1;")
    assert "COPY INTO 's3://b/path/'" in sql
    assert "FROM (SELECT * FROM t WHERE 1=1)" in sql  # trailing semicolon stripped
    assert "STORAGE_INTEGRATION = MY_INT" in sql
    assert "FILE_FORMAT = (TYPE = PARQUET COMPRESSION = ZSTD)" in sql
    assert "MAX_FILE_SIZE = 268435456" in sql  # 256 MiB
    assert "OVERWRITE = TRUE" in sql
    assert "SINGLE = FALSE" in sql


def test_build_copy_sql_with_credentials():
    s = SnowflakeUnloadStrategy(SnowflakeUnloadOptions(credentials={"AWS_KEY_ID": "k", "AWS_SECRET_KEY": "s"}))
    sql = s._build_copy_sql(target="s3://b/path/", select_sql="SELECT 1")
    assert "CREDENTIALS = (AWS_KEY_ID='k' AWS_SECRET_KEY='s')" in sql
    assert "STORAGE_INTEGRATION" not in sql


def test_build_copy_sql_rejects_both_auth_modes():
    s = SnowflakeUnloadStrategy(SnowflakeUnloadOptions(storage_integration="X", credentials={"AWS_KEY_ID": "y"}))
    with pytest.raises(ValueError, match="storage_integration or credentials"):
        s._build_copy_sql(target="s3://b/", select_sql="SELECT 1")


def test_build_copy_sql_rejects_no_auth():
    s = SnowflakeUnloadStrategy(SnowflakeUnloadOptions())
    with pytest.raises(ValueError, match="One of storage_integration / credentials"):
        s._build_copy_sql(target="s3://b/", select_sql="SELECT 1")


def test_build_copy_sql_includes_extra_options():
    s = SnowflakeUnloadStrategy(
        SnowflakeUnloadOptions(
            storage_integration="X",
            extra_options={"PARTITION": "BY ('year='||year)"},
        )
    )
    sql = s._build_copy_sql(target="s3://b/", select_sql="SELECT 1")
    assert "PARTITION = BY ('year='||year)" in sql


# ----------------------------------------------------------------------
# Target resolution
# ----------------------------------------------------------------------
def test_build_target_s3_requires_bucket():
    s = SnowflakeUnloadStrategy(SnowflakeUnloadOptions(storage_integration="X"))
    with pytest.raises(ValueError, match="bucket must be set"):
        s._build_target("s3", container=None, bucket=None, remote_dir="x/")


def test_build_target_s3_assembles_uri():
    s = SnowflakeUnloadStrategy(SnowflakeUnloadOptions(storage_integration="X"))
    assert s._build_target("s3", container=None, bucket="b", remote_dir="path/") == "s3://b/path/"


def test_build_target_gcs_assembles_uri():
    s = SnowflakeUnloadStrategy(SnowflakeUnloadOptions(storage_integration="X"))
    assert s._build_target("gcs", container=None, bucket="b", remote_dir="path/") == "gcs://b/path/"


# ----------------------------------------------------------------------
# Result row parsing
# ----------------------------------------------------------------------
def test_rows_to_shard_results_handles_dict_rows():
    rows = [
        {"file_name": "data_0_0_0.snappy.parquet", "rows_unloaded": 100, "output_bytes": 2048},
        {"file_name": "data_0_1_0.snappy.parquet", "rows_unloaded": 200, "output_bytes": 4096},
    ]
    results = _rows_to_shard_results(rows, target="s3://b/path", log=LOG)
    assert len(results) == 2
    assert results[0].rows == 100
    assert results[0].bytes == 2048
    assert results[0].remote_uri == "s3://b/path/data_0_0_0.snappy.parquet"
    assert results[1].shard_index == 1


def test_extract_unload_columns_tuple_filename_first():
    """Modern Snowflake driver: (file_name, rows, input_bytes, output_bytes, ...)."""
    rows_unloaded, output_bytes, file_name = _extract_unload_columns(("data_0.parquet", 500, 12345, 8192))
    assert (rows_unloaded, output_bytes, file_name) == (500, 8192, "data_0.parquet")


def test_extract_unload_columns_tuple_legacy_order():
    """Legacy: (rows_unloaded, input_bytes, output_bytes, file_name)."""
    rows_unloaded, output_bytes, file_name = _extract_unload_columns((500, 12345, 8192, "data_0.parquet"))
    assert (rows_unloaded, output_bytes, file_name) == (500, 8192, "data_0.parquet")


def test_extract_unload_columns_handles_dict_uppercase():
    rows_unloaded, output_bytes, file_name = _extract_unload_columns(
        {"FILE_NAME": "f.parquet", "ROWS_UNLOADED": 7, "OUTPUT_BYTES": 100}
    )
    assert (rows_unloaded, output_bytes, file_name) == (7, 100, "f.parquet")


# ----------------------------------------------------------------------
# unload() end-to-end with a fake Snowflake hook
# ----------------------------------------------------------------------
def test_unload_runs_copy_into_and_returns_shard_results():
    rows = [("data_0.parquet", 1000, 4000, 2048)]
    db = _FakeSnowflake(rows)
    storage = _FakeS3()
    s = SnowflakeUnloadStrategy(SnowflakeUnloadOptions(storage_integration="MY_INT"))

    results = s.unload(
        db_hook=db,
        storage_hook=storage,
        sql="SELECT * FROM t",
        remote_dir="exports/2026/",
        container=None,
        bucket="my-bucket",
        log=LOG,
    )

    assert len(db.executed) == 1
    assert "COPY INTO 's3://my-bucket/exports/2026/'" in db.executed[0]
    assert "STORAGE_INTEGRATION = MY_INT" in db.executed[0]

    assert len(results) == 1
    assert results[0].rows == 1000
    assert results[0].bytes == 2048
    assert results[0].remote_uri == "s3://my-bucket/exports/2026/data_0.parquet"
    assert results[0].skipped is False
