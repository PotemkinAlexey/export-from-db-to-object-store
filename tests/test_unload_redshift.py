"""RedshiftUnloadStrategy unit tests.

Same shape as the Snowflake / BigQuery tests: SQL building, dispatch,
end-to-end against a fake Redshift hook + fake S3 hook for the
post-unload listing. Real Redshift and S3 are not touched.
"""

from __future__ import annotations

import logging

import pytest

pytest.importorskip("airflow.providers.amazon.aws.hooks.redshift_sql")
pytest.importorskip("airflow.providers.amazon.aws.hooks.s3")

from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook  # noqa: E402
from airflow.providers.amazon.aws.hooks.s3 import S3Hook  # noqa: E402

from airflow_export_to_object_store.unload.redshift import (  # noqa: E402
    RedshiftUnloadOptions,
    RedshiftUnloadStrategy,
)

LOG = logging.getLogger("test-unload-redshift")


class _FakeRedshift(RedshiftSQLHook):
    def __init__(self, rows=None):  # type: ignore[override]
        self._rows = rows or []
        self.executed: list[str] = []

    def get_records(self, sql):  # type: ignore[override]
        self.executed.append(sql)
        return self._rows


class _FakeS3(S3Hook):
    def __init__(self, files=None):  # type: ignore[override]
        # files: list of (key, size)
        self._files = files or []

    def list_keys(self, bucket_name, prefix=None, **kwargs):  # type: ignore[override]
        return [k for k, _ in self._files if not prefix or k.startswith(prefix)]

    def head_object(self, key, bucket_name, **kwargs):  # type: ignore[override]
        for k, size in self._files:
            if k == key:
                return {"ContentLength": size}
        return {"ContentLength": 0}


# matches() ------------------------------------------------------------
def test_matches_redshift_to_s3():
    s = RedshiftUnloadStrategy(RedshiftUnloadOptions(iam_role="arn:aws:iam::1:role/r"))
    assert s.matches(_FakeRedshift(), _FakeS3()) is True


def test_does_not_match_non_redshift_db():
    s = RedshiftUnloadStrategy(RedshiftUnloadOptions(iam_role="arn:aws:iam::1:role/r"))

    class _NotRedshift:
        pass

    assert s.matches(_NotRedshift(), _FakeS3()) is False


def test_does_not_match_non_s3_storage():
    s = RedshiftUnloadStrategy(RedshiftUnloadOptions(iam_role="arn:aws:iam::1:role/r"))

    class _NotS3:
        pass

    assert s.matches(_FakeRedshift(), _NotS3()) is False


# SQL building ---------------------------------------------------------
def test_build_unload_sql_with_iam_role():
    s = RedshiftUnloadStrategy(RedshiftUnloadOptions(iam_role="arn:aws:iam::1:role/r"))
    sql = s._build_unload_sql(target="s3://b/path/", select_sql="SELECT * FROM t WHERE date = '2026-05-08';")
    assert sql.startswith("UNLOAD ('SELECT * FROM t WHERE date = ''2026-05-08''')")
    assert "TO 's3://b/path/'" in sql
    assert "IAM_ROLE 'arn:aws:iam::1:role/r'" in sql
    assert "FORMAT AS PARQUET" in sql
    assert "PARALLEL ON" in sql
    assert "MAXFILESIZE 256 MB" in sql
    assert "CLEANPATH" in sql


def test_build_unload_sql_with_credentials():
    s = RedshiftUnloadStrategy(RedshiftUnloadOptions(credentials="ACCESS_KEY_ID=...;SECRET_ACCESS_KEY=..."))
    sql = s._build_unload_sql(target="s3://b/", select_sql="SELECT 1")
    assert "CREDENTIALS 'ACCESS_KEY_ID=...;SECRET_ACCESS_KEY=...'" in sql
    assert "IAM_ROLE" not in sql


def test_build_unload_sql_rejects_both_auth_modes():
    s = RedshiftUnloadStrategy(RedshiftUnloadOptions(iam_role="arn:aws:iam::1:role/r", credentials="x"))
    with pytest.raises(ValueError, match="iam_role or credentials"):
        s._build_unload_sql(target="s3://b/", select_sql="SELECT 1")


def test_build_unload_sql_rejects_no_auth():
    s = RedshiftUnloadStrategy(RedshiftUnloadOptions())
    with pytest.raises(ValueError, match="One of iam_role / credentials"):
        s._build_unload_sql(target="s3://b/", select_sql="SELECT 1")


def test_build_unload_sql_parallel_off():
    s = RedshiftUnloadStrategy(RedshiftUnloadOptions(iam_role="arn:aws:iam::1:role/r", parallel=False, cleanpath=False))
    sql = s._build_unload_sql(target="s3://b/", select_sql="SELECT 1")
    assert "PARALLEL OFF" in sql
    assert "CLEANPATH" not in sql


def test_build_unload_sql_with_manifest_and_extras():
    s = RedshiftUnloadStrategy(
        RedshiftUnloadOptions(
            iam_role="arn:aws:iam::1:role/r",
            manifest=True,
            extra_options=["ENCRYPTED", "ALLOWOVERWRITE"],
        )
    )
    sql = s._build_unload_sql(target="s3://b/", select_sql="SELECT 1")
    assert "MANIFEST" in sql
    assert "ENCRYPTED" in sql
    assert "ALLOWOVERWRITE" in sql


def test_build_unload_sql_doubles_inner_quotes():
    """Single quotes inside the SELECT must be doubled — UNLOAD wraps the
    whole SELECT in single quotes."""
    s = RedshiftUnloadStrategy(RedshiftUnloadOptions(iam_role="r"))
    sql = s._build_unload_sql(
        target="s3://b/",
        select_sql="SELECT 'foo' AS bar FROM t WHERE x = 'baz'",
    )
    assert "''foo'' AS bar" in sql
    assert "x = ''baz''" in sql


# unload() end-to-end ---------------------------------------------------
def test_unload_runs_unload_and_lists_results():
    rs = _FakeRedshift()
    s3 = _FakeS3(
        [
            ("exports/2026/0000_part_00.parquet", 1024),
            ("exports/2026/0001_part_00.parquet", 2048),
        ]
    )
    s = RedshiftUnloadStrategy(RedshiftUnloadOptions(iam_role="arn:aws:iam::1:role/r"))

    results = s.unload(
        db_hook=rs,
        storage_hook=s3,
        sql="SELECT * FROM t",
        remote_dir="exports/2026",
        container=None,
        bucket="my-bucket",
        log=LOG,
    )

    assert len(rs.executed) == 1
    assert "UNLOAD ('SELECT * FROM t')" in rs.executed[0]
    assert "TO 's3://my-bucket/exports/2026/'" in rs.executed[0]

    assert len(results) == 2
    assert results[0].remote_uri == "s3://my-bucket/exports/2026/0000_part_00.parquet"
    assert results[0].bytes == 1024
    # Per-file row counts are not surfaced by UNLOAD.
    assert all(r.rows == 0 for r in results)


def test_unload_skips_manifest_file_in_results():
    """When ``MANIFEST`` is on Redshift writes an extra ``manifest`` file
    next to the data — that's catalog metadata and shouldn't appear as
    a data shard."""
    rs = _FakeRedshift()
    s3 = _FakeS3(
        [
            ("p/0000_part_00.parquet", 100),
            ("p/manifest", 5),
        ]
    )
    s = RedshiftUnloadStrategy(RedshiftUnloadOptions(iam_role="r"))
    results = s.unload(db_hook=rs, storage_hook=s3, sql="SELECT 1", remote_dir="p", container=None, bucket="b", log=LOG)
    assert len(results) == 1
    assert results[0].remote_uri.endswith("0000_part_00.parquet")


def test_unload_requires_bucket():
    s = RedshiftUnloadStrategy(RedshiftUnloadOptions(iam_role="r"))
    with pytest.raises(ValueError, match="bucket must be set"):
        s.unload(
            db_hook=_FakeRedshift(),
            storage_hook=_FakeS3(),
            sql="SELECT 1",
            remote_dir="x",
            container=None,
            bucket=None,
            log=LOG,
        )
