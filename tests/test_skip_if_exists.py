"""Idempotency: skip_if_exists short-circuits the shard before any DB work."""
from __future__ import annotations

import logging

import pytest

from airflow_export_to_object_store.options import (
    ParquetOptions,
    RetryOptions,
    ShardOptions,
    ShardResult,
)
from airflow_export_to_object_store.shard_task import ShardTaskParams, _canonical_uri
from airflow_export_to_object_store.uploaders import AzureBlobUploader, GCSUploader, S3Uploader

LOG = logging.getLogger("test-skip-if-exists")


def _params(**overrides):
    base = dict(
        shard_index=0,
        sql_text="SELECT 1",
        filename="x.parquet",
        remote_path="exports/2026/data_000.parquet",
        db_hook_id="db",
        storage_hook_id="storage",
        tmp_dir=None,
        container=None,
        bucket="my-bucket",
        overwrite=True,
        compute_md5=False,
        validate_parquet=False,
        parquet_options=ParquetOptions(),
        shard_options=ShardOptions(),
        retry_options=RetryOptions(),
        skip_if_exists=False,
    )
    base.update(overrides)
    return ShardTaskParams(**base)


def test_canonical_uri_each_backend():
    p = _params(bucket="b", container=None, remote_path="a/b.parquet")
    assert _canonical_uri("s3", p) == "s3://b/a/b.parquet"
    assert _canonical_uri("gcs", p) == "gs://b/a/b.parquet"
    p2 = _params(bucket=None, container="c", remote_path="a/b.parquet")
    assert _canonical_uri("azure", p2) == "azure://c/a/b.parquet"


def test_canonical_uri_missing_target_returns_empty():
    p = _params(bucket=None, container=None)
    assert _canonical_uri("s3", p) == ""


def test_skip_if_exists_short_circuits_without_db(monkeypatch):
    """When the remote already exists, execute_shard returns a skipped result
    without ever opening a DB cursor."""
    from airflow_export_to_object_store import shard_task

    db_calls: list[str] = []

    def _fail_get_hook(conn_id):
        db_calls.append(conn_id)
        if conn_id == "storage":
            class _Hook:
                pass
            return _Hook()
        raise AssertionError(f"DB hook should never be requested, got {conn_id}")

    monkeypatch.setattr(shard_task.BaseHook, "get_hook", staticmethod(_fail_get_hook))

    # Force the resolver to a stub uploader whose exists() returns True.
    class _StubUploader:
        name = "s3"

        def matches(self, _hook):
            return True

        def network_targets(self):
            return []

        def health_check(self, *a, **kw):
            return None

        def exists(self, *_a, **kw):
            return True

        def upload(self, *_a, **kw):
            raise AssertionError("upload should not be called when skip_if_exists hits")

    monkeypatch.setattr(shard_task, "resolve_uploader", lambda _hook: _StubUploader())

    result, metric = shard_task.execute_shard(_params(skip_if_exists=True))

    assert isinstance(result, ShardResult)
    assert result.skipped is True
    assert result.rows == 0
    assert result.bytes == 0
    assert result.remote_uri == "s3://my-bucket/exports/2026/data_000.parquet"
    assert metric["skipped"] is True
    # Only the storage hook was looked up; no DB hook.
    assert db_calls == ["storage"]


def test_uploader_exists_implementations_exist():
    """Smoke check: every backend implements exists()."""
    for u in (S3Uploader(), AzureBlobUploader(), GCSUploader()):
        assert callable(getattr(u, "exists", None)), u.name


@pytest.mark.parametrize(
    "uploader_cls,kwargs,expected_uri_prefix",
    [
        (AzureBlobUploader, dict(container=None, bucket=None), False),
        (S3Uploader, dict(container=None, bucket=None), False),
        (GCSUploader, dict(container=None, bucket=None), False),
    ],
)
def test_exists_returns_false_when_target_missing(uploader_cls, kwargs, expected_uri_prefix):
    """No bucket/container => exists() must be False, never raise."""
    assert (
        uploader_cls().exists(object(), remote_path="x", **kwargs) is False
    )
