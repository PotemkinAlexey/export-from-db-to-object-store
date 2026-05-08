"""Unit tests for GCSUploader.

GCS does not have a moto-equivalent in our default deps, so the upload path
is exercised against a mocked GCSHook (the boundary we actually depend on)
and the registry is checked through the public ``resolve_uploader``.
"""
from __future__ import annotations

import logging

import pytest

pytest.importorskip("airflow.providers.google.cloud.hooks.gcs")

from airflow.providers.google.cloud.hooks.gcs import GCSHook

from airflow_export_to_object_store.uploaders import GCSUploader, resolve_uploader

LOG = logging.getLogger("test-gcs-upload")


class _FakeGCSHook(GCSHook):
    """Subclass of the real GCSHook so isinstance() matches.

    We bypass __init__ (no Airflow connection lookup needed) and capture
    method calls instead of hitting the network.
    """

    def __init__(self):  # pragma: no cover - trivial
        self.uploads = []
        self.deletes = []
        self._client = None
        self._fail_upload = False
        self._fail_delete = False

    def upload(self, bucket_name, object_name, filename=None, data=None, **kwargs):  # type: ignore[override]
        if self._fail_upload:
            raise RuntimeError("forced upload failure")
        self.uploads.append({"bucket": bucket_name, "object": object_name, "filename": filename, "data": data})

    def delete(self, bucket_name, object_name, **kwargs):  # type: ignore[override]
        if self._fail_delete:
            raise RuntimeError("forced delete failure")
        self.deletes.append({"bucket": bucket_name, "object": object_name})

    def get_conn(self):  # type: ignore[override]
        # Returned client is only used in the read-only fallback in health_check.
        return self._client


def test_resolver_picks_gcs_for_gcs_hook():
    assert resolve_uploader(_FakeGCSHook()).name == "gcs"


def test_network_targets_known():
    assert GCSUploader().network_targets() == [("storage.googleapis.com", 443)]


def test_upload_round_trip(tmp_path):
    local = tmp_path / "data.parquet"
    local.write_bytes(b"payload")
    hook = _FakeGCSHook()

    uri = GCSUploader().upload(
        hook,
        str(local),
        "exports/2026/data.parquet",
        container=None,
        bucket="my-bucket",
        overwrite=True,
        storage_hook_id="gcs_test",
        log=LOG,
    )

    assert uri == "gs://my-bucket/exports/2026/data.parquet"
    assert hook.uploads == [
        {
            "bucket": "my-bucket",
            "object": "exports/2026/data.parquet",
            "filename": str(local),
            "data": None,
        }
    ]


def test_upload_requires_bucket(tmp_path):
    local = tmp_path / "data.parquet"
    local.write_bytes(b"x")
    with pytest.raises(ValueError, match="bucket must be set"):
        GCSUploader().upload(
            _FakeGCSHook(),
            str(local),
            "k",
            container=None,
            bucket=None,
            overwrite=True,
            storage_hook_id="gcs_test",
            log=LOG,
        )


def test_health_check_write_path(tmp_path):
    hook = _FakeGCSHook()
    GCSUploader().health_check(hook, container=None, bucket="my-bucket", log=LOG)
    assert len(hook.uploads) == 1
    assert hook.uploads[0]["bucket"] == "my-bucket"
    assert hook.uploads[0]["object"].startswith("_healthcheck_tmp_")
    assert hook.uploads[0]["data"] == b"test"
    assert len(hook.deletes) == 1


def test_health_check_falls_through_to_failure_when_nothing_works(monkeypatch):
    hook = _FakeGCSHook()
    hook._fail_upload = True

    class _BadClient:
        def get_bucket(self, _name):
            raise RuntimeError("forbidden")

    hook._client = _BadClient()

    with pytest.raises(RuntimeError, match="GCS health check failed"):
        GCSUploader().health_check(hook, container=None, bucket="b", log=LOG)


def test_health_check_falls_back_to_get_bucket():
    hook = _FakeGCSHook()
    hook._fail_upload = True

    class _OkClient:
        called = False

        def get_bucket(self, name):
            type(self).called = True
            return object()

    hook._client = _OkClient()
    GCSUploader().health_check(hook, container=None, bucket="b", log=LOG)
    assert _OkClient.called is True


def test_health_check_requires_bucket():
    with pytest.raises(ValueError, match="bucket must be specified"):
        GCSUploader().health_check(_FakeGCSHook(), container=None, bucket=None, log=LOG)
