"""Integration test: real S3Uploader.upload against moto's in-process S3."""

from __future__ import annotations

import logging

import boto3
import pytest

pytest.importorskip("moto")

from airflow_export_to_object_store.uploaders.s3 import S3Uploader  # noqa: E402

LOG = logging.getLogger("test-s3-integration")


class _FakeAwsHook:
    """Hook the S3Uploader recognises via the AWS-name fallback path."""


@pytest.fixture
def aws_env(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture
def fake_connection(monkeypatch):
    class _Conn:
        login = "test"
        password = "test"

        @property
        def extra_dejson(self):
            return {"region_name": "us-east-1"}

    monkeypatch.setattr(
        "airflow_export_to_object_store.uploaders.s3.BaseHook.get_connection",
        lambda _conn_id: _Conn(),
    )


def test_s3_upload_round_trip(tmp_path, aws_env, fake_connection):
    from moto import mock_aws

    payload = b"hello-from-export-operator" * 100
    local = tmp_path / "data.parquet"
    local.write_bytes(payload)

    bucket = "test-bucket"
    key = "exports/2026-05-08/data.parquet"

    with mock_aws():
        # Create the bucket inside the mock so the upload has somewhere to land.
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=bucket)

        uri = S3Uploader().upload(
            _FakeAwsHook(),
            str(local),
            key,
            container=None,
            bucket=bucket,
            overwrite=True,
            storage_hook_id="aws_test",
            log=LOG,
        )

        assert uri == f"s3://{bucket}/{key}"

        # Verify the object actually landed and matches what we sent.
        body = boto3.client("s3", region_name="us-east-1").get_object(Bucket=bucket, Key=key)["Body"].read()
        assert body == payload


def test_s3_upload_requires_bucket(tmp_path, aws_env, fake_connection):
    from moto import mock_aws

    local = tmp_path / "data.parquet"
    local.write_bytes(b"x")

    with mock_aws(), pytest.raises(ValueError, match="bucket must be set"):
        S3Uploader().upload(
            _FakeAwsHook(),
            str(local),
            "k",
            container=None,
            bucket=None,
            overwrite=True,
            storage_hook_id="aws_test",
            log=LOG,
        )
