"""Encryption + tag plumbing through each backend.

We don't talk to real clouds — we mock the boundary each backend
crosses (boto3 ``upload_file`` for S3, ``BlobClient.upload_blob`` /
``commit_block_list`` for Azure, ``GCSHook.upload`` and the underlying
client's blob for GCS) and assert the right kwargs reach it.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

from airflow_export_to_object_store.encryption import EncryptionOptions
from airflow_export_to_object_store.uploaders import azure as azure_mod
from airflow_export_to_object_store.uploaders.azure import AzureBlobUploader
from airflow_export_to_object_store.uploaders.gcs import GCSUploader
from airflow_export_to_object_store.uploaders.s3 import _build_extra_args

LOG = logging.getLogger("test-encryption-and-tags")


# ----------------------------------------------------------------------
# S3 ExtraArgs builder (unit-level — no boto3 / moto needed)
# ----------------------------------------------------------------------
def test_s3_extra_args_empty_for_no_options():
    assert _build_extra_args(encryption=None, tags=None) == {}


def test_s3_sse_s3_only_algorithm():
    args = _build_extra_args(encryption=EncryptionOptions(sse_algorithm="AES256"), tags=None)
    assert args == {"ServerSideEncryption": "AES256"}


def test_s3_sse_kms_implies_aws_kms_algorithm():
    """Setting just kms_key_id should default the algorithm to aws:kms."""
    args = _build_extra_args(
        encryption=EncryptionOptions(kms_key_id="arn:aws:kms:us-east-1:1:key/abc"),
        tags=None,
    )
    assert args["ServerSideEncryption"] == "aws:kms"
    assert args["SSEKMSKeyId"] == "arn:aws:kms:us-east-1:1:key/abc"


def test_s3_explicit_algorithm_wins_over_implicit():
    args = _build_extra_args(
        encryption=EncryptionOptions(sse_algorithm="aws:kms", kms_key_id="k"),
        tags=None,
    )
    assert args["ServerSideEncryption"] == "aws:kms"
    assert args["SSEKMSKeyId"] == "k"


def test_s3_tags_url_encoded():
    args = _build_extra_args(encryption=None, tags={"team": "data eng", "env": "prod"})
    # AWS expects "key=val&key=val", URL-encoded
    assert args["Tagging"] in {"team=data%20eng&env=prod", "env=prod&team=data%20eng"}


# ----------------------------------------------------------------------
# Azure BlobClient kwargs
# ----------------------------------------------------------------------
def _azure_hook():
    hook = MagicMock()
    blob = MagicMock()
    container_client = MagicMock()
    container_client.get_blob_client.return_value = blob
    service = MagicMock()
    service.get_blob_client.return_value = blob
    service.get_container_client.return_value = container_client
    hook.get_conn.return_value = service
    return hook, blob


def test_azure_simple_passes_encryption_scope_and_metadata(tmp_path):
    p = tmp_path / "data.parquet"
    p.write_bytes(b"x" * 32)
    hook, blob = _azure_hook()
    AzureBlobUploader._simple(
        hook,
        str(p),
        "k.parquet",
        "c",
        True,
        LOG,
        EncryptionOptions(encryption_scope="my-scope"),
        {"team": "data"},
    )
    _args, kwargs = blob.upload_blob.call_args
    assert kwargs["overwrite"] is True
    assert kwargs["encryption_scope"] == "my-scope"
    assert kwargs["metadata"] == {"team": "data"}


def test_azure_simple_no_encryption_scope_when_unset(tmp_path):
    p = tmp_path / "data.parquet"
    p.write_bytes(b"x")
    hook, blob = _azure_hook()
    AzureBlobUploader._simple(hook, str(p), "k.parquet", "c", True, LOG, None, None)
    _args, kwargs = blob.upload_blob.call_args
    assert "encryption_scope" not in kwargs
    assert "metadata" not in kwargs


def test_azure_block_threads_encryption_scope_through_stage_and_commit(tmp_path, monkeypatch):
    """Encryption scope must reach BOTH stage_block (per-block) and
    commit_block_list (final)."""
    monkeypatch.setattr(azure_mod, "_AZURE_BLOCK_SIZE", 4)
    p = tmp_path / "big.parquet"
    p.write_bytes(b"abcdefgh")  # 2 blocks
    hook, blob = _azure_hook()
    AzureBlobUploader._block(
        hook,
        str(p),
        "k.parquet",
        "c",
        LOG,
        EncryptionOptions(encryption_scope="my-scope"),
        {"x": "y"},
    )
    # Every stage_block call carried encryption_scope.
    for call in blob.stage_block.call_args_list:
        _, kw = call
        assert kw.get("encryption_scope") == "my-scope"
    # commit_block_list got both scope and metadata.
    _, commit_kwargs = blob.commit_block_list.call_args
    assert commit_kwargs["encryption_scope"] == "my-scope"
    assert commit_kwargs["metadata"] == {"x": "y"}


# ----------------------------------------------------------------------
# GCS: hook.upload vs CMEK fast-path
# ----------------------------------------------------------------------
def test_gcs_default_path_uses_hook_upload_with_metadata(tmp_path):
    p = tmp_path / "f.parquet"
    p.write_bytes(b"x")

    hook = MagicMock()
    GCSUploader().upload(
        hook,
        str(p),
        "objects/x.parquet",
        container=None,
        bucket="b",
        overwrite=True,
        storage_hook_id="g",
        log=LOG,
        encryption=None,
        tags={"env": "prod"},
    )
    hook.upload.assert_called_once()
    _args, kwargs = hook.upload.call_args
    assert kwargs["bucket_name"] == "b"
    assert kwargs["object_name"] == "objects/x.parquet"
    assert kwargs["filename"] == str(p)
    assert kwargs["metadata"] == {"env": "prod"}


def test_gcs_kms_key_uses_blob_client_path(tmp_path):
    """When kms_key_name is set GCSHook.upload doesn't expose it; we must
    drop down to bucket.blob(name, kms_key_name=...).upload_from_filename."""
    p = tmp_path / "f.parquet"
    p.write_bytes(b"x")

    hook = MagicMock()
    blob = MagicMock()
    bucket_obj = MagicMock()
    bucket_obj.blob.return_value = blob
    client = MagicMock()
    client.bucket.return_value = bucket_obj
    hook.get_conn.return_value = client

    GCSUploader().upload(
        hook,
        str(p),
        "objects/x.parquet",
        container=None,
        bucket="b",
        overwrite=True,
        storage_hook_id="g",
        log=LOG,
        encryption=EncryptionOptions(kms_key_name="projects/p/locations/l/keyRings/r/cryptoKeys/k"),
        tags={"env": "prod"},
    )
    hook.upload.assert_not_called()
    bucket_obj.blob.assert_called_once_with(
        "objects/x.parquet", kms_key_name="projects/p/locations/l/keyRings/r/cryptoKeys/k"
    )
    assert blob.metadata == {"env": "prod"}
    blob.upload_from_filename.assert_called_once_with(str(p))


# ----------------------------------------------------------------------
# Frozen options sanity
# ----------------------------------------------------------------------
def test_encryption_options_are_frozen():
    import dataclasses

    e = EncryptionOptions(sse_algorithm="AES256")
    with pytest.raises(dataclasses.FrozenInstanceError):
        e.sse_algorithm = "aws:kms"  # type: ignore[misc]
