"""Unit tests for AzureBlobUploader's simple and block upload paths."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, call

import pytest

from airflow_export_to_object_store.uploaders import azure as azure_mod
from airflow_export_to_object_store.uploaders.azure import AzureBlobUploader

LOG = logging.getLogger("test-azure-upload")


def _make_hook():
    """Build a fake WasbHook whose get_conn() returns a mocked BlobServiceClient."""
    hook = MagicMock()
    blob = MagicMock()
    container_client = MagicMock()
    container_client.get_blob_client.return_value = blob
    service = MagicMock()
    service.get_blob_client.return_value = blob
    service.get_container_client.return_value = container_client
    hook.get_conn.return_value = service
    return hook, service, blob


def test_simple_upload_calls_blob_client(tmp_path):
    payload = b"payload" * 64
    local = tmp_path / "data.parquet"
    local.write_bytes(payload)
    hook, service, blob = _make_hook()

    uri = AzureBlobUploader._simple(hook, str(local), "exports/data.parquet", "my-container", True, LOG)

    assert uri == "azure://my-container/exports/data.parquet"
    service.get_blob_client.assert_called_once_with(container="my-container", blob="exports/data.parquet")
    assert blob.upload_blob.called
    args, kwargs = blob.upload_blob.call_args
    assert kwargs["overwrite"] is True


def test_block_upload_splits_file_and_commits(tmp_path, monkeypatch):
    # Force a tiny block size so we actually exercise multi-block upload.
    monkeypatch.setattr(azure_mod, "_AZURE_BLOCK_SIZE", 4)

    local = tmp_path / "big.parquet"
    local.write_bytes(b"abcdefghij")  # 10 bytes -> 3 blocks of 4/4/2
    hook, service, blob = _make_hook()

    uri = AzureBlobUploader._block(hook, str(local), "exports/big.parquet", "c", LOG)

    assert uri == "azure://c/exports/big.parquet"
    # Three stage_block calls with sequential ids and matching chunk bytes.
    assert blob.stage_block.call_args_list == [
        call(block_id="00000000", data=b"abcd"),
        call(block_id="00000001", data=b"efgh"),
        call(block_id="00000002", data=b"ij"),
    ]
    blob.commit_block_list.assert_called_once_with(["00000000", "00000001", "00000002"])


def test_upload_routes_small_to_simple_and_large_to_block(tmp_path, monkeypatch):
    """The size threshold decides which internal path runs."""
    local = tmp_path / "f.parquet"
    local.write_bytes(b"x" * 1024)
    hook, _service, _blob = _make_hook()

    simple_called = []
    block_called = []

    monkeypatch.setattr(
        AzureBlobUploader,
        "_simple",
        staticmethod(lambda *a, **kw: simple_called.append(True) or "azure://x/y"),
    )
    monkeypatch.setattr(
        AzureBlobUploader,
        "_block",
        staticmethod(lambda *a, **kw: block_called.append(True) or "azure://x/y"),
    )

    AzureBlobUploader().upload(
        hook,
        str(local),
        "y",
        container="x",
        bucket=None,
        overwrite=True,
        storage_hook_id="azure_test",
        log=LOG,
    )
    assert simple_called and not block_called

    simple_called.clear()
    block_called.clear()

    # Pretend the file is huge so the block path is selected.
    monkeypatch.setattr(azure_mod.os.path, "getsize", lambda _p: 6 * 1024**3)
    AzureBlobUploader().upload(
        hook,
        str(local),
        "y",
        container="x",
        bucket=None,
        overwrite=True,
        storage_hook_id="azure_test",
        log=LOG,
    )
    assert block_called and not simple_called


def test_upload_requires_container(tmp_path):
    local = tmp_path / "f.parquet"
    local.write_bytes(b"x")
    hook, _service, _blob = _make_hook()

    with pytest.raises(ValueError, match="Container must be set"):
        AzureBlobUploader().upload(
            hook,
            str(local),
            "y",
            container=None,
            bucket=None,
            overwrite=True,
            storage_hook_id="azure_test",
            log=LOG,
        )
