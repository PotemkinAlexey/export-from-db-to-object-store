"""Azure Blob Storage uploader (simple + block-list paths)."""
from __future__ import annotations

import logging
import mimetypes
import os
import uuid
from collections.abc import Sequence
from typing import Any

try:
    from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
except ImportError:
    WasbHook = None  # type: ignore[assignment]

try:
    from azure.storage.blob import ContentSettings
except (ImportError, ModuleNotFoundError):
    ContentSettings = None  # type: ignore[assignment]


# Azure: simple upload supports up to 5 GiB; above that use block list.
_AZURE_SIMPLE_LIMIT = 5 * 1024**3
_AZURE_BLOCK_SIZE = 100 * 1024 * 1024  # 100 MiB per block


class AzureBlobUploader:
    name = "azure"

    def matches(self, storage_hook: Any) -> bool:
        return WasbHook is not None and isinstance(storage_hook, WasbHook)

    def network_targets(self) -> Sequence[tuple[str, int]]:
        return [("blob.core.windows.net", 443)]

    def health_check(
        self,
        storage_hook: Any,
        *,
        container: str | None,
        bucket: str | None,
        log: logging.Logger,
    ) -> None:
        if not container:
            raise ValueError("Container must be specified for Azure storage")

        client = storage_hook.get_conn()
        container_client = client.get_container_client(container)
        test_blob = container_client.get_blob_client(f"_healthcheck_tmp_{uuid.uuid4().hex}")

        # Try write, then read, then container metadata — first success wins.
        try:
            test_blob.upload_blob(b"test", overwrite=True)
            test_blob.delete_blob()
            log.info("Azure health check OK ✓ (write/delete allowed)")
            return
        except Exception:
            pass

        try:
            next(container_client.list_blobs(results_per_page=1), None)
            log.info("Azure health check OK ✓ (read-only allowed)")
            return
        except Exception:
            pass

        try:
            container_client.get_container_properties()
            log.info("Azure health check OK ✓ (write-only SAS)")
            return
        except Exception:
            pass

        raise RuntimeError("Azure health check failed: neither read nor write allowed")

    def upload(
        self,
        storage_hook: Any,
        local_path: str,
        remote_path: str,
        *,
        container: str | None,
        bucket: str | None,
        overwrite: bool,
        storage_hook_id: str,
        log: logging.Logger,
    ) -> str:
        if not container:
            raise ValueError("Container must be set for Azure uploads")

        size = os.path.getsize(local_path)
        if size < _AZURE_SIMPLE_LIMIT:
            return self._simple(storage_hook, local_path, remote_path, container, overwrite, log)
        log.info("Azure large file detected (%.1f GB) → block upload", size / 1024**3)
        return self._block(storage_hook, local_path, remote_path, container, log)

    @staticmethod
    def _simple(storage_hook, local_path, remote_path, container, overwrite, log):
        log.info("Azure simple upload: %s → %s", local_path, remote_path)
        client = storage_hook.get_conn()
        blob = client.get_blob_client(container=container, blob=remote_path)
        content_type = mimetypes.guess_type(local_path)[0] or "application/octet-stream"
        cs = ContentSettings(content_type=content_type) if ContentSettings else None
        with open(local_path, "rb") as f:
            if cs:
                blob.upload_blob(f, overwrite=overwrite, content_settings=cs)
            else:
                blob.upload_blob(f, overwrite=overwrite)
        return f"azure://{container}/{remote_path}"

    @staticmethod
    def _block(storage_hook, local_path, remote_path, container, log):
        log.info("Azure block upload for large file: %s", local_path)
        client = storage_hook.get_conn()
        blob = client.get_blob_client(container=container, blob=remote_path)
        blocks = []
        with open(local_path, "rb") as f:
            idx = 0
            while True:
                chunk = f.read(_AZURE_BLOCK_SIZE)
                if not chunk:
                    break
                block_id = f"{idx:08d}"
                blocks.append(block_id)
                log.info("Azure uploading block %s (%d MB)", block_id, len(chunk) / (1024**2))
                blob.stage_block(block_id=block_id, data=chunk)
                idx += 1
        blob.commit_block_list(blocks)
        return f"azure://{container}/{remote_path}"
