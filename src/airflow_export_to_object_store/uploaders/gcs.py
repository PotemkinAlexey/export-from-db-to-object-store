"""Google Cloud Storage uploader.

Uses :class:`airflow.providers.google.cloud.hooks.gcs.GCSHook` for both the
health probe (cheap object write/delete with a fallback to ``get_bucket``)
and the actual upload (the hook's ``upload()`` method already performs
resumable multipart uploads for files larger than ~8 MiB and handles auth).
"""
from __future__ import annotations

import logging
import uuid
from collections.abc import Sequence
from typing import Any

try:
    from airflow.providers.google.cloud.hooks.gcs import GCSHook
except ImportError:
    GCSHook = None  # type: ignore[assignment]


class GCSUploader:
    name = "gcs"

    def matches(self, storage_hook: Any) -> bool:
        return GCSHook is not None and isinstance(storage_hook, GCSHook)

    def network_targets(self) -> Sequence[tuple[str, int]]:
        return [("storage.googleapis.com", 443)]

    def health_check(
        self,
        storage_hook: Any,
        *,
        container: str | None,
        bucket: str | None,
        log: logging.Logger,
    ) -> None:
        if not bucket:
            raise ValueError("bucket must be specified for GCS storage")

        probe_object = f"_healthcheck_tmp_{uuid.uuid4().hex}"

        # Prefer write/delete (proves both permissions and reachability),
        # then fall back to a read on bucket metadata via the underlying client.
        try:
            storage_hook.upload(bucket_name=bucket, object_name=probe_object, data=b"test")
            try:
                storage_hook.delete(bucket_name=bucket, object_name=probe_object)
            except Exception:
                # Best-effort cleanup — write succeeded, that's enough.
                log.debug("GCS health probe cleanup failed for %s", probe_object)
            log.info("GCS health check OK ✓ (write/delete allowed)")
            return
        except Exception:
            pass

        try:
            client = storage_hook.get_conn()
            client.get_bucket(bucket)
            log.info("GCS health check OK ✓ (read-only bucket metadata)")
            return
        except Exception:
            pass

        raise RuntimeError("GCS health check failed: neither write nor bucket-read allowed")

    def exists(
        self,
        storage_hook: Any,
        *,
        container: str | None,
        bucket: str | None,
        remote_path: str,
    ) -> bool:
        if not bucket:
            return False
        try:
            return bool(storage_hook.exists(bucket_name=bucket, object_name=remote_path))
        except Exception:
            return False

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
        if not bucket:
            raise ValueError("bucket must be set for GCS uploads")

        log.info("GCS upload: %s → gs://%s/%s", local_path, bucket, remote_path)

        # GCSHook.upload supports gzip, mime_type, and multipart resumable upload
        # internally; we keep the call minimal and let the hook decide.
        storage_hook.upload(
            bucket_name=bucket,
            object_name=remote_path,
            filename=local_path,
        )
        return f"gs://{bucket}/{remote_path}"
