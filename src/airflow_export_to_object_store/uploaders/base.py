"""Uploader protocol and backend resolution."""
from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class Uploader(Protocol):
    """Strategy for uploading a local file to a cloud object store.

    Each implementation knows how to recognise its own Airflow hook type,
    perform a connectivity health check, run a network reachability probe,
    and execute the actual upload, returning a canonical URI for XCom.
    """

    name: str

    def matches(self, storage_hook: Any) -> bool:
        """True if this uploader can handle the given Airflow hook instance."""

    def network_targets(self) -> Sequence[tuple[str, int]]:
        """Hosts/ports to probe in :func:`network_health_check`. Empty = no probes."""

    def health_check(
        self,
        storage_hook: Any,
        *,
        container: str | None,
        bucket: str | None,
        log: logging.Logger,
    ) -> None:
        """Validate that credentials/permissions allow either reading or writing."""

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
        """Upload ``local_path`` and return a canonical URI (``s3://...``/``azure://...``)."""


def get_registry() -> list[Uploader]:
    """Return all built-in uploaders. Order = priority."""
    # Imported lazily so that missing optional providers don't fail module load.
    from .azure import AzureBlobUploader
    from .gcs import GCSUploader
    from .s3 import S3Uploader

    return [AzureBlobUploader(), S3Uploader(), GCSUploader()]


def resolve_uploader(storage_hook: Any, registry: Sequence[Uploader] | None = None) -> Uploader:
    """Find the uploader that matches ``storage_hook`` or raise NotImplementedError."""
    for u in registry or get_registry():
        if u.matches(storage_hook):
            return u
    raise NotImplementedError(f"Unsupported storage hook: {type(storage_hook)}")
