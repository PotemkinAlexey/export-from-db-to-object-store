"""Uploader protocol and backend resolution."""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from typing import Any, Protocol, runtime_checkable

from ..encryption import EncryptionOptions


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
        encryption: EncryptionOptions | None = None,
        tags: Mapping[str, str] | None = None,
    ) -> str:
        """Upload ``local_path`` and return a canonical URI (``s3://...``/``azure://...``).

        ``encryption`` and ``tags`` are optional; uploaders should pick the
        fields they understand and silently ignore the rest. Plugin
        implementations written before v1.2 should accept these as keyword-
        only arguments with default ``None`` to remain forward-compatible."""

    def exists(
        self,
        storage_hook: Any,
        *,
        container: str | None,
        bucket: str | None,
        remote_path: str,
    ) -> bool:
        """Return True iff an object already exists at ``remote_path``.

        Used by ``skip_if_exists`` to make re-runs idempotent. Implementations
        should treat any auth/network error as ``False`` (caller will then
        attempt the upload, which surfaces the real error)."""


def _builtin_uploaders() -> list[Uploader]:
    """Built-in backends, lazily imported so missing providers don't fail load."""
    from .azure import AzureBlobUploader
    from .gcs import GCSUploader
    from .s3 import S3Uploader

    return [AzureBlobUploader(), S3Uploader(), GCSUploader()]


def _entry_point_uploaders() -> list[Uploader]:
    """Discover third-party uploaders registered via the
    ``airflow_export_to_object_store.uploaders`` entry point group.

    Each entry point must resolve to a zero-arg callable returning an
    :class:`Uploader` instance, or to an ``Uploader`` class that the
    registry can call with no arguments. Bad entry points are logged
    and skipped — they must never break the operator.
    """
    log = logging.getLogger(__name__)
    found: list[Uploader] = []
    try:
        from importlib.metadata import entry_points

        # Python 3.10+ accepts ``group=`` kwarg; 3.9 returns dict-like obj.
        try:
            eps = entry_points(group="airflow_export_to_object_store.uploaders")
        except TypeError:  # pragma: no cover - 3.9 fallback
            eps = entry_points().get("airflow_export_to_object_store.uploaders", [])
    except Exception as e:  # pragma: no cover - defensive
        log.debug("Entry-point discovery unavailable: %s", e)
        return found

    for ep in eps:
        try:
            obj = ep.load()
            instance = obj() if callable(obj) else obj
            if isinstance(instance, Uploader):
                found.append(instance)
            else:
                log.warning("Entry point %s did not produce an Uploader (got %r)", ep.name, type(instance))
        except Exception as e:
            log.warning("Skipping bad uploader entry point %s: %s", ep.name, e)
    return found


def get_registry() -> list[Uploader]:
    """Return all known uploaders.

    Order = priority. Built-in backends come first; third-party plugins
    discovered via entry points come after — so a plugin can extend the
    registry but cannot silently shadow a built-in by accident.
    """
    return _builtin_uploaders() + _entry_point_uploaders()


def resolve_uploader(storage_hook: Any, registry: Sequence[Uploader] | None = None) -> Uploader:
    """Find the uploader that matches ``storage_hook`` or raise NotImplementedError."""
    for u in registry or get_registry():
        if u.matches(storage_hook):
            return u
    raise NotImplementedError(f"Unsupported storage hook: {type(storage_hook)}")
