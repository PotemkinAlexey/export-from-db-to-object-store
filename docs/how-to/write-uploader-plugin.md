# How-to: Write a custom storage backend plugin

Add support for a storage backend that the operator does not ship with
— a private object store, a mock filesystem for testing, or a wrapper
around an existing cloud provider with custom behaviour.

## The Uploader protocol

Your implementation must satisfy the `Uploader` protocol defined in
`airflow_export_to_object_store.uploaders.base`:

```python
from collections.abc import Mapping, Sequence
from typing import Any
import logging

from airflow_export_to_object_store.uploaders.base import Uploader
from airflow_export_to_object_store.encryption import EncryptionOptions


class MyUploader:
    name = "my-backend"   # used in log messages

    def matches(self, storage_hook: Any) -> bool:
        """Return True if this uploader can handle the given hook."""
        ...

    def network_targets(self) -> Sequence[tuple[str, int]]:
        """Hosts and ports the operator probes before starting a shard.
        Return an empty list to skip the network probe."""
        ...

    def health_check(
        self,
        storage_hook: Any,
        *,
        container: str | None,
        bucket: str | None,
        log: logging.Logger,
    ) -> None:
        """Raise on auth or permission errors; log warnings for soft issues."""
        ...

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
        """Upload local_path; return a canonical URI."""
        ...

    def exists(
        self,
        storage_hook: Any,
        *,
        container: str | None,
        bucket: str | None,
        remote_path: str,
    ) -> bool:
        """Return True if the object already exists."""
        ...
```

All five methods are required. `encryption` and `tags` in `upload` must
be keyword-only with `None` defaults — older callers may not pass them,
and new callers depend on the defaults being safe to ignore.

## A minimal concrete example: local filesystem uploader

This is the kind of uploader you would write for unit tests or a
development environment where you want files written to a local
directory instead of a cloud bucket.

```python
# src/my_package/local_uploader.py
from __future__ import annotations

import logging
import shutil
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from airflow_export_to_object_store.encryption import EncryptionOptions


class LocalFSHook:
    """Minimal Airflow-hook-like object for the local backend."""
    def __init__(self, base_dir: str):
        self.base_dir = Path(base_dir)


class LocalFSUploader:
    name = "local-fs"

    def matches(self, storage_hook: Any) -> bool:
        return isinstance(storage_hook, LocalFSHook)

    def network_targets(self) -> Sequence[tuple[str, int]]:
        return []   # local — no network probes

    def health_check(
        self,
        storage_hook: Any,
        *,
        container: str | None,
        bucket: str | None,
        log: logging.Logger,
    ) -> None:
        base = storage_hook.base_dir
        if not base.exists():
            raise FileNotFoundError(f"LocalFSUploader: base_dir {base} does not exist")
        log.info("LocalFSUploader: base_dir %s is reachable", base)

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
        dest = storage_hook.base_dir / remote_path.lstrip("/")
        dest.parent.mkdir(parents=True, exist_ok=True)
        if dest.exists() and not overwrite:
            raise FileExistsError(f"LocalFSUploader: {dest} already exists and overwrite=False")
        shutil.copy2(local_path, dest)
        log.info("LocalFSUploader: wrote %s → %s", local_path, dest)
        return f"file://{dest}"

    def exists(
        self,
        storage_hook: Any,
        *,
        container: str | None,
        bucket: str | None,
        remote_path: str,
    ) -> bool:
        dest = storage_hook.base_dir / remote_path.lstrip("/")
        return dest.exists()
```

## Registering the plugin via entry points

In `pyproject.toml` (or `setup.cfg`):

```toml
[project.entry-points."airflow_export_to_object_store.uploaders"]
local-fs = "my_package.local_uploader:LocalFSUploader"
```

The value is a dotted import path to either:

- An `Uploader` class (the registry calls it with no arguments to get
  an instance), or
- A zero-argument factory function that returns an `Uploader` instance.

After installing your package (`pip install -e .`), the registry picks
it up automatically.

## How the registry works

`get_registry()` returns built-in uploaders first, then plugin uploaders
discovered via entry points:

```
[AzureBlobUploader, S3Uploader, GCSUploader, ...your plugins...]
```

`resolve_uploader(hook)` iterates the list and returns the first
uploader whose `matches(hook)` returns `True`. Because built-ins come
first, a plugin cannot silently shadow a built-in backend — if your hook
type already matches S3Uploader, the plugin will never be reached for
that hook.

If no uploader matches, `resolve_uploader` raises `NotImplementedError`.

## Testing the plugin

```python
# tests/test_local_uploader.py
import tempfile, pathlib
from my_package.local_uploader import LocalFSHook, LocalFSUploader
from airflow_export_to_object_store.uploaders.base import resolve_uploader


def test_resolve():
    hook = LocalFSHook("/tmp/test-export")
    uploader = resolve_uploader(hook)
    assert isinstance(uploader, LocalFSUploader)


def test_upload_and_exists():
    with tempfile.TemporaryDirectory() as base:
        hook = LocalFSHook(base)
        uploader = LocalFSUploader()

        src = pathlib.Path(base) / "input.parquet"
        src.write_bytes(b"PAR1\x00" * 10)

        uri = uploader.upload(
            hook,
            str(src),
            "exports/test.parquet",
            container=None,
            bucket=None,
            overwrite=True,
            storage_hook_id="local_default",
            log=__import__("logging").getLogger(__name__),
        )
        assert uri.startswith("file://")
        assert uploader.exists(hook, container=None, bucket=None, remote_path="exports/test.parquet")
```

Run with:

```bash
pytest tests/test_local_uploader.py -v
```

## Forward-compatibility notes

- Always accept `encryption` and `tags` as keyword-only parameters with
  `None` defaults in `upload`. The operator passes them on every call in
  v1.2+; older callers may not.
- Do not raise if `encryption` or `tags` contains fields your backend
  does not support — log a debug message and ignore the field. New
  options may be added to those dataclasses in future minor versions.
- The `Uploader` protocol is decorated `@runtime_checkable`, so
  `isinstance(my_uploader, Uploader)` works for registry validation.

## See also

- [Reference → Uploader protocol](../reference/uploader-protocol.md).
- Source: `src/airflow_export_to_object_store/uploaders/base.py`.
- Built-in examples: `uploaders/s3.py`, `uploaders/gcs.py`,
  `uploaders/azure.py` in the same package.
