# Uploader protocol

`Uploader` is a `typing.Protocol` that defines the interface every storage backend must implement. Built-in backends (S3, Azure Blob Storage, GCS) are registered automatically. Custom backends are registered via Python entry points.

## Protocol definition

```python
@runtime_checkable
class Uploader(Protocol):
    name: str

    def matches(self, storage_hook: Any) -> bool: ...
    def network_targets(self) -> Sequence[tuple[str, int]]: ...
    def health_check(self, storage_hook, *, container, bucket, log) -> None: ...
    def upload(self, storage_hook, local_path, remote_path, *, container, bucket,
               overwrite, storage_hook_id, log, encryption=None, tags=None) -> str: ...
    def exists(self, storage_hook, *, container, bucket, remote_path) -> bool: ...
```

## Methods

### `name: str`

A short identifier for the backend, e.g. `"s3"`, `"azure"`, `"gcs"`. Used in log messages and error text. Must be unique within the registry.

### `matches(storage_hook) -> bool`

Returns `True` if this uploader can handle the given Airflow hook instance. The operator iterates the registry and calls `matches` on each uploader; the first match wins.

| Parameter | Type | Description |
|-----------|------|-------------|
| `storage_hook` | `Any` | The Airflow hook object, e.g. an `S3Hook` instance. |

### `network_targets() -> Sequence[tuple[str, int]]`

Returns a list of `(host, port)` tuples the uploader will connect to. Used for pre-flight network reachability checks. Return an empty list if no pre-flight check is needed.

### `health_check(storage_hook, *, container, bucket, log) -> None`

Validates that the destination is reachable and the credentials have write access. Raises an exception if the check fails. Called once before shard processing begins.

| Parameter | Type | Description |
|-----------|------|-------------|
| `storage_hook` | `Any` | Airflow hook instance. |
| `container` | `str \| None` | Azure container name. `None` for S3/GCS. |
| `bucket` | `str \| None` | S3 or GCS bucket name. `None` for Azure. |
| `log` | `logging.Logger` | Airflow task logger. |

### `upload(storage_hook, local_path, remote_path, *, container, bucket, overwrite, storage_hook_id, log, encryption=None, tags=None) -> str`

Uploads a local file to the object store. Returns the remote URI string (e.g. `"s3://bucket/path/file.parquet"`).

| Parameter | Type | Description |
|-----------|------|-------------|
| `storage_hook` | `Any` | Airflow hook instance. |
| `local_path` | `str` | Absolute path to the local Parquet file. |
| `remote_path` | `str` | Destination object path (key), relative to the bucket/container. |
| `container` | `str \| None` | Azure container name. |
| `bucket` | `str \| None` | S3 or GCS bucket name. |
| `overwrite` | `bool` | Whether to overwrite an existing object. |
| `storage_hook_id` | `str` | Airflow connection ID (for log messages). |
| `log` | `logging.Logger` | Airflow task logger. |
| `encryption` | `EncryptionOptions \| None` | Server-side encryption settings. |
| `tags` | `dict[str, str] \| None` | Object tags to apply. |

### `exists(storage_hook, *, container, bucket, remote_path) -> bool`

Returns `True` if the object at `remote_path` already exists. Called when `skip_if_exists=True` on the operator.

| Parameter | Type | Description |
|-----------|------|-------------|
| `storage_hook` | `Any` | Airflow hook instance. |
| `container` | `str \| None` | Azure container name. |
| `bucket` | `str \| None` | S3 or GCS bucket name. |
| `remote_path` | `str` | Object path to check. |

## Plugin registration

Register a custom uploader via the `airflow_export_to_object_store.uploaders` entry point group. The entry point must resolve to a zero-argument callable that returns an `Uploader` instance.

Built-in backends are registered first. Plugins cannot shadow built-in backends — if a plugin's `matches()` would return `True` for the same hook type as a built-in, the built-in is used.

### `pyproject.toml` entry

```toml
[project.entry-points."airflow_export_to_object_store.uploaders"]
my_backend = "my_package.uploaders:create_my_uploader"
```

### `setup.cfg` entry

```ini
[options.entry_points]
airflow_export_to_object_store.uploaders =
    my_backend = my_package.uploaders:create_my_uploader
```

## Minimal plugin skeleton

```python
# my_package/uploaders.py
from __future__ import annotations
from typing import Any, Sequence


class MyStorageUploader:
    name = "my_storage"

    def matches(self, storage_hook: Any) -> bool:
        # Return True for the hook type this uploader handles.
        return type(storage_hook).__name__ == "MyStorageHook"

    def network_targets(self) -> Sequence[tuple[str, int]]:
        return [("my-storage.example.com", 443)]

    def health_check(self, storage_hook, *, container, bucket, log) -> None:
        # Raise an exception if the destination is unreachable or credentials are invalid.
        storage_hook.get_conn().stat_bucket(bucket)

    def upload(
        self,
        storage_hook,
        local_path,
        remote_path,
        *,
        container,
        bucket,
        overwrite,
        storage_hook_id,
        log,
        encryption=None,
        tags=None,
    ) -> str:
        log.info("Uploading %s → %s/%s", local_path, bucket, remote_path)
        storage_hook.get_conn().upload(
            bucket=bucket,
            key=remote_path,
            filename=local_path,
            overwrite=overwrite,
        )
        return f"my-storage://{bucket}/{remote_path}"

    def exists(self, storage_hook, *, container, bucket, remote_path) -> bool:
        return storage_hook.get_conn().object_exists(bucket=bucket, key=remote_path)


def create_my_uploader() -> MyStorageUploader:
    return MyStorageUploader()
```
