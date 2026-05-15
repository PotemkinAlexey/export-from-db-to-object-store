# UnloadStrategy protocol

`UnloadStrategy` is a `typing.Protocol` that defines the interface for native database unload backends. When an `unload_strategy` is passed to `StreamingExportOperator`, the operator delegates the entire export to the strategy instead of streaming rows through the worker.

## Protocol definition

```python
@runtime_checkable
class UnloadStrategy(Protocol):
    name: str

    def matches(self, db_hook: Any, storage_hook: Any) -> bool: ...
    def unload(self, *, db_hook, storage_hook, sql, remote_dir,
               container, bucket, log) -> list[ShardResult]: ...
```

## Methods

### `name: str`

A short identifier for the strategy, e.g. `"snowflake"`, `"bigquery"`, `"redshift"`. Used in log messages and set as the `mode` key in the XCom result dict.

### `matches(db_hook, storage_hook) -> bool`

Returns `True` if this strategy supports the given hook pair. The operator calls `matches` to validate that the user-supplied `unload_strategy` is compatible with the configured hooks. A mismatch raises `ValueError("unload_strategy ... does not match ...")`.

| Parameter | Type | Description |
|-----------|------|-------------|
| `db_hook` | `Any` | The Airflow database hook instance, e.g. `SnowflakeHook`. |
| `storage_hook` | `Any` | The Airflow storage hook instance, e.g. `S3Hook`. |

### `unload(*, db_hook, storage_hook, sql, remote_dir, container, bucket, log) -> list[ShardResult]`

Executes the native unload command and returns a list of `ShardResult` objects describing each output file. The operator uses these results to build the XCom dict and the manifest (if `write_manifest=True`).

| Parameter | Type | Description |
|-----------|------|-------------|
| `db_hook` | `Any` | Airflow database hook instance. |
| `storage_hook` | `Any` | Airflow storage hook instance. |
| `sql` | `str` | The fully rendered SQL query to unload. |
| `remote_dir` | `str` | Remote path prefix where output files should be written. Rendered from `unload_dir_template`. |
| `container` | `str \| None` | Azure container name. `None` for S3/GCS. |
| `bucket` | `str \| None` | S3 or GCS bucket name. `None` for Azure. |
| `log` | `logging.Logger` | Airflow task logger. |

The returned `list[ShardResult]` may have `rows=0` if the native unload command does not report per-file row counts (this is the case for BigQuery and Redshift).

## When the operator calls the strategy

1. The operator validates that `unload_strategy.matches(db_hook, storage_hook)` returns `True`.
2. It renders `sql_template` (or uses `query`) to produce the final SQL string.
3. It renders `unload_dir_template` to produce `remote_dir`.
4. It calls `unload_strategy.unload(db_hook=..., storage_hook=..., sql=..., remote_dir=..., ...)`.
5. The returned `list[ShardResult]` is merged into the XCom output.
6. If `write_manifest=True`, the operator writes a manifest using the returned shard results.

The operator does **not** stream any rows, write local Parquet files, or call the uploader when an unload strategy is active.

## Minimal custom strategy skeleton

```python
# my_package/strategies.py
from __future__ import annotations
from typing import Any
from airflow_export_to_object_store.options import ShardResult


class MyUnloadStrategy:
    name = "my_db"

    def matches(self, db_hook: Any, storage_hook: Any) -> bool:
        return (
            type(db_hook).__name__ == "MyDBHook"
            and type(storage_hook).__name__ == "S3Hook"
        )

    def unload(
        self,
        *,
        db_hook: Any,
        storage_hook: Any,
        sql: str,
        remote_dir: str,
        container: str | None,
        bucket: str | None,
        log,
    ) -> list[ShardResult]:
        import time

        s3_uri = f"s3://{bucket}/{remote_dir}"
        log.info("Unloading to %s", s3_uri)

        t0 = time.monotonic()
        # Execute the native unload command via db_hook.
        db_hook.get_conn().execute(
            f"UNLOAD ('{sql}') TO '{s3_uri}' ..."
        )
        elapsed = time.monotonic() - t0

        # Return one ShardResult per output file.
        # If the command does not report per-file details, return a single placeholder.
        return [
            ShardResult(
                shard_index=0,
                remote_uri=f"{s3_uri}part_000.parquet",
                rows=0,       # unknown
                bytes=0,      # unknown
                md5=None,
                elapsed_s=elapsed,
            )
        ]
```
