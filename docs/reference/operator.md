# StreamingExportOperator

Streams rows from a database to an object store in Parquet format, one shard at a time.

## Minimal example

```python
from airflow_export_to_object_store import StreamingExportOperator

export = StreamingExportOperator(
    task_id="export_orders",
    db_hook_id="my_postgres",
    storage_hook_id="my_s3",
    bucket="data-lake",
    sql_template="SELECT * FROM orders WHERE created_at >= '{{ ds }}'",
    remote_path_template="{{ ds }}/orders_{{ '%03d' | format(shard_index) }}.parquet",
)
```

## Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `task_id` | `str` | — | Airflow task ID (required). |
| `db_hook_id` | `str` | — | Airflow connection ID for the source database. |
| `storage_hook_id` | `str` | — | Airflow connection ID for the destination object store. |
| `query` | `str \| None` | `None` | Static SQL query. Mutually exclusive with `sql_template`. Exactly one must be provided. |
| `sql_template` | `str \| None` | `None` | Jinja-templated SQL query. Mutually exclusive with `query`. Exactly one must be provided. |
| `sql_params` | `dict[str, Any] \| None` | `None` | Named parameters passed to the DB driver when executing `query`. Not used with `sql_template`. |
| `shards` | `list[dict] \| None` | `None` | Explicit shard definitions. Each dict is merged into the query context. When set, one shard is exported per dict entry. |
| `filename_template` | `str` | `"data_{{ '%03d' \| format(shard_index) }}.parquet"` | Jinja template for the local temp filename per shard. |
| `remote_path_template` | `str` | `"{{ ds }}/data_{{ '%03d' \| format(shard_index) }}.parquet"` | Jinja template for the remote object path per shard. Available variables: `ds`, `ts`, `shard_index`, and all Airflow macro context keys. |
| `container` | `str \| None` | `None` | Azure Blob Storage container name. Required when the storage hook is an Azure hook. |
| `bucket` | `str \| None` | `None` | S3 or GCS bucket name. Required when the storage hook is S3Hook or GCSHook. |
| `parquet_options` | `ParquetOptions \| None` | `None` | Parquet encoding and compression settings. See [parquet-options.md](parquet-options.md). |
| `retry_options` | `RetryOptions \| None` | `None` | Upload retry and backoff settings. See [retry-options.md](retry-options.md). |
| `shard_options` | `ShardOptions \| None` | `None` | Parallelism, chunk size, memory cap, and timeout settings. See [shard-options.md](shard-options.md). |
| `tmp_dir` | `str \| None` | `None` | Directory for temporary Parquet files. Defaults to the system temp directory. |
| `compute_md5` | `bool` | `False` | Compute an MD5 digest of each local Parquet file before upload. Stored in `ShardResult.md5` and the manifest. |
| `overwrite` | `bool` | `True` | Overwrite existing remote objects. When `False` and the object exists, behavior depends on `skip_if_exists`. |
| `log_timings` | `bool` | `True` | Log per-shard timing breakdowns (fetch, write, upload durations). |
| `validate_parquet` | `bool` | `True` | Open each written Parquet file with PyArrow after writing to verify it is readable before upload. |
| `skip_if_exists` | `bool` | `False` | Skip upload if the remote object already exists. The shard is recorded with `skipped=True` in XCom output. |
| `write_manifest` | `bool` | `False` | Write a `_manifest.json` file listing all exported shards after the run completes. |
| `manifest_path` | `str \| None` | `None` | Explicit path for the manifest file. Defaults to `<common_prefix_of_shards>/_manifest.json`. |
| `unload_strategy` | `UnloadStrategy \| None` | `None` | Use a native unload path (Snowflake, BigQuery, Redshift) instead of streaming. See unload references. |
| `unload_dir_template` | `str` | `"{{ ds }}/"` | Jinja template for the remote prefix used by the unload strategy. |
| `incremental` | `IncrementalConfig \| None` | `None` | Incremental export configuration. See [incremental-config.md](incremental-config.md). |
| `transform_fn` | `Any \| None` | `None` | Callable applied to each PyArrow `RecordBatch` before writing. Signature: `fn(batch: pa.RecordBatch) -> pa.RecordBatch`. |
| `encryption` | `EncryptionOptions \| None` | `None` | Server-side encryption settings. See [encryption-options.md](encryption-options.md). |
| `tags` | `dict[str, str] \| None` | `None` | Object tags applied to every uploaded file. S3 and GCS support this; Azure Blob Storage ignores it. |
| `**kwargs` | | | Passed to `BaseOperator.__init__()`. |

## Constraints

**`query` vs `sql_template`** — exactly one must be provided. Providing both or neither raises `ValueError("Provide exactly one: query OR sql_template")`.

**`bucket` vs `container`** — use `container=` for Azure Blob Storage connections, `bucket=` for S3 and GCS connections. Providing an Azure hook without `container` raises `ValueError("Container must be specified for Azure storage")`.

## XCom return value

The operator pushes a dict to XCom under the default key (`return_value`) on successful execution.

| Key | Type | Description |
|-----|------|-------------|
| `shards` | `list[dict]` | One entry per `ShardResult`, sorted by `shard_index`. See [result-shape.md](result-shape.md). |
| `metrics` | `dict` | Summary metrics from `ExportMetrics.summary()`. Includes row throughput, byte throughput, and per-phase timings. |
| `total_rows` | `int` | Sum of rows across all non-skipped shards. |
| `total_bytes` | `int` | Sum of bytes across all non-skipped shards. |
| `elapsed_s` | `float` | Wall-clock seconds for the entire `execute()` call. |
| `watermark` | `str \| None` | New watermark value if `incremental` is configured, otherwise `None`. |
| `mode` | `"stream" \| "unload"` | Present only when `unload_strategy` is used; indicates the execution path taken. |

## Install extras

Install only the backends you need. Combine extras with commas.

| Extra | Installs |
|-------|---------|
| `[s3]` | `apache-airflow-providers-amazon` |
| `[azure]` | `apache-airflow-providers-microsoft-azure` |
| `[gcs]` | `apache-airflow-providers-google` |
| `[postgres]` | `apache-airflow-providers-postgres` |
| `[snowflake]` | `apache-airflow-providers-snowflake` |
| `[bigquery]` | `apache-airflow-providers-google` |
| `[redshift]` | `apache-airflow-providers-amazon` |

```
pip install "airflow-export-to-object-store[s3,postgres]"
pip install "airflow-export-to-object-store[gcs,bigquery]"
pip install "airflow-export-to-object-store[azure,snowflake]"
```
