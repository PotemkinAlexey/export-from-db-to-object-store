# BigQuery unload strategy

`BigQueryUnloadStrategy` uses BigQuery's `EXPORT DATA` statement to write query results directly to GCS without streaming through the Airflow worker. This is the recommended path for large BigQuery exports.

Set `unload_strategy=BigQueryUnloadStrategy(options)` on `StreamingExportOperator`.

```python
from airflow_export_to_object_store.unload.bigquery import (
    BigQueryUnloadStrategy,
    BigQueryUnloadOptions,
)

export = StreamingExportOperator(
    task_id="unload_events",
    db_hook_id="my_bigquery",
    storage_hook_id="my_gcs",
    bucket="data-lake",
    sql_template="SELECT * FROM `project.dataset.events` WHERE DATE(ts) = '{{ ds }}'",
    unload_strategy=BigQueryUnloadStrategy(
        options=BigQueryUnloadOptions(
            compression="SNAPPY",
        )
    ),
)
```

## BigQueryUnloadOptions fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `file_format` | `str` | `"PARQUET"` | Output file format. `"PARQUET"` is the only format fully supported by the operator's downstream processing. |
| `compression` | `str` | `"ZSTD"` | Parquet compression codec. See valid values below. |
| `overwrite` | `bool` | `True` | Overwrite existing files at the target GCS prefix. |
| `file_pattern` | `str` | `"*.parquet"` | Wildcard pattern appended to the GCS URI. Required when the export exceeds 1 GB — BigQuery automatically splits output into numbered files matching the pattern. |
| `extra_options` | `dict[str, str]` | `{}` | Additional raw key-value pairs appended to the `OPTIONS()` clause verbatim. |

### Valid values — `compression`

| Value | Notes |
|-------|-------|
| `"ZSTD"` | Default. Good compression ratio. |
| `"SNAPPY"` | Faster decompression. Common in Spark environments. |
| `"GZIP"` | High compression, slower write. |
| `"NONE"` | No compression. |

## Generated SQL shape

```sql
EXPORT DATA
  OPTIONS (
    uri = 'gs://<bucket>/<unload_dir>/*.parquet',
    format = 'PARQUET',
    compression = 'ZSTD',
    overwrite = true
  )
AS <your_sql>
```

## Supported hook pairs

| DB hook | Storage hook | Supported |
|---------|-------------|-----------|
| `BigQueryHook` | `GCSHook` | Yes |
| `BigQueryHook` | Any other | No — raises `ValueError("bucket must be set for BigQuery → GCS unload")` or hook mismatch error. |

BigQuery's `EXPORT DATA` only supports GCS as a destination.

## Known limitations

- **Row counts not returned.** `EXPORT DATA` returns an empty result set. `ShardResult.rows` is `0` for all unloaded shards. There is no BigQuery API to retrieve per-file row counts after an `EXPORT DATA` run.
- **File pattern required for large exports.** BigQuery splits output files automatically when the result exceeds 1 GB. The default `file_pattern="*.parquet"` handles this. Setting `file_pattern` to a literal filename (no wildcard) will cause the export to fail if BigQuery decides to split the output.
- **GCS only.** `EXPORT DATA` does not support S3 or Azure Blob Storage.

## strategy.name

```python
BigQueryUnloadStrategy.name == "bigquery"
```

The operator uses `name` for log messages and the `mode` key in the XCom result.
