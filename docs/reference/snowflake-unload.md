# Snowflake unload strategy

`SnowflakeUnloadStrategy` uses Snowflake's `COPY INTO <location>` command to export data directly from Snowflake to S3 or GCS without streaming through the Airflow worker. This is the recommended path for large Snowflake exports.

Set `unload_strategy=SnowflakeUnloadStrategy(options)` on `StreamingExportOperator`.

```python
from airflow_export_to_object_store.unload.snowflake import (
    SnowflakeUnloadStrategy,
    SnowflakeUnloadOptions,
)

export = StreamingExportOperator(
    task_id="unload_events",
    db_hook_id="my_snowflake",
    storage_hook_id="my_s3",
    bucket="data-lake",
    sql_template="SELECT * FROM events WHERE dt = '{{ ds }}'",
    unload_strategy=SnowflakeUnloadStrategy(
        options=SnowflakeUnloadOptions(
            storage_integration="MY_S3_INTEGRATION",
            compression="ZSTD",
        )
    ),
)
```

## SnowflakeUnloadOptions fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `storage_integration` | `str \| None` | `None` | Name of the Snowflake storage integration object. Recommended for production. Mutually exclusive with `credentials`. |
| `credentials` | `dict[str, str] \| None` | `None` | Inline credentials dict for ad-hoc use, e.g. `{"AWS_KEY_ID": "...", "AWS_SECRET_KEY": "..."}`. Mutually exclusive with `storage_integration`. |
| `file_format` | `str` | `"PARQUET"` | Output file format. `"PARQUET"` is the only format fully supported by the operator's downstream processing. |
| `compression` | `str` | `"ZSTD"` | Parquet compression codec used inside the COPY INTO command. See valid values below. |
| `max_file_size` | `int` | `268435456` | Maximum output file size in bytes (default 256 MiB). Snowflake splits output into multiple files when this is exceeded. |
| `single` | `bool` | `False` | When `True`, forces Snowflake to write a single output file. Not recommended for large datasets. |
| `overwrite` | `bool` | `True` | Overwrite existing files at the target prefix. |
| `header` | `bool` | `False` | Include a header row. Ignored for Parquet format. |
| `extra_options` | `dict[str, str]` | `{}` | Additional raw key-value pairs appended to the `FILE_FORMAT` clause verbatim. |

### Valid values — `compression`

| Value | Notes |
|-------|-------|
| `"ZSTD"` | Default. Good compression ratio. |
| `"SNAPPY"` | Faster decompression. |
| `"GZIP"` | High compression, slower. |
| `"LZO"` | LZO codec. |
| `"BROTLI"` | Brotli codec. |
| `"LZ4"` | Fast, lower ratio. |
| `"NONE"` | No compression. |

### Auth — storage_integration vs credentials

| Auth method | Use when |
|-------------|---------|
| `storage_integration` | Production. Credentials are managed in Snowflake and rotated automatically. |
| `credentials` | Ad-hoc testing only. Keys appear in query history and audit logs. |

Providing both raises `ValueError("Set either storage_integration or credentials, not both")`. Providing neither raises `ValueError("One of storage_integration / credentials must be set for Snowflake unload")`.

## Generated SQL shape

```sql
COPY INTO 's3://<bucket>/<unload_dir>/'
FROM (<your_sql>)
STORAGE_INTEGRATION = MY_S3_INTEGRATION
FILE_FORMAT = (TYPE = PARQUET COMPRESSION = ZSTD)
MAX_FILE_SIZE = 268435456
OVERWRITE = TRUE
HEADER = FALSE
```

## Supported hook pairs

| DB hook | Storage hook | Supported |
|---------|-------------|-----------|
| `SnowflakeHook` | `S3Hook` | Yes |
| `SnowflakeHook` | `GCSHook` | Yes |
| `SnowflakeHook` | `WasbHook` (Azure) | No — raises `NotImplementedError`. Snowflake requires the storage account name which cannot be reliably derived from the hook. Use streaming mode for Azure destinations. |

## Known limitations

- **MD5 not returned.** Snowflake's `COPY INTO` does not return a per-file MD5. `ShardResult.md5` is `None` for all unloaded shards.
- **Row counts not returned.** Per-file row counts are not surfaced. `ShardResult.rows` reflects what Snowflake reports in the copy result, which may be 0 for some driver versions.
- **Azure not supported.** See hook pairs table above.

## strategy.name

```python
SnowflakeUnloadStrategy.name == "snowflake"
```

The operator uses `name` for log messages and the `mode` key in the XCom result.
