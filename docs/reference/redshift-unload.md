# Redshift unload strategy

`RedshiftUnloadStrategy` uses Redshift's `UNLOAD` command to write query results directly to S3 without streaming through the Airflow worker. This is the recommended path for large Redshift exports.

Set `unload_strategy=RedshiftUnloadStrategy(options)` on `StreamingExportOperator`.

```python
from airflow_export_to_object_store.unload.redshift import (
    RedshiftUnloadStrategy,
    RedshiftUnloadOptions,
)

export = StreamingExportOperator(
    task_id="unload_orders",
    db_hook_id="my_redshift",
    storage_hook_id="my_s3",
    bucket="data-lake",
    sql_template="SELECT * FROM orders WHERE order_date = '{{ ds }}'",
    unload_strategy=RedshiftUnloadStrategy(
        options=RedshiftUnloadOptions(
            iam_role="arn:aws:iam::123456789012:role/RedshiftUnloadRole",
            parallel=True,
            cleanpath=True,
        )
    ),
)
```

## RedshiftUnloadOptions fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `iam_role` | `str \| None` | `None` | ARN of the IAM role attached to the Redshift cluster that has write access to the S3 prefix. Recommended for production. Mutually exclusive with `credentials`. |
| `credentials` | `str \| None` | `None` | Inline credentials string, e.g. `"ACCESS_KEY_ID=...;SECRET_ACCESS_KEY=..."`. Mutually exclusive with `iam_role`. |
| `file_format` | `str` | `"PARQUET"` | Output file format. `"PARQUET"`, `"CSV"`, or `"JSON"`. Only `"PARQUET"` is fully supported by the operator's downstream processing. |
| `parallel` | `bool` | `True` | When `True` (ON): Redshift writes one file per node slice in parallel. When `False` (OFF): Redshift writes a single file, sorted by the query's `ORDER BY` clause. |
| `max_file_size_mb` | `int` | `256` | Maximum file size in mebibytes per output file. |
| `cleanpath` | `bool` | `True` | Delete all existing files at the target S3 prefix before unloading. Prevents stale files from a previous run mixing with the current export. |
| `manifest` | `bool` | `False` | Write a Redshift-native `manifest.json` alongside the output files. This is separate from the operator's own `write_manifest` feature. |
| `extra_options` | `list[str]` | `[]` | Raw SQL clauses appended verbatim to the `UNLOAD` statement, e.g. `["ENCRYPTED", "REGION 'us-east-1'"]`. |

### Auth — iam_role vs credentials

| Auth method | Use when |
|-------------|---------|
| `iam_role` | Production. The role is attached to the cluster; no keys in query text. |
| `credentials` | Ad-hoc testing. Keys appear in SVL_STATEMENTTEXT and audit logs — rotate them after use. |

Providing both raises `ValueError("One of iam_role / credentials must be set for Redshift unload")` (whichever field triggers the conflict). Providing neither raises the same error.

### parallel ON vs OFF

| Setting | Output | Use when |
|---------|--------|---------|
| `parallel=True` (ON) | One file per Redshift slice (typically 2–32 files per node). Fastest. | Most cases. Downstream query engines handle multiple files natively. |
| `parallel=False` (OFF) | Single file, rows sorted by query `ORDER BY`. | When the downstream consumer requires a single sorted file. Significantly slower for large datasets. |

## Generated SQL shape

```sql
UNLOAD ('SELECT * FROM orders WHERE order_date = ''2026-05-08''')
TO 's3://<bucket>/<unload_dir>/'
IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftUnloadRole'
FORMAT AS PARQUET
PARALLEL ON
MAXFILESIZE 256 MB
CLEANPATH
```

When `credentials` is used instead of `iam_role`:

```sql
UNLOAD (...)
TO 's3://...'
CREDENTIALS 'ACCESS_KEY_ID=...;SECRET_ACCESS_KEY=...'
...
```

## Supported hook pairs

| DB hook | Storage hook | Supported |
|---------|-------------|-----------|
| `RedshiftSQLHook` | `S3Hook` | Yes |
| `RedshiftSQLHook` | Any other | No — `UNLOAD` only writes to S3. |

## Known limitations

- **Per-file row counts not returned.** Redshift does not surface per-file row counts in the `UNLOAD` response. `ShardResult.rows` is `0` for all unloaded shards.
- **Authoritative counts.** To get the actual number of rows written per file, query `STL_UNLOAD_LOG` after the task completes:

```sql
SELECT
    path,
    line_count,
    transfer_size
FROM STL_UNLOAD_LOG
WHERE query = pg_last_query_id()
ORDER BY path;
```

- **S3 only.** `UNLOAD` does not support GCS or Azure Blob Storage.

## strategy.name

```python
RedshiftUnloadStrategy.name == "redshift"
```

The operator uses `name` for log messages and the `mode` key in the XCom result.
