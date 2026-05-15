# 03 — Native unload for terabyte-scale tables

Your goal: replace the streaming pipeline from Tutorial 02 with a
Snowflake `COPY INTO` job that writes Parquet directly to S3 — no rows
cross the network, no Python process buffers them. The same pattern
applies to BigQuery and Redshift, covered briefly at the end.

By the end of this tutorial you'll understand:

- why streaming hits a wall above ~100 GB,
- what changes when you add `unload_strategy` to the operator,
- how to configure the Snowflake storage integration,
- what the manifest looks like after a native unload.

## Why streaming stops scaling

The streaming path — Tutorial 01 and 02 — looks like this:

```
Snowflake → Python (fetch chunks) → PyArrow → local .parquet → S3
```

Every row crosses two network links (Snowflake → worker, worker → S3)
and sits in RAM on the Airflow worker. For a 10-million-row table that's
fine. For a 10-billion-row table you run into:

- **Memory**: `chunk_rows` batches pile up if writing is slower than
  fetching. Even with the soft `memory_limit_mb` guard, a terabyte table
  needs careful tuning.
- **Time**: a single Python process serialises the Arrow → Parquet write.
  `max_workers` helps for sharded queries, but the data still transits
  through the worker.
- **Disk**: the local temp file must fit on the worker's disk before
  upload. Large row groups make this painful.

Snowflake's `COPY INTO` solves all three: Snowflake writes the Parquet
files in parallel across its compute nodes, directly into the destination
bucket. The Airflow worker only runs the SQL statement and collects the
result rows.

## Prerequisites

In addition to the requirements from Tutorial 02:

- The operator installed with the Snowflake extra:
  ```bash
  pip install "airflow-export-to-object-store[s3,snowflake]"
  ```
- An Airflow Snowflake connection (`snowflake_default`) and an S3
  connection (`aws_default`).
- A Snowflake storage integration pointing at your S3 bucket (see
  below — a one-time admin step).

## Setting up a Snowflake storage integration

A storage integration is a Snowflake object that holds cloud credentials
on the Snowflake side so they never appear in SQL. A Snowflake
`ACCOUNTADMIN` runs this once:

```sql
-- Run as ACCOUNTADMIN in Snowflake
CREATE STORAGE INTEGRATION my_s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/SnowflakeExportRole'
  STORAGE_ALLOWED_LOCATIONS = ('s3://my-data-lake/');

-- Retrieve the IAM values Snowflake generated; paste into the trust policy
DESC INTEGRATION my_s3_integration;
```

Take the `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID` from
`DESC INTEGRATION` and add a trust relationship to
`SnowflakeExportRole`:

```json
{
  "Effect": "Allow",
  "Principal": { "AWS": "<STORAGE_AWS_IAM_USER_ARN>" },
  "Action": "sts:AssumeRole",
  "Condition": {
    "StringEquals": { "sts:ExternalId": "<STORAGE_AWS_EXTERNAL_ID>" }
  }
}
```

Grant the role `s3:PutObject`, `s3:GetObject`, and `s3:ListBucket` on
`my-data-lake`.

For GCS, create the integration with `STORAGE_PROVIDER = 'GCS'` and
`STORAGE_GCP_SERVICE_ACCOUNT`; grant the generated service account
`roles/storage.objectAdmin` on the bucket.

Azure unload is not yet supported by this operator (`SnowflakeUnloadStrategy.matches()` returns `False` for `WasbHook`).

## The DAG

Start from the incremental DAG in Tutorial 02 and add `unload_strategy`:

```python
from datetime import datetime

from airflow import DAG

from airflow_export_to_object_store import (
    IncrementalConfig,
    StreamingExportOperator,
)
from airflow_export_to_object_store.unload import (
    SnowflakeUnloadOptions,
    SnowflakeUnloadStrategy,
)

with DAG(
    dag_id="orders_native_unload",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
):
    StreamingExportOperator(
        task_id="orders_to_s3",
        db_hook_id="snowflake_default",
        storage_hook_id="aws_default",
        bucket="my-data-lake",

        sql_template="""
            SELECT *
            FROM analytics.orders
            WHERE updated_at >  '{{ watermark_prev }}'
              AND updated_at <= '{{ watermark_now }}'
        """,

        # The unload prefix. Snowflake writes files under this key;
        # the operator does not control individual filenames — Snowflake
        # names them data_<thread>_<chunk>_0.snappy.parquet.
        unload_dir_template="orders/{{ ds }}/",

        unload_strategy=SnowflakeUnloadStrategy(
            options=SnowflakeUnloadOptions(
                storage_integration="my_s3_integration",
                compression="ZSTD",
                max_file_size=256 * 1024 * 1024,  # 256 MiB per file
                single=False,                      # let Snowflake parallelise
            )
        ),

        incremental=IncrementalConfig(
            watermark_query="SELECT MAX(updated_at) FROM analytics.orders",
            xcom_key="watermark",
            default_value="1970-01-01 00:00:00",
        ),

        write_manifest=True,
    )
```

The key difference from Tutorial 02: `remote_path_template` is gone and
`unload_dir_template` takes its place. The operator passes this prefix to
Snowflake's `COPY INTO` statement; Snowflake decides how many output
files to write.

## What the operator does differently in unload mode

In streaming mode the operator:

1. Fetches rows in chunks to the worker.
2. Writes a local Parquet file per shard.
3. Validates the Parquet.
4. Uploads the file.

In unload mode:

1. Renders the SQL template and the `unload_dir_template`.
2. Calls `SnowflakeUnloadStrategy.unload()`, which builds and executes
   the `COPY INTO` statement against the Snowflake hook.
3. Collects Snowflake's result rows (one row per produced file).
4. Converts those rows into `ShardResult` objects.
5. Writes the manifest (if `write_manifest=True`).

Steps 2–4 happen entirely inside Snowflake. The Airflow worker has no
local Parquet file, no memory buffer, and no upload. The only network
traffic between the worker and the cloud is the SQL command and the
COPY INTO result set.

Consequences of unload mode:

- `shards` is ignored — Snowflake controls parallelism via its compute
  cluster and `MAX_FILE_SIZE`.
- `transform_fn` is ignored — transformation must happen in SQL or in a
  downstream task.
- `validate_parquet` is ignored — there is no local file to validate.
- `skip_if_exists` is ignored — Snowflake's `OVERWRITE = TRUE` governs
  re-run behaviour.
- `parquet_options` is ignored — use `SnowflakeUnloadOptions.compression`
  instead.

## What the manifest looks like

After a run that produced three files:

```json
{
  "version": 1,
  "exported_at": "2026-05-08T03:00:12+00:00",
  "total_rows": 4823901,
  "total_bytes": 312491008,
  "files": [
    {
      "shard_index": 0,
      "remote_uri": "s3://my-data-lake/orders/2026-05-08/data_0_0_0.snappy.parquet",
      "rows": 1634210,
      "bytes": 104857600,
      "md5": null,
      "skipped": false
    },
    {
      "shard_index": 1,
      "remote_uri": "s3://my-data-lake/orders/2026-05-08/data_1_0_0.snappy.parquet",
      "rows": 1589341,
      "bytes": 103809024,
      "skipped": false
    },
    {
      "shard_index": 2,
      "remote_uri": "s3://my-data-lake/orders/2026-05-08/data_2_0_0.snappy.parquet",
      "rows": 1600350,
      "bytes": 103824384,
      "skipped": false
    }
  ]
}
```

`md5` is `null` because Snowflake does not return checksums in its
`COPY INTO` result set. Row counts come from Snowflake's result; bytes
are per-file output sizes reported by Snowflake.

## BigQuery equivalent

```python
from airflow_export_to_object_store.unload import (
    BigQueryUnloadOptions,
    BigQueryUnloadStrategy,
)

StreamingExportOperator(
    task_id="orders_bq_to_gcs",
    db_hook_id="bigquery_default",
    storage_hook_id="gcs_default",
    bucket="my-data-lake",
    sql_template="SELECT * FROM `project.dataset.orders` WHERE date = '{{ ds }}'",
    unload_dir_template="orders/{{ ds }}/",
    unload_strategy=BigQueryUnloadStrategy(
        options=BigQueryUnloadOptions(
            compression="ZSTD",
            overwrite=True,
        )
    ),
    write_manifest=True,
)
```

BigQuery issues `EXPORT DATA OPTIONS(uri='gs://.../*.parquet', ...)`. The
GCS hook must have `roles/storage.objectAdmin` on the destination bucket.
Row counts are not in BigQuery's result set; the manifest records bytes
only.

## Redshift equivalent

```python
from airflow_export_to_object_store.unload import (
    RedshiftUnloadOptions,
    RedshiftUnloadStrategy,
)

StreamingExportOperator(
    task_id="orders_redshift_to_s3",
    db_hook_id="redshift_default",
    storage_hook_id="aws_default",
    bucket="my-data-lake",
    sql_template="SELECT * FROM public.orders WHERE order_date = '{{ ds }}'",
    unload_dir_template="orders/{{ ds }}/",
    unload_strategy=RedshiftUnloadStrategy(
        options=RedshiftUnloadOptions(
            iam_role="arn:aws:iam::123456789012:role/RedshiftUnloadRole",
            parallel=True,
            max_file_size_mb=256,
            cleanpath=True,
        )
    ),
    write_manifest=True,
)
```

Redshift issues `UNLOAD ('SELECT ...') TO 's3://...' IAM_ROLE '...'`.
The IAM role must trust the Redshift service and have `s3:PutObject` on
the destination prefix.

## When to use native unload vs streaming

| Scenario | Recommendation |
|---|---|
| < 10 GB, need `transform_fn` | Streaming |
| 10 GB – 100 GB, any DB | Streaming with shards |
| > 100 GB, Snowflake | `SnowflakeUnloadStrategy` |
| > 100 GB, BigQuery | `BigQueryUnloadStrategy` |
| > 100 GB, Redshift on S3 | `RedshiftUnloadStrategy` |
| Need PII masking | Streaming + `transform_fn` |

## What's next

- [How-to → Shard large tables](../how-to/shard-large-tables.md): parallel
  streaming export when you can't use native unload.
- [How-to → Tune performance](../how-to/tune-performance.md): chunk sizes,
  workers, compression — for the streaming path.
- [Reference → SnowflakeUnloadStrategy](../reference/unload-snowflake.md):
  every `SnowflakeUnloadOptions` field.
- [Reference → BigQueryUnloadStrategy](../reference/unload-bigquery.md).
- [Reference → RedshiftUnloadStrategy](../reference/unload-redshift.md).
