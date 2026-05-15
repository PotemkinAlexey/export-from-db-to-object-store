# 01 — Quickstart

Your goal: export a table from a database to a cloud bucket, as a single
Parquet file, in under five minutes. No production knobs yet — those
come in the next tutorials.

## Prerequisites

- Airflow ≥ 2.5 already running (`pip install apache-airflow` if you're
  starting from scratch).
- An Airflow connection to the **source database**. We'll call it
  `pg_default` for Postgres in this example; substitute your own.
- An Airflow connection to the **destination bucket**. We'll call it
  `aws_default` for AWS S3.

## Install

```bash
pip install "airflow-export-to-object-store[s3,postgres]"
```

The bracketed extras pull in the providers and drivers you need:
`s3` for the S3 backend, `postgres` for the psycopg2 driver. Pick the
extras matching your stack — see
[Reference → Operator parameters](../reference/operator.md#install-extras).

## Write the DAG

Save as `dags/quickstart_export.py` in your Airflow `dags_folder`:

```python
from datetime import datetime

from airflow import DAG

from airflow_export_to_object_store import StreamingExportOperator

with DAG(
    dag_id="quickstart_export",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,        # run on demand
    catchup=False,
):
    StreamingExportOperator(
        task_id="orders_to_s3",
        db_hook_id="pg_default",
        storage_hook_id="aws_default",
        bucket="my-data-lake",
        sql_template="SELECT * FROM public.orders",
        remote_path_template="orders/{{ ds }}/data.parquet",
    )
```

That's the whole DAG. Six required parameters:

| Parameter | What it is |
|---|---|
| `task_id` | Airflow task identifier |
| `db_hook_id` | Airflow connection ID for the source DB |
| `storage_hook_id` | Airflow connection ID for the bucket |
| `bucket` | S3 / GCS bucket name. (For Azure, use `container` instead.) |
| `sql_template` *or* `query` | The SQL — Jinja-templated or literal. Exactly one. |
| `remote_path_template` | Where the file lands inside the bucket. Jinja-templated. |

## Run it

In the Airflow UI:

1. Unpause `quickstart_export`.
2. Click **Trigger DAG**.
3. Watch `orders_to_s3` go green.

Then check the bucket:

```text
s3://my-data-lake/orders/2026-05-08/data.parquet
```

## What happened under the hood

1. The operator validated your storage connection (`HEAD` on the bucket)
   and probed network reachability for `s3.amazonaws.com:443`.
2. It rendered `sql_template` with Airflow's render context (so `{{ ds }}`
   and friends work — see the next tutorial for an example).
3. It opened a cursor on the source DB, fetched rows in chunks, built
   Apache Arrow batches.
4. It streamed the batches into a local Parquet file with zstd
   compression (the default — see
   [Reference → ParquetOptions](../reference/parquet-options.md)).
5. It validated the Parquet (footer, row groups, sample read).
6. It uploaded with boto3's automatic multipart transfer.
7. It returned a result dict to XCom with row counts, byte totals, and
   per-shard timings — see
   [Reference → Result shape](../reference/result-shape.md).

## Common first-run problems

| Symptom | Likely cause |
|---|---|
| `NotImplementedError: Unsupported storage hook` | You forgot the `[s3]` / `[azure]` / `[gcs]` extra. |
| `ValueError: Provide exactly one: query OR sql_template` | You set both, or neither. |
| `Container must be specified for Azure storage` | You used Azure but passed `bucket=` instead of `container=`. |
| Network probe warns then health check fails | VPC egress / proxy issue. The probe is a hint, not the cause; check the bucket policy and connection extras. |

## What's next

- [Tutorial 02 — Incremental exports with watermarks](02-incremental.md):
  the actual production shape (delta loads + idempotent re-runs +
  manifest).
- [How-to → Shard large tables](../how-to/shard-large-tables.md): split
  one logical export into N parallel files.
- [Reference → Operator parameters](../reference/operator.md): every
  parameter, exhaustively.
