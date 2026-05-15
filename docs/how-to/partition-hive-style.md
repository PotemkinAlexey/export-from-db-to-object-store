# How-to: Partition output in Hive style

Write files to paths like `year=2026/month=05/day=08/data.parquet` so
that Athena, Trino, and Spark auto-discover the partitions without a
`MSCK REPAIR TABLE`.

## The problem

Downstream query engines expect partition columns encoded in the path, not
just in the data. Without Hive-style paths you either register every
partition manually or pay for a full table scan on every query.

## Date partitioning from Airflow macros

Use `remote_path_template` with Airflow's Jinja macros. The `ds` macro
is `YYYY-MM-DD`; extract year, month, and day with the Jinja `strftime`
filter (via Pendulum's `format` method if you prefer) or just slice the
string.

The cleanest approach: use `macros.ds_format` or plain string slicing:

```python
from datetime import datetime
from airflow import DAG
from airflow_export_to_object_store import StreamingExportOperator

with DAG(
    dag_id="events_hive_daily",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
):
    StreamingExportOperator(
        task_id="events_to_s3",
        db_hook_id="pg_default",
        storage_hook_id="aws_default",
        bucket="my-data-lake",
        sql_template="SELECT * FROM events WHERE event_date = '{{ ds }}'",
        # ds is YYYY-MM-DD; slicing gives the parts without extra macros.
        remote_path_template=(
            "events/"
            "year={{ ds[:4] }}/"
            "month={{ ds[5:7] }}/"
            "day={{ ds[8:10] }}/"
            "data.parquet"
        ),
    )
```

After a run on 2026-05-08 the file lands at:

```text
s3://my-data-lake/events/year=2026/month=05/day=08/data.parquet
```

Athena and Trino recognise the `key=value` convention and expose `year`,
`month`, `day` as partition columns automatically.

## Partitioning by a shard key

When each shard covers a different partition value — region, country,
product line — embed the shard dict key directly in the path:

```python
StreamingExportOperator(
    task_id="events_by_region",
    db_hook_id="pg_default",
    storage_hook_id="aws_default",
    bucket="my-data-lake",

    shards=[
        {"region": "us-east-1"},
        {"region": "eu-west-1"},
        {"region": "ap-southeast-1"},
    ],

    sql_template="""
        SELECT *
        FROM events
        WHERE region     = '{{ region }}'
          AND event_date = '{{ ds }}'
    """,

    remote_path_template="events/{{ ds }}/region={{ region }}/data.parquet",
)
```

Result after a run on 2026-05-08:

```text
s3://my-data-lake/events/2026-05-08/region=us-east-1/data.parquet
s3://my-data-lake/events/2026-05-08/region=eu-west-1/data.parquet
s3://my-data-lake/events/2026-05-08/region=ap-southeast-1/data.parquet
```

## Cross-product partitions

Combine date macros with shard keys for a two-dimensional partition:

```python
COUNTRIES = ("US", "DE", "JP")
YEARS = (2025, 2026)

StreamingExportOperator(
    task_id="events_hive_partitioned",
    db_hook_id="pg_default",
    storage_hook_id="aws_default",
    bucket="my-data-lake",

    shards=[{"country": c, "year": y} for c in COUNTRIES for y in YEARS],

    sql_template="""
        SELECT *
        FROM events
        WHERE country = '{{ country }}'
          AND year    = {{ year }}
    """,

    remote_path_template=(
        "events/"
        "country={{ country }}/"
        "year={{ year }}/"
        "data.parquet"
    ),

    shard_options=ShardOptions(max_workers=len(COUNTRIES) * len(YEARS)),
    write_manifest=True,
)
```

Six shards run in parallel, producing six Hive-partitioned paths.

## Writing a manifest for atomic partition metadata

Add `write_manifest=True`. The operator writes `_manifest.json` at the
common prefix after all shards complete. Downstream orchestration can
poll for the manifest before running partition repair:

```sql
-- Athena: repair after the export DAG completes
MSCK REPAIR TABLE my_catalog.events;
```

With Trino or Spark:

```python
# PySpark
spark.sql("MSCK REPAIR TABLE my_catalog.events")
# or
spark.catalog.refreshTable("my_catalog.events")
```

If you use AWS Glue Data Catalog, the `MSCK REPAIR TABLE` statement picks
up new partitions automatically as long as the path follows
`key=value/key=value/` conventions.

## How downstream tools discover partitions

| Tool | Mechanism |
|---|---|
| AWS Athena | `MSCK REPAIR TABLE` or Glue Crawler |
| Trino (Hive metastore) | `MSCK REPAIR TABLE` or `system.sync_partition_metadata` |
| Spark (Hive metastore) | `spark.catalog.refreshTable()` or `MSCK REPAIR TABLE` |
| Spark (Delta / Iceberg) | N/A — use those table formats instead |
| DuckDB | `read_parquet('s3://bucket/events/**/*.parquet', hive_partitioning=True)` |

DuckDB can read Hive-partitioned Parquet without any catalog:

```python
import duckdb
duckdb.sql("""
    SELECT country, year, COUNT(*) AS n
    FROM read_parquet(
        's3://my-data-lake/events/**/*.parquet',
        hive_partitioning = TRUE
    )
    GROUP BY 1, 2
""")
```

## See also

- [How-to → Shard large tables](shard-large-tables.md): controlling
  `max_workers` for the cross-product case.
- [How-to → Write a manifest](../reference/manifest.md): manifest schema
  reference.
- [Reference → Operator parameters](../reference/operator.md):
  `remote_path_template` and `write_manifest`.
