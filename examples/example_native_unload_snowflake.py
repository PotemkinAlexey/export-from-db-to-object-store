"""Snowflake → S3 via native ``COPY INTO`` for terabyte-scale exports.

Streaming through an Airflow worker is fine up to tens of millions of
rows. Past that, ask the warehouse to write Parquet directly to the
bucket — typically 10–50× faster, no client-side fetch at all.

The :class:`SnowflakeUnloadStrategy` issues
``COPY INTO 's3://bucket/...' FROM (SELECT ...)`` with the user's
options, parses the result rows into one ``ShardResult`` per produced
file, and feeds the same manifest writer used by streaming exports.

Use ``storage_integration`` (set up once by a Snowflake admin) instead
of inline credentials in production — zero secrets cross the SQL
boundary.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG

from airflow_export_to_object_store import StreamingExportOperator
from airflow_export_to_object_store.unload import (
    SnowflakeUnloadOptions,
    SnowflakeUnloadStrategy,
)

with DAG(
    dag_id="export_orders_native_unload",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["export", "example", "snowflake", "performance"],
) as dag:
    StreamingExportOperator(
        task_id="orders_native_unload",
        db_hook_id="snowflake_default",
        storage_hook_id="aws_default",
        bucket="my-data-lake",
        sql_template="SELECT * FROM analytics.orders WHERE order_date = '{{ ds }}'",
        unload_dir_template="orders/{{ ds }}/",
        write_manifest=True,
        unload_strategy=SnowflakeUnloadStrategy(
            SnowflakeUnloadOptions(
                # Replace MY_S3_INT with the integration name your Snowflake
                # admin created against this bucket.
                storage_integration="MY_S3_INT",
                compression="ZSTD",
                max_file_size=256 * 1024 * 1024,  # ~256 MiB → many readable chunks
                overwrite=True,
            ),
        ),
    )
