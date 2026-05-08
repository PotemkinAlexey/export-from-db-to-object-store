"""Single-shard Snowflake → S3 export — the smallest useful DAG.

Use this as a starting point. Once it works, layer on the production
features in the other examples (incremental, manifest, native unload).
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG

from airflow_export_to_object_store import StreamingExportOperator

with DAG(
    dag_id="export_orders_basic",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["export", "example"],
) as dag:
    StreamingExportOperator(
        task_id="orders_to_s3",
        db_hook_id="snowflake_default",
        storage_hook_id="aws_default",
        bucket="my-data-lake",
        sql_template="SELECT * FROM analytics.orders WHERE order_date = '{{ ds }}'",
        remote_path_template="orders/{{ ds }}/data.parquet",
    )
