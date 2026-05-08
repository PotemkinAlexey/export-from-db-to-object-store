"""Example DAG demonstrating ExportFromDBToObjectStoreOperator."""
from __future__ import annotations

from datetime import datetime

from airflow import DAG

from airflow_export_to_object_store import (
    ExportFromDBToObjectStoreOperator,
    ParquetOptions,
    ShardOptions,
)


with DAG(
    dag_id="example_export_to_object_store",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["example", "export"],
) as dag:

    # Single-shard export to Azure Blob.
    export_simple = ExportFromDBToObjectStoreOperator(
        task_id="export_simple_azure",
        db_hook_id="snowflake_default",
        storage_hook_id="wasb_default",
        sql_template="SELECT * FROM analytics.orders WHERE order_date = '{{ ds }}'",
        container="data-exports",
        remote_path_template="orders/{{ ds }}/data.parquet",
        parquet_options=ParquetOptions(compression="zstd", row_group_size=512_000),
    )

    # Sharded export to AWS S3.
    export_sharded = ExportFromDBToObjectStoreOperator(
        task_id="export_sharded_s3",
        db_hook_id="postgres_default",
        storage_hook_id="aws_default",
        bucket="my-data-lake",
        sql_template="""
            SELECT *
            FROM events
            WHERE event_date = '{{ ds }}'
              AND mod(id, {{ shards_total }}) = {{ shard_id }}
        """,
        shards=[{"shard_id": i, "shards_total": 8} for i in range(8)],
        remote_path_template="events/{{ ds }}/part_{{ '%03d' | format(shard_index) }}.parquet",
        shard_options=ShardOptions(max_workers=8, chunk_rows=100_000),
        compute_md5=True,
    )

    export_simple >> export_sharded
