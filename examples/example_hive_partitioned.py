"""Hive-style partitioned export by country × year.

The cross-product of the two lists below produces six shards, each
writing to ``events/country=<X>/year=<Y>/data.parquet`` — exactly what
Athena, Trino, and Spark want for partition pruning.

Manifest collects all six files; downstream ``MSCK REPAIR TABLE`` (or
its Trino/Spark equivalents) picks them up.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG

from airflow_export_to_object_store import (
    ShardOptions,
    StreamingExportOperator,
)

COUNTRIES = ("US", "DE", "JP")
YEARS = (2025, 2026)

with DAG(
    dag_id="export_events_hive_partitioned",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["export", "example", "hive", "partitioning"],
) as dag:
    StreamingExportOperator(
        task_id="events_hive_partitioned",
        db_hook_id="postgres_default",
        storage_hook_id="aws_default",
        bucket="my-data-lake",
        sql_template="""
            SELECT *
            FROM events
            WHERE country = '{{ country }}'
              AND year    = {{ year }}
              AND event_date = '{{ ds }}'
        """,
        shards=[{"country": c, "year": y} for c in COUNTRIES for y in YEARS],
        remote_path_template="events/country={{ country }}/year={{ year }}/{{ ds }}/data.parquet",
        shard_options=ShardOptions(max_workers=len(COUNTRIES) * len(YEARS)),
        write_manifest=True,
    )
