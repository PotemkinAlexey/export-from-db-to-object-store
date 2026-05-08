"""Sharded Postgres → GCS export with manifest and per-file MD5.

Eight parallel shards split the table by ``mod(id, 8)`` and write
``part_000.parquet`` … ``part_007.parquet`` plus a single
``_manifest.json`` listing every file with its MD5.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG

from airflow_export_to_object_store import (
    ShardOptions,
    StreamingExportOperator,
)

NUM_SHARDS = 8

with DAG(
    dag_id="export_events_sharded",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["export", "example", "sharded"],
) as dag:
    StreamingExportOperator(
        task_id="events_to_gcs",
        db_hook_id="postgres_default",
        storage_hook_id="google_cloud_default",
        bucket="my-data-lake",
        sql_template="""
            SELECT *
            FROM events
            WHERE event_date = '{{ ds }}'
              AND mod(id, {{ shards_total }}) = {{ shard_id }}
        """,
        shards=[{"shard_id": i, "shards_total": NUM_SHARDS} for i in range(NUM_SHARDS)],
        remote_path_template="events/{{ ds }}/part_{{ '%03d' | format(shard_index) }}.parquet",
        shard_options=ShardOptions(max_workers=NUM_SHARDS, chunk_rows=100_000),
        compute_md5=True,
        write_manifest=True,
    )
