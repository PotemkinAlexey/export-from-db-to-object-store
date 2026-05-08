"""Incremental export with watermark, manifest, and idempotent re-runs.

This is the recommended production shape:

* :class:`IncrementalConfig` reads the previous watermark from XCom
  and pushes the new one back on success — first run uses
  ``default_value``.
* ``skip_if_exists=True`` makes ``airflow tasks clear`` and retries
  safe: if the destination object is already there, the shard
  short-circuits without re-querying the DB.
* ``write_manifest=True`` emits ``_manifest.json`` next to the data
  for downstream Athena/Trino/Spark to pick up atomically.

The ``watermark_query`` pattern (vs. ``watermark_now_template="{{ ts }}"``)
captures one consistent moment from the database itself, regardless of
how long the export takes. Recommended for any source that supports
``SELECT MAX(...)`` cheaply.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG

from airflow_export_to_object_store import (
    IncrementalConfig,
    StreamingExportOperator,
)

with DAG(
    dag_id="export_orders_incremental",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    tags=["export", "example", "incremental", "production"],
) as dag:
    StreamingExportOperator(
        task_id="orders_incremental_to_s3",
        db_hook_id="postgres_default",
        storage_hook_id="aws_default",
        bucket="my-data-lake",
        sql_template="""
            SELECT *
            FROM orders
            WHERE updated_at >  '{{ watermark_prev }}'
              AND updated_at <= '{{ watermark_now }}'
        """,
        # The {{ ts_nodash }} gives each hourly run its own object key;
        # combined with skip_if_exists this means clears/retries can re-run
        # the same hour without producing duplicates.
        remote_path_template="orders/{{ ds }}/{{ ts_nodash }}_data.parquet",
        incremental=IncrementalConfig(
            watermark_query="SELECT MAX(updated_at) FROM orders",
            xcom_key="watermark",
            default_value="1970-01-01 00:00:00",
        ),
        skip_if_exists=True,
        write_manifest=True,
        compute_md5=True,
    )
