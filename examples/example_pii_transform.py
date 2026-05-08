"""Row-level PII masking via ``transform_fn``.

The transform runs inside the fetch thread on every Arrow batch
*before* the Parquet writer sees it — masking is therefore atomic
with the export and never leaks raw PII to disk on the worker.

The function is module-level (not a lambda) so it remains compatible
with ``execution_mode='processes'`` if you ever switch the operator
into the process pool. Lambdas would fail to pickle there.
"""

from __future__ import annotations

import hashlib
from datetime import datetime

import pyarrow as pa
import pyarrow.compute as pc
from airflow import DAG

from airflow_export_to_object_store import StreamingExportOperator


def _hash_email(email: str) -> str:
    return hashlib.sha256(email.encode("utf-8")).hexdigest()[:16] + "@hashed"


def mask_pii(tbl: pa.Table) -> pa.Table:
    """Replace ``email`` with a stable hash and drop ``ssn`` entirely."""
    if "email" in tbl.schema.names:
        idx = tbl.schema.get_field_index("email")
        hashed = pa.array(
            [_hash_email(e) if e is not None else None for e in tbl.column("email").to_pylist()],
            type=pa.string(),
        )
        tbl = tbl.set_column(idx, "email", hashed)
    if "ssn" in tbl.schema.names:
        tbl = tbl.drop_columns(["ssn"])
    # Clamp negative balances to zero — illustration of arbitrary derived columns.
    if "balance" in tbl.schema.names:
        idx = tbl.schema.get_field_index("balance")
        clamped = pc.max_element_wise(tbl.column("balance"), pa.scalar(0, tbl.column("balance").type))
        tbl = tbl.set_column(idx, "balance", clamped)
    return tbl


with DAG(
    dag_id="export_users_pii_masked",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["export", "example", "pii", "transform"],
) as dag:
    StreamingExportOperator(
        task_id="users_to_s3",
        db_hook_id="postgres_default",
        storage_hook_id="aws_default",
        bucket="my-data-lake",
        sql_template="SELECT id, email, ssn, balance, created_at FROM users",
        remote_path_template="users/{{ ds }}/data.parquet",
        transform_fn=mask_pii,
        write_manifest=True,
    )
