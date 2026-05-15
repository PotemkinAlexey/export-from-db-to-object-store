# How-to: Mask PII with transform_fn

Apply column-level transformations — masking, hashing, dropping — to
every Arrow batch before it reaches the Parquet writer.

## The problem

You need to export a table that contains personal data, but the
destination bucket is accessible to analysts who should not see raw
values. You want the masking to be atomic with the export so raw values
never reach disk on the worker.

## How transform_fn works

`transform_fn` is called on each `pyarrow.Table` as it is fetched from
the database, before the Parquet writer sees the data. The function
receives a `pa.Table` and must return a `pa.Table`.

```
DB cursor → Arrow batches → [transform_fn] → Parquet writer → upload
```

Because masking happens before the write, the raw values exist only in
worker RAM for the duration of one batch. Nothing unmasked is written to
the local temp file or uploaded.

## A complete example

```python
# dags/export_users_masked.py
from __future__ import annotations

import hashlib
from datetime import datetime

import pyarrow as pa
import pyarrow.compute as pc
from airflow import DAG

from airflow_export_to_object_store import StreamingExportOperator


def mask_pii(tbl: pa.Table) -> pa.Table:
    """Hash email addresses, drop phone numbers.

    Must be a module-level function — not a lambda — so it can be
    pickled when execution_mode='processes'.
    """
    # Hash email: SHA-256, truncated to 16 hex chars + '@hashed' suffix.
    if "email" in tbl.schema.names:
        idx = tbl.schema.get_field_index("email")
        hashed = pa.array(
            [
                hashlib.sha256(v.encode()).hexdigest()[:16] + "@hashed"
                if v is not None else None
                for v in tbl.column("email").to_pylist()
            ],
            type=pa.string(),
        )
        tbl = tbl.set_column(idx, "email", hashed)

    # Drop phone — removing the column entirely is safer than nulling it.
    if "phone" in tbl.schema.names:
        tbl = tbl.drop_columns(["phone"])

    return tbl


with DAG(
    dag_id="export_users_masked",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
):
    StreamingExportOperator(
        task_id="users_to_s3",
        db_hook_id="pg_default",
        storage_hook_id="aws_default",
        bucket="my-data-lake",
        sql_template="SELECT id, email, phone, created_at FROM users",
        remote_path_template="users/{{ ds }}/data.parquet",
        transform_fn=mask_pii,
    )
```

## Chaining multiple transforms

Compose transforms with a wrapper function:

```python
def drop_ssn(tbl: pa.Table) -> pa.Table:
    if "ssn" in tbl.schema.names:
        return tbl.drop_columns(["ssn"])
    return tbl


def clamp_balance(tbl: pa.Table) -> pa.Table:
    if "balance" in tbl.schema.names:
        idx = tbl.schema.get_field_index("balance")
        col = tbl.column("balance")
        clamped = pc.max_element_wise(col, pa.scalar(0, col.type))
        return tbl.set_column(idx, "balance", clamped)
    return tbl


def transform(tbl: pa.Table) -> pa.Table:
    tbl = mask_pii(tbl)
    tbl = drop_ssn(tbl)
    tbl = clamp_balance(tbl)
    return tbl
```

Pass `transform_fn=transform`. Each function in the chain must be
module-level.

## The processes-mode constraint

If you use `execution_mode="processes"` (see
[How-to → Shard large tables](shard-large-tables.md)), `transform_fn`
must be picklable by `multiprocessing`. That means:

- **Module-level functions**: always picklable.
- **Lambdas**: not picklable — `PicklingError` at runtime.
- **Closures** (inner functions that reference outer variables): not
  picklable.
- **Methods on instances**: picklable only if the instance is picklable,
  which is often not the case.

If you need a closure-like pattern, move the captured values into module
globals or use `functools.partial` over a module-level function:

```python
import functools

def _hash_column(tbl: pa.Table, column: str) -> pa.Table:
    if column not in tbl.schema.names:
        return tbl
    idx = tbl.schema.get_field_index(column)
    hashed = pa.array(
        [hashlib.sha256(v.encode()).hexdigest() if v else None
         for v in tbl.column(column).to_pylist()],
        type=pa.string(),
    )
    return tbl.set_column(idx, column, hashed)

# functools.partial produces a picklable callable
mask_email = functools.partial(_hash_column, column="email")
```

## Verifying the output

After the DAG runs, check a sample row with PyArrow or DuckDB:

```python
import pyarrow.parquet as pq

tbl = pq.read_table("s3://my-data-lake/users/2026-05-08/data.parquet")
print(tbl.schema)
print(tbl.column("email")[0])   # e.g. "3d4e5f6a7b8c9d0e@hashed"
assert "phone" not in tbl.schema.names
```

## See also

- [How-to → Shard large tables](shard-large-tables.md): `execution_mode`
  and the pickling constraint.
- [Reference → Operator parameters](../reference/operator.md):
  `transform_fn` entry.
