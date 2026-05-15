# IncrementalConfig

Enables watermark-based incremental exports. On each run the operator reads a "previous watermark" from XCom, renders it into the SQL query, exports only new rows, then pushes the "new watermark" back to XCom for the next run.

Pass an instance to `StreamingExportOperator(incremental=...)`.

```python
from airflow_export_to_object_store.incremental import IncrementalConfig

incremental = IncrementalConfig(
    watermark_query="SELECT MAX(updated_at) FROM orders",
    xcom_key="orders_watermark",
    default_value="2020-01-01 00:00:00",
)
```

## Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `watermark_query` | `str \| None` | `None` | SQL returning a single scalar value. Executed against the source database before the export to determine the new watermark. Mutually exclusive with `watermark_now_template`. |
| `watermark_now_template` | `str \| None` | `None` | Jinja template evaluated in the Airflow task context to produce the new watermark. Example: `"{{ ts }}"`. Mutually exclusive with `watermark_query`. |
| `xcom_key` | `str` | `"watermark"` | XCom key used to read the previous watermark from the upstream task and to push the new watermark after export. |
| `default_value` | `str` | `"1970-01-01 00:00:00"` | Watermark used on the first run when no prior XCom value exists. Must be a string; use the same format your SQL query expects. |

## Mutex constraint

Exactly one of `watermark_query` or `watermark_now_template` must be set. Setting both or neither raises `ValueError`.

| Scenario | Use |
|----------|-----|
| New watermark depends on what is actually in the database (e.g., `MAX(updated_at)`) | `watermark_query` |
| New watermark is derived from the Airflow execution date (e.g., `{{ ds }}`, `{{ ts }}`) | `watermark_now_template` |

## Watermark flow

1. **Read previous watermark.** The operator reads the XCom key `xcom_key` from the previous successful run of the same task. If no value is found, `default_value` is used.
2. **Render SQL.** The previous watermark is available as `{{ watermark_prev }}` in `sql_template`.
3. **Export.** Rows are streamed and uploaded as usual.
4. **Compute new watermark.** Either `watermark_query` is executed against the database, or `watermark_now_template` is rendered with the Airflow context. The result is passed through `coerce_watermark`.
5. **Push new watermark.** The new watermark string is pushed to XCom under `xcom_key` and is also returned in the XCom result dict as `watermark`.

### Template variables

| Variable | Value |
|----------|-------|
| `{{ watermark_prev }}` | Previous watermark string (or `default_value` on first run). Available in `sql_template`. |
| `{{ watermark_now }}` | New watermark string. Available in `watermark_now_template` after evaluation (not in `sql_template`). |

## `coerce_watermark` serialization

Database queries may return non-string types. `coerce_watermark` converts these to strings before pushing to XCom:

| Python type | Serialized as |
|-------------|--------------|
| `datetime.datetime` | `isoformat()` — e.g. `"2026-05-08T12:00:00"` |
| `datetime.date` | `isoformat()` — e.g. `"2026-05-08"` |
| `decimal.Decimal` | `str()` — e.g. `"12345.67"` |
| `str` | As-is |
| `int`, `float` | `str()` |
| `None` | `default_value` is retained; a `None` watermark is never pushed |

## Example — SQL template with watermark

```python
incremental = IncrementalConfig(
    watermark_query="SELECT MAX(updated_at) FROM orders",
    xcom_key="orders_watermark",
    default_value="2020-01-01 00:00:00",
)

export = StreamingExportOperator(
    task_id="export_orders",
    db_hook_id="my_postgres",
    storage_hook_id="my_s3",
    bucket="data-lake",
    sql_template="""
        SELECT * FROM orders
        WHERE updated_at > '{{ watermark_prev }}'
        ORDER BY updated_at
    """,
    incremental=incremental,
)
```

## Dataclass signature

```python
@dataclass(frozen=True)
class IncrementalConfig:
    watermark_query: str | None = None
    watermark_now_template: str | None = None
    xcom_key: str = "watermark"
    default_value: str = "1970-01-01 00:00:00"
```

`IncrementalConfig` is frozen; all fields must be set at construction time.
