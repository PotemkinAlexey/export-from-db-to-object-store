# 02 — Incremental exports with watermarks

Your goal: build a production-shape DAG that exports only the rows that
have changed since the last run, writes a manifest for downstream
consumers, and is safe to clear and retry.

By the end of this tutorial you'll understand:

- how `IncrementalConfig` reads and writes XCom across runs,
- how `skip_if_exists=True` makes re-runs idempotent,
- how the manifest helps downstream catalogs,
- how the three knobs interact.

## The shape

```python
from datetime import datetime

from airflow import DAG

from airflow_export_to_object_store import (
    IncrementalConfig,
    StreamingExportOperator,
)

with DAG(
    dag_id="orders_incremental",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
):
    StreamingExportOperator(
        task_id="orders_to_s3",
        db_hook_id="pg_default",
        storage_hook_id="aws_default",
        bucket="my-data-lake",

        # WHERE clause uses both watermarks: prev (from XCom) and now
        # (computed by the watermark_query below).
        sql_template="""
            SELECT *
            FROM public.orders
            WHERE updated_at >  '{{ watermark_prev }}'
              AND updated_at <= '{{ watermark_now }}'
        """,

        # ts_nodash is unique per logical schedule, so re-running the
        # same hour produces the same key — combined with skip_if_exists,
        # clears and retries are no-ops if the data already landed.
        remote_path_template="orders/{{ ds }}/{{ ts_nodash }}_data.parquet",

        incremental=IncrementalConfig(
            watermark_query="SELECT MAX(updated_at) FROM public.orders",
            xcom_key="watermark",
            default_value="1970-01-01 00:00:00",
        ),

        skip_if_exists=True,
        write_manifest=True,
        compute_md5=True,
    )
```

That's the whole thing. Let's walk through what happens.

## Run 1 — first ever execution

1. `IncrementalConfig` asks XCom for previous watermark via
   `xcom_pull(task_ids="orders_to_s3", key="watermark", include_prior_dates=True)`.
2. There is no previous run, so it falls back to `default_value`:
   `"1970-01-01 00:00:00"`.
3. The operator runs `watermark_query` against the source DB to get the
   *current* watermark — say `"2026-05-08 13:42:11"`.
4. Both values land in the rendering context as `{{ watermark_prev }}`
   and `{{ watermark_now }}`.
5. The SQL renders to `WHERE updated_at > '1970-01-01 00:00:00' AND
   updated_at <= '2026-05-08 13:42:11'` — i.e. "everything up to the
   freeze point".
6. The operator probes `s3://my-data-lake/orders/2026-05-08/2026-05-08T130000_data.parquet` —
   not there yet, so it does a normal export.
7. On success it pushes `"2026-05-08 13:42:11"` to XCom under the key
   `watermark`.

## Run 2 — one hour later

1. XCom returns `"2026-05-08 13:42:11"` from run 1.
2. `watermark_query` returns the new max — say `"2026-05-08 14:39:50"`.
3. SQL renders to `WHERE updated_at > '2026-05-08 13:42:11' AND
   updated_at <= '2026-05-08 14:39:50'` — only the rows that arrived
   in this hour.
4. Object key is different (`{{ ts_nodash }}` changed), so no
   short-circuit; export runs.
5. New watermark pushed to XCom.

## Re-running run 1 (a "clear")

1. XCom for the previous run is still there because of
   `include_prior_dates=True`.

   Wait — that's confusing. If you cleared the run, doesn't its XCom
   go too? Yes. But the *previous* run's XCom is intact, so
   `xcom_pull(include_prior_dates=True)` finds the most recent
   prior-run value (run 0's "1970-...", or run 2's
   "2026-05-08 14:39:50"). The behaviour depends on which runs you
   cleared — see the trade-off below.

2. The operator probes the destination: file already exists.
3. `skip_if_exists=True` short-circuits the shard, returning a
   `ShardResult` with `skipped=True` and zero rows/bytes. **No DB
   query happens, no upload.**
4. The watermark is *not* re-pushed (the operator only pushes on a
   genuine successful upload — see [Trade-offs](#trade-offs) below for
   why).

## What you get on disk

After three hourly runs:

```text
s3://my-data-lake/orders/2026-05-08/
  2026-05-08T130000_data.parquet
  2026-05-08T140000_data.parquet
  2026-05-08T150000_data.parquet
  _manifest.json
```

`_manifest.json` is an atomic catalog that downstream readers (Athena,
Trino, Spark) can list-without-listing. It looks like:

```json
{
  "version": 1,
  "exported_at": "2026-05-08T15:00:42+00:00",
  "total_rows": 12345,
  "total_bytes": 8765432,
  "files": [
    {
      "shard_index": 0,
      "remote_uri": "s3://my-data-lake/orders/.../2026-05-08T150000_data.parquet",
      "rows": 12345,
      "bytes": 8765432,
      "md5": "abc123...",
      "skipped": false
    }
  ]
}
```

The manifest is rewritten every run. If you want a stable pointer file
that downstream readers can poll, build a separate task that copies the
manifest to a known name.

## Trade-offs

### Why not use just `{{ ts }}` for `watermark_now`?

You can — `IncrementalConfig(watermark_now_template="{{ ts }}")`.

Pros:

- No extra DB round-trip.
- Watermark is a deterministic logical timestamp.

Cons:

- If the export takes 15 minutes, rows committed to the source during
  those 15 minutes carry an `updated_at` later than `{{ ts }}` and
  won't be picked up — even though they're "before" the next run's
  watermark. You get clock-skew gaps.

The `watermark_query` form runs `MAX(updated_at)` *before* the
export starts, so the snapshot is consistent regardless of how long
the export takes.

### Why is the watermark not pushed on a `skipped` run?

Because nothing happened to the data: a re-run that found the file
already there didn't observe a new MAX. Pushing a stale watermark
wouldn't break anything (the next run would re-compute via the query),
but it would be *misleading* in XCom history.

The current behaviour: watermark pushed iff the operator actually
processed shards (skipped or otherwise — but with at least one
upload).

> See `_commit_watermark` in
> [src/airflow_export_to_object_store/operator.py](../../src/airflow_export_to_object_store/operator.py)
> for the exact condition.

### What if the source's `updated_at` isn't monotonic?

If late-arriving rows can appear with `updated_at` in the past, the
`> watermark_prev` window misses them.

Mitigations:

- Add a "lateness" buffer in the SQL: `WHERE updated_at > ('{{ watermark_prev }}'::timestamp - INTERVAL '1 hour')`.
- Switch to a CDC log (Debezium, Snowflake `CHANGES`) instead of a
  high-watermark column.

This is a data-modelling concern, not an operator concern.

## What's next

- [How-to → Shard large tables](../how-to/shard-large-tables.md): take
  this DAG and split it across N parallel shards.
- [Tutorial 03 — Native unload](03-native-unload.md): for terabyte-scale
  tables, replace the streaming pipeline with `COPY INTO`.
- [Reference → IncrementalConfig](../reference/incremental-config.md):
  every option exhaustively.
- [Explanation → Idempotency, watermarks, and the manifest](../explanation/idempotency-and-state.md):
  the design rationale.
