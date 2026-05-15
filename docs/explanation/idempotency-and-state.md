# Idempotency and State Management

Idempotency in a data pipeline means that running the same operation twice produces the same result as running it once. The operator provides three distinct idempotency mechanisms, each operating at a different layer of the system. Understanding why all three exist — and how they interact — is important because they are not redundant: each covers a failure scenario the others cannot.

## Three Mechanisms, Three Layers

**Object-level idempotency** is provided by `skip_if_exists`. Before opening any database cursor, the worker calls `uploader.exists(storage_hook, remote_path=remote_path)`. If the object already exists in the store, the worker returns a `ShardResult(skipped=True, rows=0, bytes=0)` immediately, without querying the database. This mechanism answers the question: "Did this specific file already land in object storage?" It is the cheapest check because it requires only a HEAD request to the object store.

**Data-level idempotency** is provided by watermarks. The watermark mechanism tracks which rows have already been exported across DAG runs. A typical incremental query looks like:

```sql
SELECT * FROM orders
WHERE updated_at > {{ watermark_prev }} AND updated_at <= {{ watermark_now }}
```

This ensures that each DAG run exports a non-overlapping window of rows, and re-running the same DAG run (with the previous watermark intact from XCom) produces the same set of rows. This mechanism answers the question: "Which rows belong to this run?" It operates at the data level, not the file level.

**Catalog-level idempotency** is provided by the manifest. The manifest is written last, after all shards complete successfully. If the operator fails mid-run, no manifest exists for that run. Downstream readers that depend on the manifest to discover data will not see partial results. This mechanism answers the question: "Did this run complete successfully?" It is the system's external commitment that a complete, consistent dataset is available.

## Why All Three Are Necessary

Consider the failure modes each mechanism covers that the others cannot:

`skip_if_exists` handles the case where an export succeeded in a previous run but the watermark was not pushed (because the task was marked failed in Airflow after the upload completed). Without `skip_if_exists`, a retry would re-export the same rows to the same paths, potentially causing double-counting in downstream systems that do not deduplicate.

Watermarks handle the case where a run produces new files that did not exist before. `skip_if_exists` cannot help here because the objects are new — there is nothing to skip. The watermark defines which rows are in scope, and without it every run would export all rows from the beginning of time.

The manifest handles the case where some but not all shards completed before a failure. Object-level existence checks and watermarks cannot distinguish "all 12 shards completed" from "7 of 12 shards completed." The manifest, written atomically after all shards succeed, is the only signal that the complete dataset for a run is present.

## The XCom Watermark and include_prior_dates

The previous watermark is retrieved from XCom using `include_prior_dates=True`:

```python
prev = ti.xcom_pull(
    task_ids=self.task_id,
    key=cfg.xcom_key,
    include_prior_dates=True
)
```

Without `include_prior_dates=True`, XCom pull returns only the value pushed in the current DAG run. This is the correct behavior for most XCom use cases — you want the value your task set in this run. For watermarks, it is wrong.

When a DAG run is cleared and re-run, Airflow creates a fresh task instance for the run. At the point when the operator reads the watermark, the current run has not yet pushed a watermark — it has not finished. The watermark from the previous successful run lives in XCom under the previous run's execution date. `include_prior_dates=True` tells Airflow to search across all prior runs, returning the most recent watermark regardless of when it was pushed.

This is critical for correctness: if a cleared run reads `None` as its previous watermark and falls back to `default_value`, it may re-export rows that were already exported in the previous run, depending on how the incremental query is written.

## Why the Watermark Is Not Pushed on a Skipped Run

The watermark is only pushed to XCom when a genuine upload occurred. A run where every shard was skipped via `skip_if_exists` does not push a watermark.

The reason is that watermarks serve as evidence of what was processed. If a run is skipped entirely — because all the target objects already exist — nothing was processed in that run. Pushing the new watermark value would record a state transition in XCom history that corresponds to no actual data movement. This creates a misleading XCom history: an operator reading the watermark in a future diagnostic session would see a sequence of watermark values and assume each one corresponds to a successful export, when in fact some runs produced no data.

More practically, if a run is skipped and the watermark is advanced, the next non-skipped run will use the advanced watermark as its lower bound. Whether this is correct depends entirely on why the shards were skipped. If they were skipped because they were already uploaded, the watermark advance is probably correct. But the operator cannot know why the objects exist — they might have been uploaded by a different process, or by a manual re-run of a different DAG. Skipping the watermark push on a skipped run is the conservative, correct choice: it leaves the watermark anchored at the last run that actually moved data.

## watermark_query vs watermark_now_template

The operator offers two ways to determine the upper bound of the current export window.

`watermark_query` executes a SQL query against the database to determine the watermark: for example, `SELECT MAX(updated_at) FROM orders`. This captures a consistent snapshot of the database's current high-water mark at the moment the query runs, before any rows are exported. The advantage is consistency: if rows are being inserted while the export runs, the watermark reflects the state of the database at a specific point in time, and rows committed after that point are deferred to the next run. The cost is an extra round-trip to the database before any export work begins.

`watermark_now_template` renders the watermark from the Jinja context: for example, `{{ ds }}` (the DAG run date) or `{{ macros.ds_add(ds, -1) }}`. This is deterministic — the watermark is computed from the DAG run metadata, not from a live database query. The advantage is simplicity and speed. The disadvantage is that it can miss rows committed to the database during a long export: if the export takes two hours and rows with `updated_at` in that window are committed after the template is rendered, they fall within the window's bounds but were not present when the window opened, and will be missed if the next run's lower bound is set to this run's rendered upper bound.

Neither is universally correct. `watermark_query` is better for operational tables with continuous writes; `watermark_now_template` is better for batch-loaded tables where you want predictable, schedule-aligned export windows.
