# How-to: Handle failures gracefully

Configure upload retries, per-shard timeouts, and idempotent re-runs so
that a transient network blip does not require a full restart.

## Layers of retry

There are two independent retry mechanisms:

1. **Airflow-level retries** (`retries=N` on the operator): Airflow
   re-runs the entire task instance from the beginning. All shards
   re-execute, all files are re-uploaded (unless `skip_if_exists=True`).

2. **Operator-level upload retries** (`RetryOptions.upload_retries`):
   the operator retries only the upload step of a single shard, with
   exponential backoff. The database fetch and local write do not repeat.

Use both: operator-level retries handle transient upload errors without
re-querying the database; Airflow-level retries handle deeper failures
(OOM, worker crash, database timeout).

## Upload retries and backoff

```python
from airflow_export_to_object_store import RetryOptions, StreamingExportOperator

StreamingExportOperator(
    task_id="export_orders",
    db_hook_id="pg_default",
    storage_hook_id="aws_default",
    bucket="my-data-lake",
    sql_template="SELECT * FROM orders WHERE order_date = '{{ ds }}'",
    remote_path_template="orders/{{ ds }}/data.parquet",

    retry_options=RetryOptions(
        upload_retries=5,      # up to 5 upload attempts per shard
        backoff_base=1.5,      # wait = backoff_base^attempt seconds
        backoff_cap=20.0,      # capped at 20 seconds between attempts
    ),

    # Airflow-level retries are set here:
    retries=2,
    retry_delay=timedelta(minutes=5),
)
```

Wait times with these settings: 1.5 s, 2.25 s, 3.375 s, 5.0625 s —
then capped at 20 s for any further attempt. The sequence covers brief
S3 / GCS / Azure throttling events without waiting too long on the first
retry.

**What upload retries cover**: `boto3`/`google-cloud-storage`/`azure-storage-blob`
errors during the multipart upload. They do not retry the database query
or the Parquet write.

**What they do not cover**: `ValueError` or `RuntimeError` from the
operator itself (configuration errors, schema mismatches). Those are not
retried because retrying them would not help.

## Per-shard timeouts

Set `ShardOptions.timeout` to kill a shard that is hanging:

```python
from airflow_export_to_object_store import ShardOptions

shard_options=ShardOptions(
    max_workers=4,
    timeout=1800.0,   # 30 minutes per shard; raises TimeoutError if exceeded
)
```

When a shard exceeds `timeout` seconds, the operator raises
`concurrent.futures.TimeoutError` for that shard. In thread mode the
shard's future is cancelled. In process mode the subprocess is
terminated with `cancel()`.

The timeout covers the entire shard: fetch + write + validate + upload.
If your table is large and your network is slow, set `timeout` to at
least 2× your expected p99 shard duration.

## Sibling-shard cancellation

When one shard fails (any unretried exception), the operator signals all
other in-flight shards to stop:

- **Thread mode** (`execution_mode="threads"`, the default): a cancel
  event is set. Each shard's fetch loop checks the event between chunks
  and exits cleanly. Shards that are mid-upload finish their current
  HTTP request then stop.
- **Process mode** (`execution_mode="processes"`): the operator calls
  `future.cancel()` on each pending future and sends SIGTERM to
  in-progress subprocesses.

In both modes the operator waits for the remaining futures to settle
before raising the original exception. This prevents orphaned database
connections and ensures temporary files are cleaned up.

## Idempotent re-runs with skip_if_exists

Combine `skip_if_exists=True` with `overwrite=False` to make re-runs
after partial failures a no-op for shards that already landed:

```python
StreamingExportOperator(
    task_id="export_events_sharded",
    db_hook_id="pg_default",
    storage_hook_id="aws_default",
    bucket="my-data-lake",
    shards=[{"part": 0}, {"part": 1}, {"part": 2}],
    sql_template="SELECT * FROM events WHERE part_id = {{ part }}",
    remote_path_template="events/{{ ds }}/part={{ part }}/data.parquet",

    skip_if_exists=True,   # shard is skipped if the object already exists
    overwrite=False,       # do not overwrite if somehow the check misses
)
```

If shards 0 and 1 succeeded before the worker crashed, re-running the
task skips those two (logging `skipped=True` per shard) and only
re-executes shard 2. This avoids re-querying the database and
re-uploading data you already have.

`skip_if_exists` calls `uploader.exists()` at the start of each shard.
That call counts as a `HEAD` request (S3) or `get_blob_properties`
(Azure) or metadata fetch (GCS). It adds ~10–50 ms per shard.

## Airflow vs operator retries: which to use

| Scenario | Use |
|---|---|
| Transient S3 / GCS / Azure throttle | `RetryOptions.upload_retries` |
| Flaky database connection | Airflow `retries` + `skip_if_exists=True` |
| Worker OOM or crash | Airflow `retries` + `skip_if_exists=True` |
| Bad SQL or schema mismatch | Fix the DAG — neither retry helps |
| Partial shard failure mid-run | `skip_if_exists=True` on next retry |

## Monitoring slow shards

The operator logs a warning if a shard's memory usage exceeds
`ShardOptions.memory_limit_mb` (default 1024 MB):

```
WARNING - Shard 2 memory usage 1.3 GB exceeds limit 1.0 GB; consider reducing chunk_rows
```

It also logs a per-shard ASCII metrics block at INFO level showing MB/s
throughput and a performance grade (A+ / A / B / C). Use these to
identify which shard is the bottleneck before reaching for larger
machines.

## See also

- [Reference → RetryOptions](../reference/retry-options.md).
- [Reference → ShardOptions](../reference/shard-options.md).
- [How-to → Tune performance](tune-performance.md): reducing per-shard
  duration so timeouts are easier to set.
