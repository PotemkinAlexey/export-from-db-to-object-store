# How-to: Shard large tables

Split one logical export into N Parquet files written in parallel.

## The problem

A single-shard export serialises all rows through one cursor and one
writer. For a table with hundreds of millions of rows that means one slow
query and one file that grows until it is complete. Sharding runs N
queries in parallel, each covering a slice of the data, and writes N
files concurrently.

## The `shards` parameter

Pass a list of dicts. Each dict is one shard. The operator merges each
dict into the Jinja render context for `sql_template` and
`remote_path_template`, and adds `shard_index` automatically (0-based,
in list order).

```python
shards=[
    {"region": "us-east-1"},
    {"region": "eu-west-1"},
    {"region": "ap-southeast-1"},
]
```

Keys you define appear alongside the standard Airflow macros (`ds`, `ts`,
etc.) and the injected `shard_index`.

## Example: sharding by region

```python
from airflow_export_to_object_store import ShardOptions, StreamingExportOperator

StreamingExportOperator(
    task_id="events_by_region",
    db_hook_id="pg_default",
    storage_hook_id="aws_default",
    bucket="my-data-lake",

    shards=[
        {"region": "us-east-1"},
        {"region": "eu-west-1"},
        {"region": "ap-southeast-1"},
    ],

    sql_template="""
        SELECT *
        FROM events
        WHERE region   = '{{ region }}'
          AND event_ts >= '{{ ds }} 00:00:00'
          AND event_ts <  '{{ ds }} 00:00:00'::timestamp + INTERVAL '1 day'
    """,

    # shard_index is 0-indexed; %03d zero-pads to three digits.
    remote_path_template="events/{{ ds }}/{{ region }}/data_{{ '%03d' | format(shard_index) }}.parquet",

    shard_options=ShardOptions(max_workers=3),
)
```

Each shard gets its own `{{ region }}` value from the shard dict, plus
`{{ shard_index }}` (0, 1, 2) injected by the operator.

## Example: sharding by a numeric key range

Use this when the table has no natural partition column but does have a
monotonically increasing surrogate key:

```python
SHARD_RANGES = [
    {"lo": 0,          "hi": 10_000_000},
    {"lo": 10_000_000, "hi": 20_000_000},
    {"lo": 20_000_000, "hi": 30_000_000},
    {"lo": 30_000_000, "hi": None},       # open-ended last shard
]

StreamingExportOperator(
    task_id="orders_by_key",
    db_hook_id="pg_default",
    storage_hook_id="aws_default",
    bucket="my-data-lake",

    shards=SHARD_RANGES,

    sql_template="""
        SELECT *
        FROM public.orders
        WHERE id >= {{ lo }}
          {% if hi is not none %}AND id < {{ hi }}{% endif %}
    """,

    remote_path_template="orders/{{ ds }}/part_{{ '%03d' | format(shard_index) }}.parquet",

    shard_options=ShardOptions(max_workers=4),
)
```

## Controlling parallelism

`ShardOptions.max_workers` sets the thread-pool size. The default is 6.

```python
from airflow_export_to_object_store import ShardOptions

shard_options=ShardOptions(
    max_workers=8,       # 8 concurrent shards
    chunk_rows=100_000,  # rows per Arrow batch inside each shard
)
```

The practical ceiling is your database's connection pool. A pool of 10
connections caps useful `max_workers` at ~8–9 (leaving headroom for
other queries). Setting `max_workers` above the pool size causes shards
to wait for a connection rather than doing useful work.

## Switching to process-based execution

By default each shard runs in a thread. If your database driver leaks
memory across queries — or if per-shard memory pressure is severe — switch
to a subprocess pool:

```python
shard_options=ShardOptions(
    max_workers=4,
    execution_mode="processes",
)
```

In `"processes"` mode each shard runs in a fresh interpreter subprocess.
Memory is reclaimed when the subprocess exits. The trade-off: subprocess
startup overhead (~0.5–1 s per shard) and a constraint on `transform_fn`.

**Constraint**: when `execution_mode="processes"`, `transform_fn` must be
a module-level callable — not a lambda and not a closure. Lambdas and
closures cannot be pickled by `multiprocessing`.

```python
# Good — top-level function, picklable
def my_transform(tbl):
    return tbl.rename_columns({"ts": "event_time"})

# Bad — lambda, will raise PicklingError in processes mode
transform_fn=lambda tbl: tbl.rename_columns({"ts": "event_time"})
```

## Checking results per shard

The operator pushes a dict to XCom on success. Each shard is an entry
in `shards`:

```python
result = context["ti"].xcom_pull(task_ids="events_by_region")
for shard in result["shards"]:
    print(shard["shard_index"], shard["rows"], shard["remote_uri"])
```

## See also

- [How-to → Tune performance](tune-performance.md): `chunk_rows`,
  `max_workers`, compression choices.
- [How-to → Handle failures](handle-failures.md): per-shard timeouts and
  sibling-cancellation behaviour.
- [How-to → Partition Hive-style](partition-hive-style.md): combine
  sharding with Hive path conventions for Athena / Trino.
- [Reference → ShardOptions](../reference/shard-options.md).
