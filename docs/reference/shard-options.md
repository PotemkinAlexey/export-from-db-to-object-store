# ShardOptions

Controls how shards are executed in parallel: worker count, chunk size, memory cap, timeout, and the underlying executor type.

Pass an instance to `StreamingExportOperator(shard_options=...)`.

```python
from airflow_export_to_object_store.options import ShardOptions

shard_options = ShardOptions(
    max_workers=4,
    chunk_rows=100_000,
    memory_limit_mb=2048,
    timeout=300.0,
    execution_mode="threads",
)
```

## Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_workers` | `int` | `6` | Maximum number of shards processed concurrently. Each worker reads, writes Parquet, and uploads one shard. |
| `chunk_rows` | `int` | `50_000` | Number of rows fetched from the database per round trip. Smaller values reduce peak memory; larger values reduce round-trip overhead. |
| `memory_limit_mb` | `int` | `1024` | Soft memory ceiling in mebibytes per worker. The operator checks resident memory and pauses shard submission when the limit is approached. Not a hard OS-level limit. |
| `timeout` | `float \| None` | `None` | Wall-clock timeout in seconds for the entire shard phase. `None` means no timeout. When the timeout is exceeded the executor is shut down and a `TimeoutError` is raised. |
| `execution_mode` | `ExecutionMode` | `"threads"` | Executor backend. See the modes table below. |

## Execution modes

| Mode | Executor | GIL | Resident memory | Serialization | Best for |
|------|----------|-----|----------------|---------------|----------|
| `"threads"` | `ThreadPoolExecutor` | Arrow/Parquet/cloud-IO operations release the GIL; CPU-bound Python code does not. Threads share the process address space. | Low — shared heap. | None — objects passed by reference. | Most workloads. Network-bound uploads and Arrow operations scale well with threads. |
| `"processes"` | `ProcessPoolExecutor` | No GIL contention — each worker is a separate OS process. | High — 200–500 MB resident overhead per worker beyond data. | Full pickle round-trip for arguments and return values. | Workloads with heavy CPU-bound Python transforms (`transform_fn`) that do not release the GIL. |

**Choosing between modes.** Start with `"threads"`. The GIL is not a bottleneck for Arrow serialization, Parquet writes, or cloud SDK calls because those operations release the GIL. Switch to `"processes"` only when profiling shows that a CPU-bound `transform_fn` is saturating a single core. The memory and pickling overhead of `"processes"` is substantial: `max_workers=6` with `"processes"` adds roughly 1.2–3 GB of resident memory before any data is loaded.

## Dataclass signature

```python
@dataclass(frozen=True)
class ShardOptions:
    max_workers: int = 6
    chunk_rows: int = 50_000
    memory_limit_mb: int = 1024
    timeout: float | None = None
    execution_mode: ExecutionMode = "threads"
```

`ExecutionMode = Literal["threads", "processes"]`

`ShardOptions` is frozen; all fields must be set at construction time.
