# Concurrency Model

The operator exports multiple shards concurrently. Understanding why it uses threads by default, what changes when you switch to processes, and how failure in one shard propagates to the others is important for both tuning and debugging.

## Why Threads Work Despite the GIL

Python's Global Interpreter Lock means only one Python thread runs at a time. The conventional wisdom is therefore that threading is not useful for CPU-bound work in Python. The export pipeline is not CPU-bound.

The hot path in each shard is dominated by three operations: fetching rows from the database over the network, serializing those rows into Parquet via PyArrow, and uploading the resulting file to cloud storage. All three release the GIL:

- **PyArrow** performs its columnar operations in C++ and releases the GIL for the duration of batch assembly and Parquet serialization.
- **boto3 / google-cloud-storage / azure-storage-blob** release the GIL during socket I/O (they delegate to OpenSSL's C layer for TLS).
- **Database drivers** (psycopg2, cx_Oracle, the Snowflake connector) release the GIL during cursor execution and fetch operations.

The result is that two shards running in separate threads will genuinely overlap their I/O waiting periods. Thread 1 can be fetching the next chunk from the database while Thread 2 is writing its batch to the Parquet file, and Thread 3 is uploading a completed file to S3 — with actual CPU parallelism on the C-extension work. The GIL is only held during pure Python glue code between these operations, which is a small fraction of wall-clock time.

This is why threading is the right default: it achieves real concurrency with low overhead and no architectural complexity.

## What Changes in Process Mode

Process mode (`executor=processes`) swaps the `ThreadPoolExecutor` for a `ProcessPoolExecutor`. Each shard worker runs in a separate operating system process. The benefit is hard isolation: a shard that leaks memory, opens a connection to a pathological database driver, or triggers a segfault in a native extension cannot affect its siblings. In thread mode these failure modes can corrupt the shared process state or cause silent data loss.

The cost of this isolation is substantial:

**Memory**: Each worker process is a full Python interpreter with its own copy of the operator's in-memory state. On a machine running 8 shards in process mode, you pay 200–500 MB of resident memory per worker for interpreter overhead alone, before any data is loaded.

**Pickling**: `ShardTaskParams` must cross the process boundary via pickle. The frozen dataclass design makes this reliable — frozen dataclasses with primitive fields, options dataclasses, and path objects are all straightforwardly picklable. The same constraint applies to `ShardResult`. The implication for `transform_fn` is significant and deserves its own section.

**No shared cancel event**: In thread mode, the operator holds a `threading.Event` object that all shard threads share. A shard can observe `cancel.is_set()` between chunks and exit cleanly when a sibling fails. `threading.Event` cannot cross a process boundary, so in process mode there is no cooperative cancellation signal. A shard running in a child process runs to completion regardless of what happens in sibling processes.

## Failure Propagation: Threads vs Processes

The operator uses `concurrent.futures.wait(futures, return_when=FIRST_EXCEPTION)` in both modes. This call returns as soon as any future raises an exception, yielding two sets: `done` (completed or failed futures) and `not_done` (still running or not yet started).

**In thread mode**, the operator responds to a failure by calling `cancel.set()`. Shards that are already running observe this event in their per-chunk loop and exit early. Shards in `not_done` that have not yet started are cancelled via `f.cancel()`. The operator then calls `wait(not_done)` again to collect them before propagating the exception. The result is a clean, prompt shutdown: running shards drain their current chunk and exit, queued shards never start.

**In process mode**, `cancel.set()` has no effect across processes. The operator still calls `f.cancel()` on not-yet-started futures — a process that has not yet been launched will not be launched. But a process that is already running will run to its natural completion (success or failure). The operator must wait for these processes to finish before it can report the failure. This means that in process mode, a single shard failure can leave several other shards running for their full duration before the operator terminates.

This is a deliberate asymmetry, not an oversight. The alternative — sending SIGTERM to running worker processes — would risk leaving temporary files on disk, open database cursors unfinished, and partial uploads in object storage. Letting already-running shards complete is a cleaner failure mode, even though it delays the error report.

`wait(return_when=FIRST_EXCEPTION)` is used instead of `as_completed` because `as_completed` requires consuming results one by one in a loop, which means the loop itself must handle the failure case and cancel remaining futures. The `wait` API makes the "detect first failure, then decide" pattern explicit and readable, and it avoids the asymmetry between the first failure and subsequent ones that an `as_completed` loop would introduce.

## The transform_fn Pickling Constraint

When running in process mode, `transform_fn` is part of `ShardTaskParams` and must be pickled to be sent to the worker process. Python's pickle protocol can serialize top-level module-level callables (regular functions defined at the module level, classes with `__call__`, and `functools.partial` applied to any of the above). It cannot serialize lambdas or closures — functions that capture variables from an enclosing scope.

This is not a limitation of the operator's design; it is a fundamental property of Python's object serialization. The operator cannot work around it without abandoning the process boundary model entirely.

In thread mode there is no such constraint. The transform function is simply a callable in a shared memory space, and any callable works — lambdas, closures, bound methods, local functions. The pickling constraint only applies in process mode.

If a DAG requires a transform that involves closures (for example, a parameterized transform that captures configuration from the DAG definition), the solution is to wrap it in a top-level callable that accepts the parameters as constructor arguments. This is a picklable pattern because the class and its `__call__` method are module-level, and the captured parameters become instance attributes that pickle handles naturally.
