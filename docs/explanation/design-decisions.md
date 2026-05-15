# Design Decisions

Every non-trivial design choice in this operator reflects a tradeoff. This document explains the reasoning behind seven decisions that shape how the operator behaves, what it costs to run, and how it can be extended.

## Streaming Over Buffering

The pipeline reads rows from the database in chunks and writes them to Parquet incrementally, without ever materializing the full result set in memory. This is the streaming model. The alternative — the buffering model — would read all rows into a Python list or pandas DataFrame, then write the file in one operation.

Buffering is simpler to implement and easier to reason about, but it has an unbounded memory cost. A table with 100 million rows and 500 bytes per row requires roughly 50 GB of memory to buffer completely. No Airflow worker should need 50 GB of heap for a single export task.

Streaming with Arrow's columnar IPC makes the memory cost proportional to `chunk_rows`, not to the total result set size. The `chunk_rows` parameter becomes the only tunable knob for the memory-vs-throughput tradeoff, and the correct value is typically in the range of thousands to tens of thousands of rows — memory measured in tens to hundreds of megabytes, not gigabytes.

The consequence of this choice is that the streaming pipeline is more complex than a buffering one. There is a sentinel thread, a queue, a cancel event, and a per-chunk loop to manage. This complexity is the price of bounded memory, and it is worth paying for any table larger than what comfortably fits in worker memory.

## Threads Over asyncio

Airflow operators are synchronous Python functions. They run in a thread managed by the Airflow executor, and they are expected to block until the export is complete. Parallelizing multiple shards requires either threads, processes, or asyncio.

asyncio would require the operator to run an event loop, which in turn requires the Airflow executor thread to yield control to the loop's scheduler. This is architecturally awkward in a synchronous operator context — Airflow does not provide an event loop, so the operator would have to create one, run it to completion, and tear it down. More importantly, asyncio compatibility is not universal across Airflow hooks: most hooks are synchronous and blocking, and wrapping them in `asyncio.run_in_executor` would achieve nothing more than threading with extra ceremony.

Threading is the natural choice because the hot path releases the GIL. Arrow, PyArrow's Parquet writer, and the major cloud storage SDKs all perform their I/O and computation in C extensions that release the GIL. Threads do not need to be truly parallel in the Python sense — they need to overlap their waiting periods, and they achieve this naturally when the GIL is not held during the waits. The result is genuine concurrency without the architectural complexity of asyncio.

Process-based parallelism remains available for workloads that need hard memory isolation, at the cost of higher overhead. Threading is the default because it works well for the common case.

## Protocols Over Inheritance

The `Uploader` and `UnloadStrategy` extension points are defined as `@runtime_checkable` Protocol classes, not abstract base classes. Plugin authors implement the required methods; they do not import or subclass anything from this package.

The inheritance-based alternative would require plugin authors to depend on this package and subclass `BaseUploader` or `BaseUnloadStrategy`. This creates a coupling between plugin versions and core package versions. A plugin built against version 1.x of the core package would break if the base class signature changed in version 2.x, even if the change was backward-compatible from the core's perspective.

Protocol-based extension avoids this coupling. A plugin implements the documented interface — a small set of methods with specific signatures — and the runtime check (`isinstance(obj, Uploader)`) verifies compliance without any inheritance relationship. Plugin authors can implement the interface using whatever base classes or libraries they choose, and they can upgrade the core package without rebuilding their plugins as long as the protocol is stable.

The built-in implementations come first in the registry resolution order. This is a deliberate safety measure: plugin authors cannot accidentally shadow the built-in S3, GCS, or Azure uploaders by registering a plugin with the same hook class. The built-in wins, and the plugin is only consulted for hook types the built-in does not recognize.

## Frozen Dataclasses for Options

`ParquetOptions`, `ShardOptions`, `RetryOptions`, and `ShardTaskParams` are all defined as `frozen=True` dataclasses. Once constructed, they cannot be mutated.

The alternative — mutable dataclasses or plain dicts — would allow the operator to modify options mid-run. This is almost never the right thing to do and is frequently the source of subtle bugs: a shard that modifies a shared options object would affect all other shards. In process mode, this particular bug is impossible because each process gets its own copy of the pickled options, but it would silently corrupt results in thread mode.

Frozen dataclasses make mutation a runtime error. The bug is caught immediately, on the line that attempts the mutation, rather than later when a different shard produces unexpected results. This property is especially valuable in process mode, where the frozen dataclass is pickled and sent to a worker — the pickled object is naturally immutable (it is a fresh deserialized copy), and the frozen constraint ensures that the original and the copy have the same invariants.

## Manifest as Atomic Catalog

The manifest is written after all shards complete successfully, as a single atomic write. If any shard fails, no manifest is written for that run. Downstream readers that use the manifest to discover data will never see partial results.

The alternative — writing the manifest incrementally as each shard completes — would allow downstream readers to see partial data. This is sometimes desirable (earlier visibility into completed shards), but it creates a correctness problem: a downstream reader that begins processing the manifest while shards are still running may observe a dataset that is internally inconsistent. Row counts may not sum correctly. Partitions may be missing.

Atomic manifest writes are the standard pattern in object store-based data lakes (the "landing zone with sentinel file" pattern). The manifest is the sentinel: its presence signals that the dataset is complete and consistent. Its absence signals that the run has not finished. This binary signal is simpler and more reliable than a progress indicator, and it requires no coordination between the manifest writer and downstream readers.

The consequence is that the manifest is rewritten on every successful run, not appended. This means readers cannot rely on the manifest accumulating across runs — each run's manifest is a complete description of that run's output, not a cumulative history. For cumulative history, downstream consumers should query a catalog table or union multiple manifests by run date.

## Watermark Pushed Only on Genuine Upload

The watermark is pushed to XCom only when at least one shard performed a genuine upload. A run where all shards were skipped via `skip_if_exists` does not advance the watermark.

The alternative — always pushing the watermark regardless of whether any data moved — is simpler to implement (no conditional logic in `_commit_watermark`) but produces a misleading XCom history. Every skipped run would advance the watermark, creating a gap between the last genuinely exported row and the current watermark anchor. If a future run is not skipped (because the objects were deleted or the skip check was disabled), it would miss the rows that correspond to the skipped runs' windows.

Conditional watermark push is the conservative correctness choice. It ensures that the watermark history in XCom is a faithful record of what was actually exported, not a record of when the operator ran.

## Entry-Point Plugin Discovery With Fault Isolation

Plugins are discovered via Python package entry points, and bad entry points are logged and skipped rather than raising an exception that would abort the operator load.

The alternative — failing fast on a broken plugin — is safer in the sense that it makes plugin problems immediately visible. But in a shared Airflow environment where dozens of teams install packages into the same Python environment, a broken plugin from an unrelated team would prevent all exports from running, not just the ones that use the broken plugin.

Fault-isolated discovery means a bad entry point produces a warning in the logs and is silently excluded from the registry. Operators that do not use the broken plugin are unaffected. Operators that do use it will fail at runtime with a "no uploader found" error that clearly identifies the missing hook type, rather than failing at import time with an obscure traceback from a plugin's `__init__.py`.

This choice accepts a tradeoff: broken plugins are harder to catch in testing because the failure is silent at import time. The mitigation is that the warning is emitted at startup, so it appears in the Airflow worker logs on first use and can be caught in integration tests that inspect log output.
