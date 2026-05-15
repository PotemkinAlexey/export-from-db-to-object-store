# The Streaming Pipeline

In streaming mode, each shard executes an identical pipeline: rows flow out of a database cursor, are assembled into Arrow batches, optionally transformed, written to a local Parquet file, validated, and uploaded to object storage. This document explains why the pipeline is structured this way — the reasoning behind each stage and the tradeoffs each design choice encodes.

## The Database Abstraction

The operator does not know which database it is reading from. It interacts with the database through an Airflow hook, which in turn exposes a PEP-249-compatible cursor. Any Airflow connection type that provides `get_records` or the `execute`/`fetchmany` pair works without modification: PostgreSQL, MySQL, Snowflake, Oracle, Redshift in JDBC mode, and others.

The specific fetch strategy depends on what the hook provides. Some hooks buffer the entire result set server-side before returning; others support true server-side cursors that stream rows incrementally. The operator cannot control which strategy the hook uses — it can only control how many rows it requests at a time via `chunk_rows`. For databases that support server-side cursors (PostgreSQL's `server_side_cursors=True` mode, for example), `chunk_rows` directly controls network round-trips and peak memory. For databases that materialize server-side, `chunk_rows` only controls client-side batch assembly.

This abstraction is why `chunk_rows` is a tuning knob rather than a fixed constant. The right value depends on the database driver's cursor behavior, the row width, available memory, and acceptable round-trip overhead.

## Arrow Batch Assembly

Rows arrive as Python sequences (lists of tuples, typically). The worker assembles them into `pa.RecordBatch` objects using PyArrow's schema inference or an explicit schema if one is configured. Arrow's columnar layout is the key property here: values in the same column are stored contiguously in memory, which makes compression and encoding dramatically more effective than row-oriented layouts.

The choice of Arrow as the in-memory representation is not incidental. Arrow is PyArrow's native format, so the conversion from a batch to a Parquet row group is a near-zero-copy operation — PyArrow writes Arrow buffers directly into the Parquet encoding layer without materializing a row-oriented intermediate. For wide tables with many numeric columns, this makes the write path substantially faster than an approach that serializes rows one at a time.

## Why chunk_rows Exists

`chunk_rows` is a single knob that controls the fundamental memory-vs-round-trip tradeoff in the pipeline.

A smaller `chunk_rows` means the worker holds fewer rows in memory at any moment. For a table with 1000-byte average row width and `chunk_rows=1000`, peak in-memory row data is roughly 1 MB per batch (before columnar compression). This is appropriate when memory pressure is a concern — many shards running concurrently, each with wide rows.

A larger `chunk_rows` means fewer round-trips to the database (for drivers that stream), better Arrow encoding efficiency (more values per column means better delta/dictionary encoding), and fewer Parquet row group boundaries. The downside is higher peak memory and a longer time between cancel-event checks — a shard in the middle of assembling a large batch cannot observe the cancel event until the batch is complete.

The tension between these forces is why there is no universally correct value. Wide rows with many shards want small `chunk_rows`; narrow rows with few shards want large `chunk_rows`.

## Parquet Row Groups and Footer Layout

Each call to `ParquetWriter.write_batch()` produces one Parquet row group in the output file. The Parquet format encodes row groups independently: each row group has its own column statistics (min/max values), dictionary encodings, and compression blocks. This means the row group count in the output file equals the number of chunks fetched from the database.

The Parquet footer — written at the end of the file — contains the row group metadata, column schemas, and offsets. This is what makes Parquet a "footer-first" format: readers typically read the footer first to understand the file's structure, then seek to specific row groups without reading the entire file.

The implication is that a Parquet file is not fully readable until the footer is written. If the writer process is killed after writing row groups but before closing the `ParquetWriter`, the footer will be absent and the file will be corrupt. This is one reason why validation runs after the file is closed.

## Validation: Catching Truncated Writes

After the `ParquetWriter` is closed, the worker performs a validation step. It reads the file's footer using PyArrow and performs a sample read of the first row group. This is not a full re-read of the data — it is a sanity check designed to catch the most common failure modes:

- **Truncated writes**: If the file system ran out of space mid-write, the file will be shorter than expected and the footer read will fail.
- **Corrupt footer**: A write interrupted at exactly the wrong moment (during footer serialization) can produce a syntactically invalid footer that PyArrow will reject.
- **Driver-level buffering bugs**: Some storage backends buffer writes and flush lazily; the sample read forces a flush and confirms that the data is actually on disk.

The validation does not check data correctness — it cannot, because it has nothing to compare against. It checks structural integrity: the file can be opened, the footer parses, and at least one row group can be read. A file that passes validation is a valid Parquet file; whether it contains the right data is a question for downstream consumers.

## The Sentinel Thread Pattern

Inside `ShardWorker`, the reader (DB fetch loop) and the writer (Parquet write loop) communicate through a queue. The reader puts Arrow batches onto the queue; the writer reads from it and calls `write_batch()`. The sentinel thread exists to handle one specific failure mode: the reader dying mid-stream.

If the DB fetch loop raises an exception after putting some batches onto the queue, the writer will drain those batches and then block indefinitely on `queue.get()`, waiting for a batch that will never arrive. Without the sentinel, the shard would hang forever.

The sentinel is a small thread that monitors the reader. When the reader finishes — either successfully or with an exception — the sentinel puts a sentinel value (a distinguished object, not a real batch) onto the queue. The writer recognizes the sentinel and exits its loop. If the reader failed, the writer then re-raises the reader's exception.

This pattern is necessary because Python queues do not have a native "closed" state. The sentinel value is the conventional solution: it converts the "queue is done" condition into a value the consumer can observe in normal flow, without polling or timeouts.

The cancel event (in thread mode) is checked between chunks in the writer loop, not in the reader loop. This placement is deliberate: the writer is the natural checkpoint because it touches each batch exactly once, and checking after each write keeps the shard responsive to cancellation without requiring the reader to know about the cancel event.

## Upload Retry Loop

After validation, the worker uploads the local Parquet file to object storage. Uploads are retried on transient failures using exponential backoff with a cap:

```
delay = min(backoff_base ^ attempt, backoff_cap)
```

Both `backoff_base` and `backoff_cap` are configurable via `RetryOptions`. The cap exists because exponential growth without a ceiling would produce impractically long waits on later attempts. The base and cap together define the retry envelope: a base of 2.0 and a cap of 60 seconds means attempt 0 waits 1 second, attempt 1 waits 2 seconds, attempt 2 waits 4 seconds, and all subsequent attempts wait 60 seconds.

Retries are unconditional on the configured error types. The operator does not attempt to distinguish idempotent from non-idempotent upload errors — a partial upload followed by a retry may produce a complete upload (object stores typically treat a PUT as atomic), and the existence check (`skip_if_exists`) at the start of the next DAG run will detect and skip already-uploaded shards. Retrying a failed upload within the same run is therefore safe, and the retried upload, if successful, produces a correct result.
