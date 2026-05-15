# Architecture of the Streaming Export Operator

The `StreamingExportOperator` is an Airflow operator designed to move data from a relational database into an object store — reliably, incrementally, and at scale. Understanding its architecture means understanding the two fundamentally different paths data can take through it, and why both paths share the same bookkeeping surface.

## Two Modes, One Interface

The operator runs in one of two modes: **streaming mode** and **native unload mode**. Which mode is active depends entirely on whether an `unload_strategy` is configured. In most deployments the distinction is invisible to the DAG author — the XCom result shape, the manifest file, the watermark lifecycle, and the tracing output are identical regardless of which path was taken.

**Streaming mode** is the default. The operator itself moves the data: it opens a database cursor, reads rows into Arrow batches, writes a local Parquet file, validates it, and uploads the result to object storage. The operator is the data mover.

**Native unload mode** delegates the movement entirely to the warehouse. When a `BigQueryUnloadStrategy` or `RedshiftUnloadStrategy` is configured, the operator renders the SQL and destination path, calls `strategy.unload(...)`, and receives back a list of `ShardResult` objects describing what the warehouse wrote. The operator never touches a row of data. This path exists because warehouses like BigQuery and Redshift can export directly to cloud storage at 10–50x the throughput achievable through a Python process — the bottleneck in streaming mode is always the operator's network connection and serialization overhead, not the database.

The choice between modes is about **where the bottleneck lives**. For modest data volumes or when the warehouse's native export is unavailable, streaming mode is flexible and needs nothing beyond an Airflow hook and object store credentials. For terabyte-scale exports, native unload is the only practical option.

## Layers in the Streaming Path

Streaming mode is layered deliberately, so that each layer has a single responsibility.

```
Operator (StreamingExportOperator)
  │
  ├─ Pre-flight: resolve uploader, run health checks
  ├─ Incremental: read previous watermark from XCom, resolve current watermark
  ├─ Template rendering: SQL + path per shard → ShardTaskParams
  │
  └─ Executor (ThreadPoolExecutor or ProcessPoolExecutor)
       │
       └─ ShardWorker (per shard)
            │
            DB cursor
              │
              └─ Arrow batches (chunk_rows rows at a time)
                   │
                   └─ transform_fn (optional, per batch)
                        │
                        └─ ParquetWriter → local .parquet file
                             │
                             └─ validation (footer + sample read)
                                  │
                                  └─ Uploader → object store
```

The **operator** is the orchestrator. It knows about Airflow: XCom, task instances, hooks, DAG run context, Jinja templating. It sets up the concurrency pool, dispatches shards, collects results, writes the manifest, and commits the watermark. It does not know how to write a Parquet row.

The **ShardWorker** is the hot loop. It knows nothing about Airflow, watermarks, or manifests. It receives a `ShardTaskParams` — a frozen, self-contained description of exactly one shard's work — and it produces a `ShardResult`. Its only concern is getting rows out of the database and bytes into an object.

**ShardTaskParams** is the seam between these two concerns. It is a frozen dataclass, which makes it safely picklable (required for process-pool mode) and prevents accidental mutation between the operator and the worker. Everything the worker needs — the rendered SQL query, the local temp file path, the remote destination path, the Parquet and retry options, the cancel event if running in thread mode — is carried in `ShardTaskParams`.

**Uploaders** are protocol-based plugins. The `Uploader` protocol defines a small interface (`upload`, `exists`, `head`), and the built-in implementations for S3, GCS, and Azure are discovered through Python entry points. The operator resolves the right uploader by inspecting the storage hook type at startup. Plugin authors implement the protocol without inheriting from any base class — duck typing enforced at runtime via `@runtime_checkable`.

## What the Two Modes Share

The operator's bookkeeping layer sits above both modes and is unaware of which path was taken:

- **Manifest**: Written after all shards complete, the manifest is a JSON catalog of every object produced in the run. It records paths, row counts, byte sizes, and checksums. Whether the warehouse wrote those objects or the operator did is not represented in the manifest.

- **Watermark**: The incremental state machine reads a previous watermark from XCom before any data moves and pushes a new watermark after all data has moved successfully. This happens whether streaming or native unload produced the data.

- **XCom result**: The operator pushes a summary dict — shard count, total rows, total bytes — back to XCom so downstream tasks and monitoring can consume it without reading the manifest.

- **Tracing**: Span annotations are emitted at the operator and shard levels, and both modes produce them at the same points.

This shared surface is why the two modes feel like one operator rather than two separate implementations. The DAG author chooses the data movement strategy; everything else — idempotency, observability, downstream catalog — remains stable.

## Pre-flight as a First-Class Phase

Before any data moves, the operator runs health checks: a network probe to the object store endpoint, a bucket HEAD request to verify credentials and permissions, a disk space check against the configured temp directory, and a memory headroom check. These checks exist because the failure modes they catch are otherwise silent until the middle of a large export — discovering that the bucket is inaccessible after 20 minutes of DB reads is a worse outcome than failing immediately at pre-flight.

The health checks are not idempotency checks. They do not determine whether the data already exists. Their purpose is to catch environmental problems — wrong credentials, full disk, network partition — before any work begins.
