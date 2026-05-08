# airflow-export-to-object-store

[![PyPI version](https://img.shields.io/pypi/v/airflow-export-to-object-store.svg)](https://pypi.org/project/airflow-export-to-object-store/)
[![Python versions](https://img.shields.io/pypi/pyversions/airflow-export-to-object-store.svg)](https://pypi.org/project/airflow-export-to-object-store/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Universal streaming Airflow operator: execute SQL on **any** PEP-249 / Airflow-hooked database,
stream results as Apache Arrow batches, write them to **Parquet**, and upload to
**Azure Blob Storage**, **AWS S3** or **Google Cloud Storage** — with sharding,
retries, auto-tuned chunks and rich metrics.

Designed for high-volume exports with a **minimal memory footprint**.

---

## Features

- **Database-agnostic** — Postgres, Snowflake, Databricks SQL, Teradata, or anything DB-API.
- **Arrow-native streaming** — no full in-memory materialisation; auto-tuned batch size.
- **Parquet writer** — configurable compression (zstd by default), row group size, timestamp coercion.
- **Sharding** — parallel shard execution via `ThreadPoolExecutor`.
- **Retry logic** — unified `@with_retries` with exponential backoff.
- **Object storage** — Azure Blob (simple + block-list for >5 GB), AWS S3 (boto3 multipart), Google Cloud Storage (resumable multipart via `GCSHook.upload`).
- **Health checks** — network, memory, disk, container/bucket connectivity.
- **Per-shard metrics** — pushed to XCom, plus a colourful ASCII summary in logs.
- **Jinja templates** — full Airflow macros support (`{{ ds }}`, etc.) in SQL and remote paths.
- **Parquet validation** — footer / row-groups / sample-read sanity checks before upload.
- **Idempotent re-runs** — `skip_if_exists=True` short-circuits any shard whose remote object is already in place (no DB cursor opened, no upload).
- **Manifest** — `write_manifest=True` emits `_manifest.json` next to the data with file list, sizes, MD5s, totals — atomic catalog for downstream consumers.

## Installation

```bash
pip install airflow-export-to-object-store
```

With backend extras:

```bash
pip install "airflow-export-to-object-store[azure]"        # Azure Blob
pip install "airflow-export-to-object-store[s3]"           # AWS S3
pip install "airflow-export-to-object-store[gcs]"          # Google Cloud Storage
pip install "airflow-export-to-object-store[snowflake]"    # Snowflake driver
pip install "airflow-export-to-object-store[postgres]"     # psycopg2
pip install "airflow-export-to-object-store[databricks]"   # Databricks SQL
pip install "airflow-export-to-object-store[teradata]"     # Teradata
pip install "airflow-export-to-object-store[memcheck]"     # psutil for memory health checks
```

## Quick start

```python
from airflow import DAG
from airflow_export_to_object_store import (
    StreamingExportOperator,
    ParquetOptions,
    ShardOptions,
)

with DAG("example_export", schedule_interval=None) as dag:
    export = StreamingExportOperator(
        task_id="export_orders",
        db_hook_id="snowflake_conn",
        storage_hook_id="azure_blob_conn",
        sql_template="SELECT * FROM orders WHERE date = '{{ ds }}'",
        container="data-exports",
        remote_path_template="orders/{{ ds }}/data_{{ shard_index }}.parquet",
        parquet_options=ParquetOptions(compression="zstd", row_group_size=512_000),
        shard_options=ShardOptions(max_workers=6, chunk_rows=50_000),
    )
```

### Sharded export

```python
StreamingExportOperator(
    task_id="export_sharded",
    db_hook_id="postgres_conn",
    storage_hook_id="aws_s3_conn",
    bucket="my-data-lake",
    sql_template="""
        SELECT * FROM events
        WHERE event_date = '{{ ds }}'
          AND mod(id, {{ shards_total }}) = {{ shard_id }}
    """,
    shards=[
        {"shard_id": i, "shards_total": 8} for i in range(8)
    ],
    remote_path_template="events/{{ ds }}/part_{{ '%03d' | format(shard_index) }}.parquet",
)
```

## Idempotency and manifest

```python
StreamingExportOperator(
    task_id="export_orders",
    db_hook_id="snowflake_default",
    storage_hook_id="aws_default",
    bucket="my-data-lake",
    sql_template="SELECT * FROM orders WHERE date = '{{ ds }}'",
    remote_path_template="orders/{{ ds }}/data.parquet",
    skip_if_exists=True,         # safe to re-run / clear / retry
    write_manifest=True,          # writes orders/{{ ds }}/_manifest.json
)
```

`skip_if_exists` probes the destination via the backend's `head_object` /
`exists()` API before opening any DB cursor. If the file is already there
the shard returns immediately with `ShardResult.skipped=True` and zero
rows/bytes. Combined with deterministic `remote_path_template` this makes
clear-and-retry of partially-failed exports safe by default.

`write_manifest` writes a small JSON catalog at the common prefix of all
shard paths (or at `manifest_path` if you set it explicitly):

```json
{
  "version": 1,
  "exported_at": "2026-05-08T12:34:56+00:00",
  "total_rows": 1500000,
  "total_bytes": 1234567890,
  "files": [
    {"shard_index": 0, "remote_uri": "s3://...", "rows": 250000, "bytes": 205678123, "md5": null, "skipped": false}
  ]
}
```

Downstream readers (Athena, Trino, Spark, schema registries) can act on
the manifest atomically without listing the bucket.

## Native unload (warehouse → bucket, server-side)

Streaming through this process is fine for tens of millions of rows. For
hundreds of millions or terabytes, ask the warehouse to write Parquet
**directly** to the bucket — orders of magnitude faster, no client-side
fetch at all.

```python
from airflow_export_to_object_store import StreamingExportOperator
from airflow_export_to_object_store.unload import (
    SnowflakeUnloadOptions,
    SnowflakeUnloadStrategy,
)

StreamingExportOperator(
    task_id="export_orders_native",
    db_hook_id="snowflake_default",
    storage_hook_id="aws_default",
    bucket="my-data-lake",
    sql_template="SELECT * FROM orders WHERE date = '{{ ds }}'",
    unload_dir_template="orders/{{ ds }}/",   # files go here
    write_manifest=True,                       # _manifest.json next to them
    unload_strategy=SnowflakeUnloadStrategy(
        SnowflakeUnloadOptions(
            storage_integration="MY_S3_INT",   # preferred, set by Snowflake admin
            compression="ZSTD",
            max_file_size=256 * 1024 * 1024,   # ~256 MiB per file
        ),
    ),
)
```

The strategy:
* runs `COPY INTO 's3://bucket/orders/{{ ds }}/' FROM (...)` on Snowflake's
  warehouses (parallelised across compute nodes),
* parses the result set so each produced file becomes one `ShardResult`,
* feeds the same manifest writer used by the streaming path.

Auth: prefer `storage_integration` over inline `credentials={...}` —
the integration is set up by a Snowflake admin once and zero secrets
cross the SQL boundary.

Today the Snowflake strategy supports S3 and GCS targets out of the box;
Azure unload requires the storage account name (open an issue with your
setup if you need it).

## Plugins (third-party uploaders)

Need an in-house S3-compatible store, custom DB-API bridge, or weird
cloud? Register an `Uploader` from your package via entry points:

```toml
# your_pkg/pyproject.toml
[project.entry-points."airflow_export_to_object_store.uploaders"]
my_storage = "my_pkg.uploaders:MyUploader"
```

The class (or zero-arg factory) is auto-discovered when this package
loads. Built-in backends still take priority; bad plugins are logged
and skipped — they cannot break the operator.

## Tracing (OpenTelemetry)

Install the optional extra:

```bash
pip install "airflow-export-to-object-store[otel]"
```

The operator emits spans named `export.execute`, `export.run_shards`,
`export.shard`, `export.shard.validate`, `export.shard.upload`, and
`export.unload` with attributes such as `shard.index`, `shard.rows`,
`shard.bytes`, `unload.strategy`, `export.task_id`. With Airflow 2.10+'s
built-in OTel exporter (or your own SDK setup) these light up
automatically. Without `opentelemetry-api` installed the helpers are
no-ops with negligible overhead.

## Configuration objects

| Object | Key fields |
|---|---|
| `ParquetOptions` | `compression`, `row_group_size`, `coerce_timestamps`, `write_statistics`, `use_dictionary` |
| `RetryOptions` | `upload_retries`, `backoff_base`, `backoff_cap` |
| `ShardOptions` | `max_workers`, `chunk_rows`, `memory_limit_mb`, `timeout`, `execution_mode` |

## Concurrency model

By default (`execution_mode="threads"`) shards run on a `ThreadPoolExecutor`
inside the Airflow task process. The hot path — DB I/O, Arrow batch
construction, dictionary decode, schema cast, zstd-compressed Parquet writes,
and cloud uploads — all release the GIL because it lives in C/C++ extensions
(PyArrow, hashlib, boto3/azure-storage/google-cloud network libs). Threads
therefore scale on real workloads while keeping a single Airflow process,
shared connection pool, and trivial log capture.

When any shard fails, the operator sets a shared cancellation `Event`; running
shards observe it on the next iteration of fetch / write / queue-put, drop
their in-flight batch, and exit promptly without uploading. Failed task time
is bounded by one chunk's runtime instead of the slowest survivor.

Set `execution_mode="processes"` when you need hard isolation between
shards — typically for memory-leaky DB drivers, or when one shard's OOM
must not take down the rest. In that mode the operator uses a
`ProcessPoolExecutor`. Trade-offs:

- **Memory**: each subprocess re-imports Airflow + PyArrow + driver
  (≈ 200–500 MiB resident).
- **No cross-shard cancellation**: `threading.Event` is process-local, so
  already-running shards in subprocesses run to completion. Only
  not-yet-started futures are cancelled.
- **Pickling**: shard parameters and `ShardResult` cross the process
  boundary as pickles. Both are simple frozen dataclasses, so this is
  cheap, but custom subclasses must be picklable too.

## Output (XCom)

```python
{
    "shards": [{"shard_index": 0, "remote_uri": "s3://...", "rows": ..., "bytes": ..., "md5": ..., "elapsed_s": ...}],
    "metrics": {...},
    "total_rows": ...,
    "total_bytes": ...,
    "elapsed_s": ...,
}
```

## Development

```bash
git clone https://github.com/PotemkinAlexey/export-from-db-to-object-store.git
cd export-from-db-to-object-store
pip install -e ".[dev,azure,s3]"
pre-commit install
pytest
```

## License

Apache License 2.0 — see [LICENSE](LICENSE).
