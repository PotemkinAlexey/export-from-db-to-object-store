# airflow-export-to-object-store

[![PyPI version](https://img.shields.io/pypi/v/airflow-export-to-object-store.svg)](https://pypi.org/project/airflow-export-to-object-store/)
[![Python versions](https://img.shields.io/pypi/pyversions/airflow-export-to-object-store.svg)](https://pypi.org/project/airflow-export-to-object-store/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

A single Airflow operator that streams any SQL query into Apache Parquet
files in **AWS S3**, **Azure Blob Storage**, or **Google Cloud Storage** —
with sharding, retries, idempotent re-runs, manifests, native
warehouse-side unload, watermark-based incremental exports, and a small
extension surface for custom backends and row-level transforms.

```python
from airflow_export_to_object_store import StreamingExportOperator

export = StreamingExportOperator(
    task_id="export_orders",
    db_hook_id="snowflake_default",
    storage_hook_id="aws_default",
    bucket="my-data-lake",
    sql_template="SELECT * FROM orders WHERE date = '{{ ds }}'",
    remote_path_template="orders/{{ ds }}/data.parquet",
)
```

## Contents

- [Why this operator](#why-this-operator)
- [Install](#install)
- [Quick start](#quick-start)
- [Core concepts](#core-concepts)
  - [Sharding](#sharding)
  - [Concurrency model](#concurrency-model)
  - [Idempotency and manifest](#idempotency-and-manifest)
- [Production patterns](#production-patterns)
  - [Incremental exports (watermark)](#incremental-exports-watermark)
  - [Hive-style partitioning](#hive-style-partitioning)
  - [Native unload (server-side, 10–50× faster)](#native-unload-server-side-1050-faster)
  - [Row-level transforms](#row-level-transforms)
  - [Per-shard timeout](#per-shard-timeout)
- [Observability](#observability)
  - [OpenTelemetry tracing](#opentelemetry-tracing)
  - [XCom output](#xcom-output)
- [Extensibility](#extensibility)
  - [Plugins (third-party uploaders)](#plugins-third-party-uploaders)
- [Configuration reference](#configuration-reference)
- [Examples](#examples)
- [Development](#development)
- [License](#license)

## Why this operator

| | this | typical "fetch + boto3" DAG | one-off Python in a `PythonOperator` |
|---|---|---|---|
| Database-agnostic | ✅ Postgres / Snowflake / Databricks / Teradata / any DB-API | per-DB | per-DB |
| Streaming (no full materialisation) | ✅ Arrow batches with auto-tuned chunk size | ❌ usually `pandas.read_sql` | ❌ |
| Parallel shards | ✅ thread or process pool | ❌ | ❌ |
| Cross-shard cancellation on failure | ✅ | ❌ | ❌ |
| Idempotent re-runs | ✅ `skip_if_exists` | manual | manual |
| Manifest for downstream consumers | ✅ | manual | manual |
| Native warehouse unload (Snowflake / etc.) | ✅ as a strategy | manual | manual |
| Watermark / incremental | ✅ first-class | manual XCom plumbing | manual |
| OpenTelemetry traces | ✅ optional | ❌ | ❌ |
| Plugins for custom backends | ✅ entry points | fork | fork |

## Install

```bash
pip install airflow-export-to-object-store
```

With backend extras:

```bash
pip install "airflow-export-to-object-store[s3]"           # AWS S3
pip install "airflow-export-to-object-store[azure]"        # Azure Blob
pip install "airflow-export-to-object-store[gcs]"          # Google Cloud Storage
pip install "airflow-export-to-object-store[snowflake]"    # Snowflake driver + provider
pip install "airflow-export-to-object-store[postgres]"     # psycopg2
pip install "airflow-export-to-object-store[databricks]"   # Databricks SQL
pip install "airflow-export-to-object-store[teradata]"     # Teradata
pip install "airflow-export-to-object-store[memcheck]"     # psutil for memory health checks
pip install "airflow-export-to-object-store[otel]"         # OpenTelemetry tracing
```

Stack multiple: `pip install "airflow-export-to-object-store[s3,snowflake,otel]"`.

Requires Python ≥ 3.9 and Airflow ≥ 2.5.

## Quick start

Single-shard export from Snowflake to S3:

```python
from airflow import DAG
from airflow_export_to_object_store import StreamingExportOperator

with DAG("export_orders", schedule_interval="@daily", catchup=False) as dag:
    StreamingExportOperator(
        task_id="orders_to_s3",
        db_hook_id="snowflake_default",
        storage_hook_id="aws_default",
        bucket="my-data-lake",
        sql_template="SELECT * FROM orders WHERE date = '{{ ds }}'",
        remote_path_template="orders/{{ ds }}/data.parquet",
    )
```

That's enough to get a Parquet file under `s3://my-data-lake/orders/2026-05-08/data.parquet`.

## Core concepts

### Sharding

The operator splits one logical export into N parallel shards. Each
shard runs the same SQL template with shard-specific Jinja variables:

```python
StreamingExportOperator(
    task_id="export_events",
    db_hook_id="postgres_default",
    storage_hook_id="aws_default",
    bucket="my-data-lake",
    sql_template="""
        SELECT * FROM events
        WHERE event_date = '{{ ds }}'
          AND mod(id, {{ shards_total }}) = {{ shard_id }}
    """,
    shards=[{"shard_id": i, "shards_total": 8} for i in range(8)],
    remote_path_template="events/{{ ds }}/part_{{ '%03d' | format(shard_index) }}.parquet",
)
```

Default `shard_options.max_workers=6`. The implicit variable
`shard_index` (0…N-1) is always available.

### Concurrency model

Default `execution_mode="threads"`: shards run on a `ThreadPoolExecutor`
inside the Airflow task process. The hot path — DB I/O, Arrow batch
construction, dictionary decode, schema cast, zstd-compressed Parquet
writes, cloud uploads — all release the GIL because it lives in C/C++
extensions. Threads scale, share a single connection pool, and Airflow's
log capture works without ceremony.

When any shard fails the operator sets a shared cancellation `Event`;
running shards observe it on the next loop iteration, drop their
in-flight batch, and exit promptly. Failed task time is bounded by
one chunk's runtime, not the slowest survivor.

`execution_mode="processes"` switches to a `ProcessPoolExecutor` for
hard isolation between shards (memory-leaky drivers, per-shard OOM).
Trade-offs:

- **Memory**: each subprocess re-imports Airflow + PyArrow + driver
  (≈ 200–500 MiB resident).
- **No cross-shard cancellation**: `threading.Event` doesn't cross
  process boundaries; running shards finish naturally, only
  not-yet-started futures are cancelled.

### Idempotency and manifest

```python
StreamingExportOperator(
    task_id="export_orders",
    db_hook_id="snowflake_default",
    storage_hook_id="aws_default",
    bucket="my-data-lake",
    sql_template="SELECT * FROM orders WHERE date = '{{ ds }}'",
    remote_path_template="orders/{{ ds }}/data.parquet",
    skip_if_exists=True,    # safe to re-run / clear / retry
    write_manifest=True,    # writes orders/{{ ds }}/_manifest.json
    compute_md5=True,       # MD5 lands in the manifest
)
```

`skip_if_exists` probes the destination via `head_object` /
`exists()` **before** opening any DB cursor. If the file is already
there the shard returns immediately with `ShardResult.skipped=True`
and zero rows/bytes.

`write_manifest` writes a small JSON catalog at the common prefix of
all shard paths (or at `manifest_path` if you set it explicitly):

```json
{
  "version": 1,
  "exported_at": "2026-05-08T12:34:56+00:00",
  "total_rows": 1500000,
  "total_bytes": 1234567890,
  "files": [
    {"shard_index": 0, "remote_uri": "s3://...", "rows": 250000, "bytes": 205678123, "md5": "...", "skipped": false}
  ]
}
```

Downstream readers (Athena, Trino, Spark, schema registries) can act on
the manifest atomically without listing the bucket.

## Production patterns

### Incremental exports (watermark)

Most real exports are incremental — "rows that changed since the last
run". Plug in `IncrementalConfig` and the operator handles the state:

```python
from airflow_export_to_object_store import (
    IncrementalConfig,
    StreamingExportOperator,
)

StreamingExportOperator(
    task_id="export_orders_incremental",
    db_hook_id="postgres_default",
    storage_hook_id="aws_default",
    bucket="my-data-lake",
    sql_template="""
        SELECT *
        FROM orders
        WHERE updated_at >  '{{ watermark_prev }}'
          AND updated_at <= '{{ watermark_now }}'
    """,
    remote_path_template="orders/{{ ds }}/data.parquet",
    incremental=IncrementalConfig(
        watermark_query="SELECT MAX(updated_at) FROM orders",
        # OR: watermark_now_template="{{ ts }}",
        xcom_key="watermark",
        default_value="1970-01-01 00:00:00",
    ),
    write_manifest=True,
    skip_if_exists=True,
)
```

What happens at runtime:

1. The operator reads the previous run's watermark from XCom
   (`include_prior_dates=True`) — or falls back to `default_value` on
   the very first run.
2. It computes a fresh watermark either by running `watermark_query`
   against the source (recommended — captures one consistent moment
   even when the export takes hours) or by rendering
   `watermark_now_template` locally.
3. Both values are exposed as `{{ watermark_prev }}` /
   `{{ watermark_now }}` to the SQL, file-name and remote-path
   templates.
4. On success the new watermark is pushed back to XCom under
   `xcom_key`, ready to become `watermark_prev` next time. Failures
   leave XCom untouched, so a re-run replays the same window.

### Hive-style partitioning

Hive layout (`country=US/year=2026/data.parquet`) gives downstream
engines partition pruning. Express it with shards:

```python
StreamingExportOperator(
    sql_template="""
        SELECT * FROM events
        WHERE country = '{{ country }}'
          AND year    = {{ year }}
    """,
    shards=[
        {"country": c, "year": y}
        for c in ("US", "DE", "JP")
        for y in (2025, 2026)
    ],
    remote_path_template="events/country={{ country }}/year={{ year }}/data.parquet",
)
```

Each shard writes its own Parquet file at the right Hive prefix and
contributes to the manifest. No extra option needed.

### Native unload (server-side, 10–50× faster)

For terabyte-scale exports, streaming through this Airflow process is
the wrong shape — the warehouse can write Parquet directly to the
bucket, parallelised across its own compute, with zero per-row Python
overhead.

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
            storage_integration="MY_S3_INT",   # preferred — set up by Snowflake admin
            compression="ZSTD",
            max_file_size=256 * 1024 * 1024,   # ~256 MiB per file
        ),
    ),
)
```

The strategy runs `COPY INTO 's3://bucket/orders/{{ ds }}/' FROM (...)`
on Snowflake's warehouses, parses the result set so each produced file
becomes one `ShardResult`, and feeds the same manifest writer used by
the streaming path.

Auth: prefer `storage_integration` over inline `credentials={...}` —
the integration is set up once by a Snowflake admin and zero secrets
cross the SQL boundary.

Today the Snowflake strategy supports S3 and GCS targets; Azure
unload requires the storage account name (raises a clear error until
someone wires it).

### Row-level transforms

Mask PII, derive columns, coerce types — anything that operates on a
`pyarrow.Table`:

```python
import pyarrow as pa

def mask_email(tbl: pa.Table) -> pa.Table:
    idx = tbl.schema.get_field_index("email")
    masked = pa.array(["<redacted>"] * tbl.num_rows, type=pa.string())
    return tbl.set_column(idx, "email", masked)

StreamingExportOperator(
    ...,
    transform_fn=mask_email,
)
```

The function runs inside the fetch thread, on every batch, before the
Parquet writer sees it. Returning an empty `Table` for a batch is fine
(continue with the next chunk). Errors are wrapped in a `RuntimeError`
tagged with the shard index.

> **Process mode caveat**: when `execution_mode="processes"`,
> `transform_fn` must be a top-level callable — lambdas and closures
> can't be pickled.

### Per-shard timeout

```python
from airflow_export_to_object_store import ShardOptions

StreamingExportOperator(
    ...,
    shard_options=ShardOptions(
        max_workers=4,
        chunk_rows=50_000,
        timeout=30 * 60,   # 30-minute per-shard deadline
    ),
)
```

A daemon `threading.Timer` flips the local stop event on deadline, the
fetch / write loops exit promptly via the existing cancellation path,
the shard raises `TimeoutError`, and sibling shards see the operator's
cross-shard cancel. `timeout=None` (default) keeps the previous
"run forever if you must" behaviour.

## Observability

### OpenTelemetry tracing

Install the optional extra:

```bash
pip install "airflow-export-to-object-store[otel]"
```

Spans emitted (with attributes in parens):

| Span | Attributes |
|---|---|
| `export.execute` | `task_id`, `db_hook_id`, `storage_hook_id`, `mode`, `shards` |
| `export.shard` | `shard.index` |
| `export.shard.validate` | `shard.index` |
| `export.shard.upload` | `shard.index`, `shard.bytes`, `shard.rows` |
| `export.unload` | `unload.strategy` |

With Airflow 2.10+'s built-in OTel exporter (or your own SDK setup)
these light up automatically. Without `opentelemetry-api` installed,
the helpers are no-ops.

### XCom output

The operator pushes a dict to XCom on success:

```python
{
    "shards": [
        {
            "shard_index": 0,
            "remote_uri": "s3://...",
            "rows": 250_000,
            "bytes": 205_678_123,
            "md5": "abc...",
            "elapsed_s": 12.4,
            "skipped": False,
        },
        ...,
    ],
    "metrics": {"total_rows": ..., "total_bytes_mb": ..., "shards": [...]},
    "total_rows": 1_500_000,
    "total_bytes": 1_234_567_890,
    "elapsed_s": 87.5,
    # Only when applicable:
    "mode": "unload",
    "watermark": "2026-05-08 12:34:56",
}
```

Plus a colourful per-shard ASCII summary in the task log.

## Extensibility

### Plugins (third-party uploaders)

Need an in-house S3-compatible store, or a backend we don't ship?
Register an `Uploader` from your own package via entry points:

```toml
# your_pkg/pyproject.toml
[project.entry-points."airflow_export_to_object_store.uploaders"]
my_storage = "my_pkg.uploaders:MyUploader"
```

```python
# your_pkg/uploaders.py
from airflow_export_to_object_store.uploaders import Uploader  # for type-checking

class MyUploader:
    name = "my_storage"

    def matches(self, storage_hook): ...
    def network_targets(self): ...
    def health_check(self, storage_hook, *, container, bucket, log): ...
    def exists(self, storage_hook, *, container, bucket, remote_path): ...
    def upload(self, storage_hook, local_path, remote_path, *, container,
               bucket, overwrite, storage_hook_id, log): ...
```

Built-in backends still take priority; bad plugins are logged and
skipped — they cannot break the operator.

## Configuration reference

| Object | Key fields |
|---|---|
| `ParquetOptions` | `compression`, `row_group_size`, `coerce_timestamps`, `write_statistics`, `use_dictionary` |
| `RetryOptions` | `upload_retries`, `backoff_base`, `backoff_cap` |
| `ShardOptions` | `max_workers`, `chunk_rows`, `memory_limit_mb`, `timeout`, `execution_mode` |
| `IncrementalConfig` | `watermark_query` *or* `watermark_now_template`, `xcom_key`, `default_value` |
| `SnowflakeUnloadOptions` | `storage_integration` *or* `credentials`, `file_format`, `compression`, `max_file_size`, `single`, `overwrite`, `extra_options` |

`StreamingExportOperator` operator parameters:

| Parameter | Default | Purpose |
|---|---|---|
| `db_hook_id` / `storage_hook_id` | required | Airflow connections |
| `query` *or* `sql_template` | required (one of) | the SELECT |
| `sql_params` | `{}` | extra Jinja vars (flattened) |
| `shards` | `[{}]` (single) | per-shard Jinja contexts |
| `filename_template` | `data_{shard_index:03d}.parquet` | local file name |
| `remote_path_template` | `{ds}/data_{shard_index:03d}.parquet` | object key |
| `bucket` / `container` | None | S3/GCS bucket or Azure container |
| `parquet_options` | `ParquetOptions()` | zstd/512k row groups by default |
| `retry_options` | `RetryOptions()` | 3 retries, exponential backoff |
| `shard_options` | `ShardOptions()` | 6 workers, 50k chunk_rows |
| `tmp_dir` | system temp | local staging |
| `compute_md5` | `False` | per-file MD5 (skipped >10 GB) |
| `overwrite` | `True` | replace existing remote object |
| `validate_parquet` | `True` | footer + sample read sanity |
| `skip_if_exists` | `False` | idempotent re-runs |
| `write_manifest` / `manifest_path` | `False` / None | catalog JSON |
| `unload_strategy` | None | server-side bulk export |
| `unload_dir_template` | `"{{ ds }}/"` | unload destination prefix |
| `incremental` | None | watermark-based exports |
| `transform_fn` | None | per-batch Arrow transform |

## Examples

[`examples/`](examples/) contains runnable DAGs:

- `example_basic.py` — single-shard Snowflake → S3 (the "hello world").
- `example_sharded_postgres_to_gcs.py` — 8 parallel shards by `mod(id, N)`,
  manifest, MD5.
- `example_incremental_with_skip.py` — watermark + idempotent re-runs +
  manifest, the recommended production shape.
- `example_native_unload_snowflake.py` — Snowflake `COPY INTO` for
  terabyte-scale exports.
- `example_pii_transform.py` — row-level transform to mask emails before
  upload.
- `example_hive_partitioned.py` — Hive-style `country=US/year=2026/`
  layout via shard parameters.

## Development

```bash
git clone https://github.com/PotemkinAlexey/export-from-db-to-object-store.git
cd export-from-db-to-object-store
pip install -e ".[dev,s3,gcs,snowflake,otel]"
pre-commit install

ruff check src tests
ruff format --check src tests
pytest
```

CI runs lint + tests on Python 3.9, 3.10, 3.11, 3.12. Releases are
cut by tagging `vX.Y.Z`; the `publish.yml` workflow uses PyPI
trusted publishing — no API tokens.

See [CHANGELOG.md](CHANGELOG.md) for release notes.

## License

Apache License 2.0 — see [LICENSE](LICENSE).
