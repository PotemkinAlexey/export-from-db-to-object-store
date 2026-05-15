# How-to: Tune performance on large exports

The main levers are `chunk_rows`, `max_workers`, `execution_mode`,
compression, and `row_group_size`. This guide explains what each knob
does and when to move it.

## Quick reference

| Knob | Default | Move up when | Move down when |
|---|---|---|---|
| `chunk_rows` | 50,000 | too many DB round-trips | OOM on worker |
| `max_workers` | 6 | shards queue behind each other | DB connection pool exhausted |
| `compression` | `zstd` | — (default is good) | downstream reads dominate |
| `row_group_size` | 512,000 | analytics scans are slow | random-access reads are slow |
| `tmp_dir` | system `/tmp` | `/tmp` is small or slow | — |

## chunk_rows

`chunk_rows` is the number of rows the operator fetches from the database
cursor in one batch and converts to a single Arrow record batch. The
default is 50,000.

**Larger values** (100,000–200,000):

- Fewer round-trips to the database — Arrow conversion overhead is
  amortised over more rows.
- Higher peak memory per shard: `chunk_rows` rows × (avg row width in bytes).
- Better Arrow compression at write time because more data is visible
  per batch.

**Smaller values** (10,000–25,000):

- Lower peak memory.
- Useful when rows are wide (many columns, long text fields).

Start at 100,000 for narrow rows (< 20 columns, no text blobs). Drop to
25,000 if the worker's memory warning fires regularly.

```python
from airflow_export_to_object_store import ShardOptions

shard_options=ShardOptions(
    chunk_rows=100_000,
    memory_limit_mb=2048,   # raise the soft-limit warning threshold too
)
```

## max_workers

`max_workers` is the number of shards running concurrently. The default
is 6.

The practical ceiling is the database connection pool, not the number of
CPU cores. Each shard holds one DB connection for its entire lifetime.
A pool of 10 connections caps useful concurrency at ~8–9 (leave at least
one or two free for monitoring and other DAG tasks).

**Cloud databases with auto-scaling** (Aurora Serverless, AlloyDB,
Snowflake virtual warehouses): `max_workers` up to 12–16 is often fine.

**On-premises Postgres / MySQL** with a fixed pool: stay at 4–8.

```python
shard_options=ShardOptions(
    max_workers=8,
)
```

## execution_mode

The default `"threads"` mode runs each shard in a Python thread. Threads
share memory (Arrow buffers are visible to all shards) and start in
microseconds.

Switch to `"processes"` when:

- Your DB driver leaks memory across connections (some ODBC drivers do).
- You are writing very large shards and need the OS to reclaim memory
  between shards.
- You hit the GIL for CPU-heavy `transform_fn` work (rare with Arrow,
  which releases the GIL for most operations).

```python
shard_options=ShardOptions(
    execution_mode="processes",
    max_workers=4,
)
```

Subprocess startup adds ~0.5–1 s per shard. For 3–4 shards this is
negligible; for 50+ shards it adds up. Also: `transform_fn` must be a
module-level callable in process mode (not a lambda).

## Compression

`ParquetOptions.compression` controls the codec used inside the Parquet
file. The default is `zstd`.

| Codec | Ratio | Compression speed | Decompression speed |
|---|---|---|---|
| `zstd` | best | medium | medium |
| `snappy` | good | fast | fastest |
| `lz4` | good | fastest | fastest |
| `gzip` | better than snappy | slow | slow |
| `none` | none | — | — |

Use `zstd` (default) when the destination is a data lake and reads are
occasional. Use `snappy` when the data is read frequently by Athena or
Trino and decompression throughput matters. Use `none` only when the data
is already incompressible (pre-compressed blobs, encrypted bytes).

```python
from airflow_export_to_object_store import ParquetOptions

parquet_options=ParquetOptions(
    compression="snappy",
)
```

## row_group_size

`ParquetOptions.row_group_size` is the number of rows per Parquet row
group. The default is 512,000.

**Larger row groups** (1,000,000+):

- Better sequential scan throughput (Athena, Trino, Spark full-table
  scans).
- Worse random-access latency (reading a single row requires
  decompressing the whole group).
- Larger RAM requirement for readers.

**Smaller row groups** (100,000–256,000):

- Faster random access.
- More metadata per file, which increases footer size.

For analytics workloads (GROUP BY, aggregates), use 1,000,000:

```python
parquet_options=ParquetOptions(
    row_group_size=1_000_000,
    compression="zstd",
    write_statistics=True,   # enables predicate pushdown in Athena / Trino
)
```

## tmp_dir

By default the operator writes local Parquet files to the system temp
directory (`/tmp` on Linux). If `/tmp` is small (a common default on
cloud VMs) or is backed by a slow disk:

```python
StreamingExportOperator(
    ...
    tmp_dir="/mnt/fast-ssd/airflow-tmp",
)
```

Point to a fast SSD with enough headroom for all concurrent shards:
`chunk_rows × row_width × max_workers` bytes plus the full Parquet file
before upload.

## Reading the metrics block in logs

After each run the operator logs an ASCII metrics summary at INFO level:

```
[export] shard=0 rows=2_431_800 bytes=187.3 MB fetch=12.4s write=8.1s
         upload=3.2s total=23.7s throughput=102.3 MB/s grade=A
[export] shard=1 rows=2_389_100 bytes=183.9 MB fetch=11.9s write=7.8s
         upload=3.1s total=22.8s throughput=106.5 MB/s grade=A+
```

**Grade meanings**:

| Grade | Throughput |
|---|---|
| A+ | > 100 MB/s per shard |
| A | 50–100 MB/s |
| B | 20–50 MB/s |
| C | < 20 MB/s |

A `C` grade on fetch suggests the database is the bottleneck (slow query,
missing index, row-level security overhead). A `C` grade on upload
suggests network saturation or a distant bucket region.

## When to switch to native unload

If you cannot achieve A-grade throughput with streaming and the source
is Snowflake, BigQuery, or Redshift — switch to the native unload
strategy. See [Tutorial 03 — Native unload](../tutorials/03-native-unload.md).

Rule of thumb:

- < 10 GB: streaming, no tuning needed.
- 10 GB – 100 GB: streaming with shards, tune `chunk_rows` and
  `max_workers`.
- > 100 GB: use native unload.

## See also

- [Reference → ShardOptions](../reference/shard-options.md).
- [Reference → ParquetOptions](../reference/parquet-options.md).
- [How-to → Shard large tables](shard-large-tables.md): splitting one
  query into N parallel shards.
- [Tutorial 03 — Native unload](../tutorials/03-native-unload.md): for
  warehouse-scale exports.
