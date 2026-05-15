# Known Limits

No system is unlimited. This document describes the known limits of the streaming export operator: where it reaches the boundaries of its design, what behavior to expect when those boundaries are approached, and where workarounds exist. Some limits reflect deliberate tradeoffs; others are implementation gaps that may be addressed in future versions.

## Scalability Limits

**No built-in database connection pooling.** Each shard opens its own Airflow hook and database cursor independently. With `max_workers=16`, the operator opens 16 simultaneous database connections. These connections are drawn from Airflow's connection pool, which is shared across all tasks running on the same worker. In environments where database connections are expensive (Oracle, Snowflake, some MySQL configurations) or where the pool is small relative to the number of concurrent tasks, running many shards can exhaust the pool and cause connection-wait latency or timeouts.

The workaround is to set `max_workers` conservatively relative to the Airflow connection pool size, and to use Airflow's `max_active_tasks_per_dag` to limit concurrent runs of the export DAG. Connection pooling at the operator level is not implemented; the operator delegates connection management entirely to Airflow hooks.

**`memory_limit_mb` is a soft limit.** The `ShardOptions.memory_limit_mb` setting is checked before a shard begins and logged as a warning if the system's available memory is below the threshold. It is not enforced as a hard constraint during execution — the operator does not kill a shard that exceeds the limit mid-run. The rationale is that forcibly killing a shard mid-stream would leave temporary files on disk and potentially corrupt the Parquet file being written. A hard kill that the operator cannot clean up after is worse than an overcommitted memory state that completes and then fails on upload.

The practical implication is that `memory_limit_mb` should be treated as a planning tool, not a safety valve. It tells you whether your `chunk_rows` and shard configuration are likely to fit within the worker's memory budget, but it will not protect a worker from OOM if the configuration is badly overprovisioned.

## Native Unload Limits

**Azure native unload from Snowflake raises `NotImplementedError`.** Snowflake's `COPY INTO` command for Azure targets requires a storage account name that cannot be reliably derived from an Airflow `WasbHook` connection. The connection stores an account name only in some authentication modes; in SAS token mode it is absent. Rather than silently produce a malformed `COPY INTO` statement, the implementation raises `NotImplementedError` to make the gap explicit. The workaround is to use streaming mode for Snowflake-to-Azure exports, or to construct the storage account name from the connection URI in a custom `UnloadStrategy` subclass.

**BigQuery `EXPORT DATA` does not return per-file row counts.** The BigQuery native unload strategy issues a `EXPORT DATA OPTIONS(...)` statement, which writes files directly to GCS. BigQuery does not return a row count per exported file in the response. The `ShardResult` objects for BigQuery native unload therefore carry `rows=0`. This is not an error; it accurately represents what the API provides. Total row counts can be obtained from BigQuery's job statistics after the fact, but the operator does not fetch them. If per-shard row counts are required, use streaming mode.

**Redshift `UNLOAD` does not return per-file row counts.** The same limitation applies to Redshift's native unload. Redshift does write row counts to `STL_UNLOAD_LOG`, a system table that records the result of each `UNLOAD` statement. The operator does not query `STL_UNLOAD_LOG` after the unload completes. If per-shard row counts are required for Redshift exports, they can be obtained by querying `STL_UNLOAD_LOG` in a downstream task using the run's query ID, which is available in the XCom result.

**Snowflake `COPY INTO` does not return per-file MD5 checksums.** Snowflake's response to a `COPY INTO` statement includes file names and row counts but not MD5 hashes of the written files. The `ShardResult.md5` field is `None` for Snowflake native unload. If checksum verification is required, the MD5 must be fetched from the object store directly after the unload completes.

## Correctness Limits

**Non-monotonic `updated_at` causes missed rows.** The watermark mechanism assumes that the column used as the incremental predicate increases monotonically over time. If rows can be updated such that their `updated_at` decreases (for example, because of clock skew between database nodes, bulk backfills, or application bugs that set timestamps incorrectly), those rows may fall below the watermark and be missed permanently. The operator has no mechanism to detect or recover from this. The workaround is to use a full-table export (no watermark) for tables with non-monotonic update timestamps, or to add a secondary change-tracking mechanism at the application level.

**No schema evolution detection between runs.** If a table's schema changes between runs — columns are added, removed, or their types change — the operator will happily write Parquet files with different schemas in different runs without warning. The files are individually valid Parquet; a reader that unions all files from multiple runs may encounter schema incompatibility errors at query time. The operator does not compare the inferred schema of the current run against the schema from prior runs. Schema evolution is the responsibility of the consumer. Downstream systems that are sensitive to schema changes should validate schemas against a registered schema before processing new files.

**No built-in deduplication.** If the same row appears in multiple export windows (because `updated_at` is not strictly monotonic, or because the incremental bounds overlap), the row will appear in multiple Parquet files. The operator does not deduplicate across files or runs. Deduplication is the responsibility of the consumer, typically via a `ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC)` pattern at query time.

## Operational Limits

**The manifest is rewritten on every run, not appended.** Each successful run writes a new manifest file at the configured path, overwriting any previous manifest at that path. The manifest therefore represents only the current run's output. There is no built-in cumulative manifest that tracks all files ever written by the operator. If a cumulative catalog is required, a downstream task should read the per-run manifest and upsert its entries into a persistent catalog table.

**No sub-operation timeouts.** `ShardOptions.timeout` wraps the entire shard operation — fetch, write, and upload combined. If the timeout fires, the shard raises a `TimeoutError` and the file is abandoned. Within the shard, there are no separate timeouts for the DB fetch, the Parquet write, or the upload. A shard that hangs on a slow database query will consume its entire timeout budget before the upload even begins. If fine-grained timeout control is needed, it must be implemented at the database level (statement timeout, query timeout parameters on the cursor) and at the cloud SDK level (request timeout parameters on the uploader).

**`transform_fn` must be a top-level callable in process mode.** This is a consequence of Python's pickle protocol and is discussed in detail in the concurrency model documentation. In practice, it means that transform functions cannot be lambdas or closures when using process-based concurrency. This is not a workaround-able limit within the current architecture — it is a fundamental property of cross-process function serialization in Python.
