# Documentation

Welcome. The docs follow the [Diátaxis](https://diataxis.fr/) framework — four
quadrants matched to what you're trying to do right now.

## Tutorials

Hand-held, beginning-to-end. Read these first.

- [01 — Quickstart](tutorials/01-quickstart.md) — your first export, in
  five minutes.
- [02 — Incremental exports with watermarks](tutorials/02-incremental.md) —
  build a production-shape DAG that's safe to clear and retry.
- [03 — Native unload from Snowflake](tutorials/03-native-unload.md) —
  switch to server-side `COPY INTO` for terabyte-scale jobs.

## How-to

Recipes for specific tasks. Skim, copy, adapt.

- [Shard large tables](how-to/shard-large-tables.md)
- [Mask PII before write](how-to/mask-pii.md)
- [Enable customer-managed encryption (KMS / CMK / CMEK)](how-to/enable-encryption.md)
- [Partition output Hive-style](how-to/partition-hive-style.md)
- [Handle failures gracefully (cancellation, timeouts, retries)](how-to/handle-failures.md)
- [Write a custom uploader plugin](how-to/write-uploader-plugin.md)
- [Instrument with OpenTelemetry](how-to/instrument-tracing.md)
- [Tune performance for very large exports](how-to/tune-performance.md)

## Reference

Every public knob, exhaustively listed.

- [`StreamingExportOperator` parameters](reference/operator.md)
- [`ParquetOptions`](reference/parquet-options.md)
- [`ShardOptions`](reference/shard-options.md)
- [`RetryOptions`](reference/retry-options.md)
- [`EncryptionOptions`](reference/encryption-options.md)
- [`IncrementalConfig`](reference/incremental-config.md)
- [`SnowflakeUnloadStrategy` / `SnowflakeUnloadOptions`](reference/snowflake-unload.md)
- [`BigQueryUnloadStrategy` / `BigQueryUnloadOptions`](reference/bigquery-unload.md)
- [`RedshiftUnloadStrategy` / `RedshiftUnloadOptions`](reference/redshift-unload.md)
- [Uploader Protocol](reference/uploader-protocol.md)
- [UnloadStrategy Protocol](reference/unload-strategy-protocol.md)
- [Result / XCom shape](reference/result-shape.md)
- [Exceptions](reference/exceptions.md)

## Explanation

The "why" behind the design choices.

- [Architecture overview](explanation/architecture.md)
- [Concurrency model (GIL, threads vs. processes)](explanation/concurrency-model.md)
- [Streaming pipeline internals](explanation/streaming-pipeline.md)
- [Idempotency, watermarks, and the manifest](explanation/idempotency-and-state.md)
- [Design decisions](explanation/design-decisions.md)
- [Known limits](explanation/limits.md)

---

## Other docs at the repo root

- [README](../README.md) — short overview + install
- [CHANGELOG](../CHANGELOG.md)
- [CONTRIBUTING](../CONTRIBUTING.md)
- [SECURITY](../SECURITY.md)
