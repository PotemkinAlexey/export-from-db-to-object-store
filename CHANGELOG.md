# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-05-08

### Added
- `py.typed` marker (PEP 561) so downstream type checkers see this package as typed.
- New module split exposing testable, single-responsibility units:
  - `templating.py` (Jinja for SQL and paths, including path-traversal guard).
  - `utils.py` (`compute_md5_eff`, `coerce_ts_table`).
  - `parquet_validator.py` (`validate_parquet_schema`).
  - `parquet_io.py` with `ShardWorker` encapsulating the fetch/write/heartbeat
    pipeline (was a 320-line method with three nested closures).
  - `uploaders/` package (Protocol-based registry; Azure Blob and AWS S3 backends).
- Test suite covering options, retries, metrics, templating, utils, parquet
  validation, uploader resolution, and an end-to-end `ShardWorker` run on
  sqlite.
- Public CHANGELOG.

### Changed
- `parquet_options`/`retry_options`/`shard_options` now default to `None`
  (the operator builds defaults internally). Functionally equivalent — no
  behavior change for existing callers.
- Rendered SQL is logged at DEBUG instead of INFO to avoid leaking secrets
  embedded via parameter substitution.
- Azure healthcheck blob name now carries a UUID suffix to avoid collisions
  between concurrent runs against the same container.
- Storage backend dispatch and network/health probes flow through an
  `Uploader` Protocol; adding a new backend is now a single class.
- The temporary-directory cleanup runs once at the start of `execute()`
  instead of every time the DAG file is parsed by the scheduler.

### Fixed
- `ExportMetrics.summary()` no longer crashes when called without any
  recorded shards (`max()` on an empty sequence).
- `flatten_and_render_params` rejects top-level scalars and surfaces
  duplicate flattened keys instead of silently overwriting them.
- Heartbeat thread no longer logs immediately on first iteration.
- Removed naive `; DROP` / `UNION SELECT` / `--` SQL "injection" check that
  produced false positives on legitimate SQL.
- Dropped a dead `futures = {}` placeholder.

### Internal
- `operator.py` shrank from 1,491 to ~350 lines (orchestration only).

## [0.1.0] - 2026-05-08

Initial release. Single-class operator imported from a monolithic file.
