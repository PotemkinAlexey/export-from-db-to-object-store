# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.0] - 2026-05-08

### Added
- Cross-shard cancellation. The operator owns a single `threading.Event`
  in thread mode; on the first failed shard it sets the event, cancels
  not-yet-started futures, and waits for in-flight shards to exit.
  Each `ShardWorker` checks `_should_stop()` in fetch/write/heartbeat
  loops and exits without uploading partial files.
- `ShardOptions.execution_mode = "threads" | "processes"`. The default
  remains `"threads"`. `"processes"` switches the operator to a
  `ProcessPoolExecutor` for hard isolation between shards (leaky
  drivers, unbounded memory growth). Trade-offs documented in README.
- New `shard_task.py` module: `ShardTaskParams` (frozen, picklable) and
  a top-level `execute_shard(params, cancel)` function that both
  executors target. The retry-decorated upload now lives here too.

### Changed
- Operator no longer holds the upload method directly; per-shard metrics
  are returned from `execute_shard` and merged into `ExportMetrics` after
  each shard completes (so process-mode shards report metrics back to
  the parent operator).
- Linter cleanup: `Callable` imported from `collections.abc` instead of
  `typing` (ruff UP035) and import order tightened in `parquet_io.py`.

## [0.3.0] - 2026-05-08

### Added
- Google Cloud Storage backend (`uploaders/gcs.py`, `gcs` extra). The
  `GCSUploader` plugs into the existing registry: any `GCSHook` instance
  is auto-detected and uploads via the hook's resumable-upload path.
  Health check tries write/delete, then falls back to a `get_bucket` read.
- Tests covering registry resolution, upload arguments, both health-check
  paths, and the bucket-required error.

### Changed
- CI now installs `[dev,s3,gcs]` so the GCS provider is available during tests.

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
