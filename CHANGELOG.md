# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - 2026-05-08

### Added
- **BigQuery → GCS native unload** via ``BigQueryUnloadStrategy`` and
  ``BigQueryUnloadOptions``. Issues
  ``EXPORT DATA OPTIONS(uri='gs://.../*.parquet', format='PARQUET',
  compression='ZSTD', overwrite=true) AS SELECT ...`` against the
  user's BigQueryHook, then lists the destination GCS prefix to
  build ``ShardResult``s. Per-file row counts are not surfaced by
  ``EXPORT DATA``, so they're set to ``0``; manifest still reports
  per-file bytes.
- **Redshift → S3 native unload** via ``RedshiftUnloadStrategy`` and
  ``RedshiftUnloadOptions``. Builds an ``UNLOAD ('...') TO 's3://...'``
  statement (single-quote escaping handled), supports ``IAM_ROLE``
  *or* ``CREDENTIALS``, ``PARALLEL ON/OFF``, ``MAXFILESIZE``,
  ``CLEANPATH``, ``MANIFEST``, plus an ``extra_options`` escape hatch
  for raw clauses. Filters the auxiliary ``manifest`` file from the
  results so the export manifest doesn't list catalog metadata as a
  data shard.
- New ``redshift`` and ``bigquery`` extras in pyproject.

### Changed
- ``unload`` package re-exports both new strategies and their option
  dataclasses alongside the existing Snowflake ones.

## [1.0.0] - 2026-05-08

First stable release.

### Added
- `CONTRIBUTING.md` covering local setup, project shape, what we
  welcome / push back on, PR checklist, release process.
- Six runnable example DAGs under `examples/` covering the basic
  shape, sharded streaming, incremental + idempotent re-runs, native
  Snowflake unload, PII transforms, and Hive-style partitioning.

### Changed
- `README.md` reorganised: explicit table of contents, a "Why this
  operator" comparison up front, separate sections for core concepts /
  production patterns / observability / extensibility / configuration
  reference / examples / development. No new features; lots of
  redundancy removed.
- `StreamingExportOperator` class docstring rewritten to reflect the
  full v1 surface (modes, parameters, idempotency, manifest, unload,
  incremental, transform_fn).

Stable promise from this point: any change that breaks a public API
ships in a major version bump (2.0+) with a migration note. The
`Uploader` and `UnloadStrategy` Protocols are stable extension points;
new optional methods may be added but existing methods will not change
shape without a major bump.

## [0.9.0] - 2026-05-08

### Added
- **Per-shard timeout** — ``ShardOptions.timeout`` (seconds) is now
  honoured. A daemon ``threading.Timer`` started in ``ShardWorker.run``
  flips the local stop event after the deadline, the fetch / write
  threads exit promptly, and ``run`` raises ``TimeoutError``. The
  operator's cross-shard cancellation propagates to siblings just like
  any other failure. ``timeout=None`` keeps the previous behaviour
  (no watchdog).
- **Row-level transform hook** — pass ``transform_fn`` to apply a
  ``pyarrow.Table -> pyarrow.Table`` mapping to every batch before the
  Parquet writer sees it. Use cases: PII masking, derived columns,
  type coercion. Empty-table returns are fine (continue with next
  chunk); raised exceptions are wrapped with the shard index so they
  don't get lost. Must be a top-level callable when running with
  ``execution_mode="processes"``.

### Changed
- ``ShardWorker`` accepts an optional ``transform_fn``;
  ``ShardTaskParams`` carries it across the executor boundary.

## [0.8.0] - 2026-05-08

### Added
- **Watermark-based incremental exports** via the new
  ``IncrementalConfig``. The operator reads the previous watermark
  from XCom (with ``include_prior_dates=True``), computes a fresh one
  by either running a user-provided ``watermark_query`` against the
  source or rendering ``watermark_now_template`` locally, exposes both
  as ``{{ watermark_prev }}`` / ``{{ watermark_now }}`` in the SQL +
  path templates, and pushes the new value back to XCom on success
  under the configured ``xcom_key``.
- Operator output now includes ``watermark`` in its XCom payload (only
  when ``incremental`` is set).
- New ``incremental`` module with ``IncrementalConfig`` and
  ``coerce_watermark`` (handles ``datetime``, ``date``, ``Decimal``,
  ``None``).
- README documents both the incremental pattern and Hive-style
  partitioning via shards (no operator option needed — shard params
  feed both SQL and ``remote_path_template``).

### Changed
- ``StreamingExportOperator.__init__`` accepts ``incremental:
  IncrementalConfig | None``; when set, the operator wires watermarks
  through templating + XCom in both streaming and unload modes.

## [0.7.0] - 2026-05-08

### Added
- **Third-party uploader plugins** via the
  ``airflow_export_to_object_store.uploaders`` entry-point group. Drop
  an ``Uploader``-implementing class into your own package's
  ``pyproject.toml`` and the registry picks it up at runtime, after the
  built-ins. Bad entry points (import errors, wrong shape) are logged
  and skipped — never fatal.
- **OpenTelemetry tracing** (optional ``[otel]`` extra). The operator
  emits spans named ``export.execute``, ``export.run_shards``,
  ``export.shard``, ``export.shard.validate``, ``export.shard.upload``,
  ``export.unload`` with attributes for task / shard / unload. No-op
  overhead when ``opentelemetry-api`` is not installed.
- New ``tracing`` module: ``span(...)`` context manager,
  ``set_attribute(...)``, ``is_available()``.

### Changed
- ``get_registry()`` now composes built-in uploaders with discovered
  entry-point plugins.

## [0.6.0] - 2026-05-08

### Added
- **Native unload (warehouse → bucket)**. When the warehouse can write
  Parquet directly to object storage, streaming through this process is
  10–50× slower than asking it to do so server-side. The new
  ``unload_strategy=`` parameter delegates the export to the warehouse:
  * Snowflake: ``SnowflakeUnloadStrategy`` issues
    ``COPY INTO '<location>' FROM (SELECT ...)`` with the user's
    ``SnowflakeUnloadOptions`` (storage integration *or* inline
    credentials, file format, compression, ``MAX_FILE_SIZE``,
    ``SINGLE``, ``OVERWRITE``, plus an ``extra_options`` escape hatch).
  * S3 and GCS targets are supported today; Azure requires the storage
    account name (raised explicitly until someone wires it).
- The strategy parses ``COPY INTO``'s result rows into ``ShardResult``
  objects, so the existing manifest writer keeps working unchanged in
  unload mode.
- New ``unload_dir_template`` operator parameter (defaults to
  ``"{{ ds }}/"``) controls the destination prefix for unload mode.
- New ``unload`` package: ``UnloadStrategy`` Protocol,
  ``SnowflakeUnloadStrategy``, ``SnowflakeUnloadOptions``.

### Changed
- ``snowflake`` extra now also pulls in
  ``apache-airflow-providers-snowflake`` so the SnowflakeHook is
  available without juggling extras.
- CI installs the ``snowflake`` extra alongside ``s3`` and ``gcs``.

## [0.5.0] - 2026-05-08

### Added
- **Idempotent re-runs** via ``skip_if_exists=True``. The shard probes the
  destination through a new ``Uploader.exists()`` method (implemented for
  S3 / Azure / GCS) before touching the DB; matching objects make the
  shard return early with ``ShardResult.skipped=True`` and zero rows/bytes.
- ``ShardResult.skipped: bool`` field (default ``False``) so downstream
  XCom consumers can distinguish freshly-uploaded shards from re-used ones.
- **Manifest writer** (``write_manifest=True``). Emits ``_manifest.json``
  at the common prefix of all shard remote paths (or ``manifest_path`` if
  set explicitly). Schema is versioned and lists every file with
  shard_index, remote_uri, rows, bytes, md5 and skipped flag, plus
  totals — a small atomic catalog for Athena / Trino / Spark / registries.
- New ``manifest`` module with ``build_manifest``, ``resolve_manifest_path``,
  ``write_manifest_local`` (atomic via tempfile + os.replace).

### Changed
- ``ShardTaskParams`` carries a new ``skip_if_exists`` flag.
- ``execute_shard`` resolves the uploader once at entry so the existence
  probe and the upload share the same backend instance.

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
