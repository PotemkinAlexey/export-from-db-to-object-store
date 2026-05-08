# Contributing

Thank you for thinking about contributing! This is a small project and
the bar for accepting changes is "make the operator do its job better
for someone, without making it worse for everyone else." A few notes
that should make the loop smooth.

## Local setup

```bash
git clone https://github.com/PotemkinAlexey/export-from-db-to-object-store.git
cd export-from-db-to-object-store
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev,s3,gcs,snowflake,otel]"
pre-commit install
```

Run the full test suite:

```bash
ruff check src tests
ruff format --check src tests
pytest
```

CI runs the same three commands across Python 3.9–3.12. If they pass
locally, the PR will pass CI.

## Project shape

The operator is intentionally split into small, single-responsibility
modules so each piece can be unit-tested without spinning up Airflow
or a real cloud:

| Module | Responsibility |
|---|---|
| `operator.py` | Orchestration, Airflow integration |
| `parquet_io.py` | `ShardWorker` — fetch / write / heartbeat threads |
| `shard_task.py` | Module-level entry point shared by both pool types |
| `db_adapter.py` | Universal DB-API connection wrapper |
| `templating.py` | Jinja for SQL + paths, with safety guards |
| `parquet_validator.py` | Pre-upload Parquet sanity checks |
| `uploaders/` | Backend Protocol + Azure / S3 / GCS implementations |
| `unload/` | Native server-side unload strategies (Snowflake, …) |
| `incremental.py` | Watermark dataclass + helpers |
| `manifest.py` | Manifest builder + atomic local writer |
| `metrics.py`, `tracing.py`, `retry.py`, `utils.py` | Cross-cutting |

## What we welcome

- **New uploader backends** (e.g. MinIO-specific tuning, Backblaze B2,
  Wasabi). Implement the `Uploader` Protocol; ideally ship as a
  separate package and register via the
  `airflow_export_to_object_store.uploaders` entry point group.
  Built-in PRs are also fine for major clouds.
- **New unload strategies** (BigQuery `EXPORT DATA`, Redshift `UNLOAD`).
  Implement the `UnloadStrategy` Protocol; same pattern as Snowflake.
- **Driver compatibility fixes** for the streaming path
  (`UniversalDbAdapter` and the fetch loop).
- **Bug fixes with a regression test.**
- **Docs improvements** — examples, edge cases, gotchas you hit in
  production.

## What we'd push back on

- Switching `pyarrow.parquet` for a different format (ORC / Avro / CSV)
  by default — Parquet is the standard.
- Adding `asyncio` — the threading model already releases the GIL on
  the hot path; async would add complexity without a measurable win.
- Adding a config-file layer (YAML / JSON) — Airflow operators belong
  in Python.
- Replacing `ThreadPoolExecutor` with a custom scheduler. The current
  one is fine and has cross-shard cancellation.

## Pull request checklist

- [ ] One coherent change per PR (split big ones).
- [ ] Tests for the new behaviour (positive case + at least one
      negative case).
- [ ] `ruff check` and `ruff format --check` pass.
- [ ] CHANGELOG entry under the `Unreleased` heading (the maintainer
      bumps the version on release).
- [ ] README updated if the change affects user-visible behaviour.

## Releases

Releases are cut by tagging `vX.Y.Z` on `main`. The
[publish.yml](.github/workflows/publish.yml) workflow uses PyPI's
trusted-publishing (OIDC) — there are no API tokens.

SemVer pre-1.0: minor versions could carry breaking changes. Once
1.0.0 ships, breakages go in major bumps and get a migration note in
the CHANGELOG.

## License

By contributing you agree that your contributions are licensed under
the [Apache License 2.0](LICENSE), the same license as the project.
