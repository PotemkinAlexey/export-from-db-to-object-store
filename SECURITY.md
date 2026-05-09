# Security policy

## Reporting a vulnerability

**Do not open a public GitHub issue for security problems.**

Email the maintainer at `potemkin_81@ukr.net` with:

- A description of the issue and the affected component(s)
- The minimum reproduction (DAG snippet, operator config, traceback)
- The version of `airflow-export-to-object-store`, Airflow, and Python
- Whether you'd like credit in the release notes if a fix ships

You'll get an acknowledgement within 72 hours. Confirmed vulnerabilities
ship as a patch release with a CVE entry where appropriate.

## Supported versions

Only the latest minor version receives security fixes. Pre-1.0 versions
are not supported.

| Version | Supported |
|---------|-----------|
| 1.x.y   | ✅        |
| 0.x.y   | ❌        |

## Scope

This package handles credentials only via Airflow Connections — it does
not store, log, or transmit secrets itself. Specific concerns we care
about:

- **SQL injection** in user-supplied templates. The operator renders
  Jinja with `StrictUndefined` and warns when rendered SQL doesn't
  start with `SELECT`/`WITH`, but it does NOT sanitise user-controlled
  parameters baked into the SQL string. Treat your `sql_params` and
  any `shards` values as trusted code.
- **Credentials in logs**. Rendered SQL is logged at `DEBUG` (not
  `INFO`) precisely because templates may carry secrets via
  `sql_params`. Don't lower the log level in production unless you
  trust the log sink.
- **Server-side encryption** is supported via `EncryptionOptions`
  (SSE-KMS for S3, encryption-scope for Azure, CMEK for GCS).
  Provider-managed encryption is always on regardless of these
  options.
- **Path traversal** in `remote_path_template` is blocked: rendered
  paths containing `..` or starting with `/` are rejected.

## Out of scope

- Vulnerabilities in upstream dependencies (Airflow, PyArrow, boto3,
  azure-storage-blob, google-cloud-storage). Report to those projects.
- Configuration mistakes that expose buckets / credentials. The
  package gives you the knobs; bucket policy is yours to set.
