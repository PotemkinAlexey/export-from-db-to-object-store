# Exceptions

All errors raised by `StreamingExportOperator` and its supporting classes are standard Python built-ins. There are no custom exception classes.

## ValueError

| Message | Raised by | Trigger | Fix |
|---------|-----------|---------|-----|
| `"Provide exactly one: query OR sql_template"` | `StreamingExportOperator.__init__` | Neither `query` nor `sql_template` was provided, or both were provided simultaneously. | Pass exactly one of `query=` or `sql_template=`. |
| `"Container must be specified for Azure storage"` | `StreamingExportOperator.__init__` or uploader | The storage hook is an Azure Blob Storage hook but `container` was not set. | Add `container="my-container"` to the operator. |
| `"unload_strategy ... does not match ..."` | `StreamingExportOperator.execute` | `unload_strategy.matches(db_hook, storage_hook)` returned `False` for the configured hooks. | Verify that the `db_hook_id` and `storage_hook_id` connection types match the strategy's supported pairs. See the strategy's reference doc for the supported hook matrix. |
| `"Set either storage_integration or credentials, not both"` | `SnowflakeUnloadStrategy.unload` | Both `storage_integration` and `credentials` were set in `SnowflakeUnloadOptions`. | Set only one: use `storage_integration` in production and `credentials` for ad-hoc testing. |
| `"One of storage_integration / credentials must be set for Snowflake unload"` | `SnowflakeUnloadStrategy.unload` | Neither `storage_integration` nor `credentials` was set in `SnowflakeUnloadOptions`. | Set `storage_integration` to the name of your Snowflake storage integration object, or set `credentials` for testing. |
| `"bucket must be set for BigQuery → GCS unload"` | `BigQueryUnloadStrategy.unload` | `bucket` was not provided on the operator when using the BigQuery unload strategy. | Add `bucket="my-gcs-bucket"` to the operator. |
| `"One of iam_role / credentials must be set for Redshift unload"` | `RedshiftUnloadStrategy.unload` | Neither `iam_role` nor `credentials` was set in `RedshiftUnloadOptions`. | Set `iam_role` to the ARN of the IAM role attached to your Redshift cluster, or set `credentials` for testing. |

## NotImplementedError

| Message | Raised by | Trigger | Fix |
|---------|-----------|---------|-----|
| `"Unsupported storage hook: <hook_type>"` | Uploader registry | No registered uploader's `matches()` returned `True` for the given storage hook. | Check that the correct install extra is installed (e.g. `[s3]`, `[azure]`, `[gcs]`). If using a custom backend, verify the entry point is registered and the `matches()` method covers the hook type. |
| `"<message about Azure>"` | `SnowflakeUnloadStrategy.unload` | A `WasbHook` (Azure Blob Storage) was used as the storage hook with `SnowflakeUnloadStrategy`. Snowflake's `COPY INTO` requires the storage account name which cannot be reliably derived from the hook. | Use the streaming path (no `unload_strategy`) for Snowflake → Azure exports. |

## RuntimeError

| Message | Raised by | Trigger | Fix |
|---------|-----------|---------|-----|
| `"DB hook <id> has no get_first/get_records"` | `StreamingExportOperator.execute` | The database hook does not implement `get_first` or `get_records`, which are required for the watermark query or chunked streaming. This occurs with very old or non-standard Airflow hook implementations. | Upgrade the Airflow provider package for the database, or implement `get_first` / `get_records` on a custom hook subclass. |
