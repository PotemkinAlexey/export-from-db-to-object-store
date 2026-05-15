# How-to: Enable server-side encryption

Use `EncryptionOptions` to control which encryption key protects each
uploaded object.

## Background

All three cloud providers encrypt data at rest by default using
provider-managed keys — you get this without configuring anything.
`EncryptionOptions` is for customer-managed keys (CMK / CMEK), where you
want to control the key lifecycle, rotation, and access policy
independently of the provider's defaults.

## AWS: SSE-S3 (AES-256 managed by S3)

This is a step up from the default (no explicit header) but still uses
S3-managed keys. Useful when a bucket policy requires it.

```python
from airflow_export_to_object_store import StreamingExportOperator
from airflow_export_to_object_store.encryption import EncryptionOptions

StreamingExportOperator(
    task_id="export_orders",
    db_hook_id="pg_default",
    storage_hook_id="aws_default",
    bucket="my-data-lake",
    sql_template="SELECT * FROM orders WHERE order_date = '{{ ds }}'",
    remote_path_template="orders/{{ ds }}/data.parquet",
    encryption=EncryptionOptions(
        sse_algorithm="AES256",
    ),
)
```

S3 sets the `x-amz-server-side-encryption: AES256` header on upload.

## AWS: SSE-KMS (customer-managed CMK)

```python
StreamingExportOperator(
    task_id="export_orders",
    db_hook_id="pg_default",
    storage_hook_id="aws_default",
    bucket="my-data-lake",
    sql_template="SELECT * FROM orders WHERE order_date = '{{ ds }}'",
    remote_path_template="orders/{{ ds }}/data.parquet",
    encryption=EncryptionOptions(
        sse_algorithm="aws:kms",
        kms_key_id="arn:aws:kms:us-east-1:123456789012:key/mrk-abc1234567890",
    ),
)
```

The IAM role used by the Airflow worker must have `kms:GenerateDataKey`
and `kms:Decrypt` on the key ARN. Use an MRK (multi-region key) if your
worker and bucket are in different regions.

S3 sets `x-amz-server-side-encryption: aws:kms` and
`x-amz-server-side-encryption-aws-kms-key-id: <ARN>` on upload.

## Azure: encryption scope

Azure Blob Storage does not support per-blob customer-provided keys (CPK)
via this operator. Use an encryption scope instead — a scope is a named
server-side encryption policy, configured at the storage account level, that
maps to a key in Azure Key Vault.

```python
StreamingExportOperator(
    task_id="export_orders",
    db_hook_id="pg_default",
    storage_hook_id="azure_default",
    container="my-container",
    sql_template="SELECT * FROM orders WHERE order_date = '{{ ds }}'",
    remote_path_template="orders/{{ ds }}/data.parquet",
    encryption=EncryptionOptions(
        encryption_scope="my-keyvault-scope",
    ),
)
```

Create the scope in the Azure portal (Storage account → Encryption →
Encryption scopes → Add) or with the CLI:

```bash
az storage account encryption-scope create \
  --account-name mystorageaccount \
  --name my-keyvault-scope \
  --key-source Microsoft.KeyVault \
  --key-uri "https://my-vault.vault.azure.net/keys/my-key/abc123"
```

The service principal or managed identity used by the Airflow connection
must have `Key Vault Crypto User` on the vault.

## GCS: CMEK (Cloud KMS)

```python
StreamingExportOperator(
    task_id="export_orders",
    db_hook_id="pg_default",
    storage_hook_id="gcs_default",
    bucket="my-data-lake",
    sql_template="SELECT * FROM orders WHERE order_date = '{{ ds }}'",
    remote_path_template="orders/{{ ds }}/data.parquet",
    encryption=EncryptionOptions(
        kms_key_name=(
            "projects/my-project/locations/us-central1"
            "/keyRings/my-ring/cryptoKeys/my-key"
        ),
    ),
)
```

The service account used by the GCS hook must have
`roles/cloudkms.cryptoKeyEncrypterDecrypter` on the key resource. The key
must be in the same location as the GCS bucket (or a global key).

## Using encryption with sharded exports

`EncryptionOptions` applies to every shard. Each uploaded file gets the
same encryption settings:

```python
from airflow_export_to_object_store import ShardOptions

StreamingExportOperator(
    task_id="export_events_sharded",
    db_hook_id="pg_default",
    storage_hook_id="aws_default",
    bucket="my-data-lake",
    shards=[{"region": "us"}, {"region": "eu"}, {"region": "ap"}],
    sql_template="SELECT * FROM events WHERE region = '{{ region }}'",
    remote_path_template="events/{{ ds }}/{{ region }}/data.parquet",
    shard_options=ShardOptions(max_workers=3),
    encryption=EncryptionOptions(
        sse_algorithm="aws:kms",
        kms_key_id="arn:aws:kms:us-east-1:123456789012:key/mrk-abc123",
    ),
)
```

## Fields reference

| Field | Provider | What it does |
|---|---|---|
| `sse_algorithm` | AWS | `"AES256"` for SSE-S3; `"aws:kms"` for SSE-KMS |
| `kms_key_id` | AWS | Key ARN or alias; required when `sse_algorithm="aws:kms"` |
| `encryption_scope` | Azure | Named encryption scope; per-blob CPK not supported |
| `kms_key_name` | GCS | Full KMS key resource name |

Fields for the wrong provider are silently ignored by the uploader.

## See also

- [Reference → EncryptionOptions](../reference/encryption-options.md).
- [How-to → Add object tags](../how-to/add-object-tags.md): complement
  encryption with metadata tags for cost allocation and lifecycle rules.
