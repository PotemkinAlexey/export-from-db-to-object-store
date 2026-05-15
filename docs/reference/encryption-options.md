# EncryptionOptions

Configures server-side encryption for uploaded objects. Fields are cloud-specific; each backend reads only the fields relevant to its API and ignores the rest.

Pass an instance to `StreamingExportOperator(encryption=...)`.

## Fields by cloud

### Amazon S3

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `sse_algorithm` | `str \| None` | `None` | SSE algorithm. `"AES256"` for SSE-S3 (AWS-managed keys). `"aws:kms"` for SSE-KMS (customer-managed keys). |
| `kms_key_id` | `str \| None` | `None` | KMS key ARN or alias. Required when `sse_algorithm="aws:kms"`. Ignored when `sse_algorithm="AES256"`. |

**Ignored by S3:** `encryption_scope`, `kms_key_name`.

### Azure Blob Storage

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `encryption_scope` | `str \| None` | `None` | Azure encryption scope name configured on the storage account. When set, blobs are encrypted with the specified scope's key. |

**Ignored by Azure:** `kms_key_id`, `sse_algorithm`, `kms_key_name`.

### Google Cloud Storage

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `kms_key_name` | `str \| None` | `None` | Cloud KMS key resource name in the form `projects/.../locations/.../keyRings/.../cryptoKeys/...`. |

**Ignored by GCS:** `kms_key_id`, `sse_algorithm`, `encryption_scope`.

## Backend field matrix

| Field | S3 | Azure | GCS |
|-------|----|-------|-----|
| `kms_key_id` | Read | Ignored | Ignored |
| `sse_algorithm` | Read | Ignored | Ignored |
| `encryption_scope` | Ignored | Read | Ignored |
| `kms_key_name` | Ignored | Ignored | Read |

## Examples

### SSE-S3 (AWS-managed key)

```python
from airflow_export_to_object_store.encryption import EncryptionOptions

encryption = EncryptionOptions(
    sse_algorithm="AES256",
)
```

### SSE-KMS (customer-managed key)

```python
encryption = EncryptionOptions(
    sse_algorithm="aws:kms",
    kms_key_id="arn:aws:kms:us-east-1:123456789012:key/mrk-abc123",
)
```

### Azure encryption scope

```python
encryption = EncryptionOptions(
    encryption_scope="my-encryption-scope",
)
```

### GCS CMEK

```python
encryption = EncryptionOptions(
    kms_key_name="projects/my-project/locations/us-central1/keyRings/my-ring/cryptoKeys/my-key",
)
```

## Dataclass signature

```python
@dataclass(frozen=True)
class EncryptionOptions:
    kms_key_id: str | None = None
    sse_algorithm: str | None = None
    encryption_scope: str | None = None
    kms_key_name: str | None = None
```

`EncryptionOptions` is frozen; all fields must be set at construction time. Fields for clouds you are not using are safe to omit.
