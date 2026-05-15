# Result shape

Documents the `ShardResult` dataclass, the XCom dict returned by `StreamingExportOperator.execute()`, and the manifest JSON written when `write_manifest=True`.

## ShardResult

One `ShardResult` is produced per exported shard. The operator serialises each instance to a plain dict before pushing to XCom.

| Field | Type | Description |
|-------|------|-------------|
| `shard_index` | `int` | Zero-based shard index, matching the order of query execution or the `shards` list. |
| `remote_uri` | `str` | Full URI of the uploaded object, e.g. `"s3://bucket/exports/2026-05-08/data_000.parquet"`. |
| `rows` | `int` | Number of rows in this shard. `0` for unload strategies that do not report per-file row counts (BigQuery, Redshift). |
| `bytes` | `int` | Size in bytes of the uploaded Parquet file. `0` if not reported by the unload strategy. |
| `md5` | `str \| None` | Hex MD5 digest of the local Parquet file. `None` unless `compute_md5=True` on the operator, or when MD5 is not available (e.g. Snowflake unload). |
| `elapsed_s` | `float` | Wall-clock seconds for this shard (fetch + write + upload). For unload strategies, this covers the entire unload command duration divided across shards. |
| `skipped` | `bool` | `True` when `skip_if_exists=True` and the remote object already existed. When `skipped=True`, `rows` and `bytes` describe the local Parquet that would have been uploaded, not the existing remote object. |

### Dataclass signature

```python
@dataclass(frozen=True)
class ShardResult:
    shard_index: int
    remote_uri: str
    rows: int
    bytes: int
    md5: str | None
    elapsed_s: float
    skipped: bool = False
```

## XCom return value

The operator pushes a single dict to XCom under the default key (`return_value`). Pull it in a downstream task with `task_instance.xcom_pull(task_ids="export_task")`.

| Key | Type | Present | Description |
|-----|------|---------|-------------|
| `shards` | `list[dict]` | Always | One `ShardResult.__dict__` per shard, sorted by `shard_index`. |
| `metrics` | `dict` | Always | `ExportMetrics.summary()` — row throughput, byte throughput, and per-phase timings. |
| `total_rows` | `int` | Always | Sum of `rows` across all non-skipped shards. |
| `total_bytes` | `int` | Always | Sum of `bytes` across all non-skipped shards. |
| `elapsed_s` | `float` | Always | Wall-clock seconds for the entire `execute()` call. |
| `watermark` | `str \| None` | Always | New watermark if `incremental` is configured; `None` otherwise. |
| `mode` | `"stream" \| "unload"` | Unload path only | Indicates that `unload_strategy` was used. Absent when streaming. |

### Example XCom dict

```python
{
    "shards": [
        {
            "shard_index": 0,
            "remote_uri": "s3://bucket/exports/2026-05-08/data_000.parquet",
            "rows": 250000,
            "bytes": 205678123,
            "md5": "abc123def456...",
            "elapsed_s": 12.4,
            "skipped": False,
        },
        {
            "shard_index": 1,
            "remote_uri": "s3://bucket/exports/2026-05-08/data_001.parquet",
            "rows": 250000,
            "bytes": 201234567,
            "md5": "789abc012...",
            "elapsed_s": 11.8,
            "skipped": False,
        },
    ],
    "metrics": {
        "rows_per_second": 41666.7,
        "bytes_per_second": 34118765.0,
        "fetch_s": 8.2,
        "write_s": 6.1,
        "upload_s": 10.3,
    },
    "total_rows": 500000,
    "total_bytes": 406912690,
    "elapsed_s": 24.2,
    "watermark": None,
}
```

## Manifest JSON

Written to the object store when `write_manifest=True`. The manifest is a single JSON file listing all exported shards.

**Default path:** `<common_prefix_of_shard_remote_uris>/_manifest.json`

Override with `manifest_path=` on the operator.

### Schema

| Field | Type | Description |
|-------|------|-------------|
| `version` | `int` | Always `1`. |
| `exported_at` | `str` | ISO 8601 timestamp (UTC) when the manifest was written. |
| `total_rows` | `int` | Sum of rows across all shards. |
| `total_bytes` | `int` | Sum of bytes across all shards. |
| `files` | `array` | One object per shard. See file object fields below. |

### File object fields

| Field | Type | Description |
|-------|------|-------------|
| `shard_index` | `int` | Zero-based shard index. |
| `remote_uri` | `str` | Full URI of the uploaded object. |
| `rows` | `int` | Row count for this shard. |
| `bytes` | `int` | Byte size for this shard. |
| `md5` | `str \| null` | MD5 hex digest, or `null` if not computed. |
| `skipped` | `bool` | `true` if the shard was skipped due to `skip_if_exists`. |

### Example manifest

```json
{
  "version": 1,
  "exported_at": "2026-05-08T12:34:56+00:00",
  "total_rows": 1500000,
  "total_bytes": 1234567890,
  "files": [
    {
      "shard_index": 0,
      "remote_uri": "s3://bucket/exports/2026-05-08/data_000.parquet",
      "rows": 250000,
      "bytes": 205678123,
      "md5": "abc123...",
      "skipped": false
    },
    {
      "shard_index": 1,
      "remote_uri": "s3://bucket/exports/2026-05-08/data_001.parquet",
      "rows": 250000,
      "bytes": 201234567,
      "md5": "def456...",
      "skipped": false
    }
  ]
}
```
