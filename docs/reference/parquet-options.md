# ParquetOptions

Controls Parquet encoding, compression, and schema behaviour when writing shard files locally.

Pass an instance to `StreamingExportOperator(parquet_options=...)`.

```python
from airflow_export_to_object_store.options import ParquetOptions

parquet_options = ParquetOptions(
    compression="snappy",
    row_group_size=256_000,
    coerce_timestamps="us",
    write_statistics=True,
)
```

## Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `compression` | `str` | `"zstd"` | Parquet compression codec. See valid values below. |
| `row_group_size` | `int` | `512_000` | Maximum number of rows per Parquet row group. Smaller values reduce memory but increase file metadata overhead. |
| `coerce_timestamps` | `str \| None` | `"ms"` | Cast all timestamp columns to this resolution before writing. See valid values below. Set to `None` to preserve the original resolution. |
| `write_statistics` | `bool` | `False` | Write column statistics (min/max/null count) into Parquet row group metadata. Useful for predicate pushdown in query engines; adds write overhead. |
| `use_dictionary` | `bool` | `True` | Enable dictionary encoding for low-cardinality columns. Reduces file size for string and categorical data. |

## Valid values — `compression`

| Value | Notes |
|-------|-------|
| `"zstd"` | Default. Best compression ratio; widely supported. |
| `"snappy"` | Faster decompression; larger files than zstd. Common in Spark environments. |
| `"gzip"` | High compression; slowest write. |
| `"lz4"` | Fastest codec; lowest compression ratio. |
| `"none"` | No compression. Use when the destination applies its own compression. |

## Valid values — `coerce_timestamps`

| Value | Resolution | Notes |
|-------|-----------|-------|
| `"ms"` | Milliseconds | Default. Compatible with most query engines. |
| `"us"` | Microseconds | Required for Pandas `datetime64[us]` round-trips. |
| `"s"` | Seconds | Drops sub-second precision. |
| `None` | Original | Preserves nanoseconds if the source produces them; may cause compatibility issues with engines that do not support nanosecond Parquet timestamps. |

## Dataclass signature

```python
@dataclass(frozen=True)
class ParquetOptions:
    compression: str = "zstd"
    row_group_size: int = 512_000
    coerce_timestamps: str | None = "ms"
    write_statistics: bool = False
    use_dictionary: bool = True
```

`ParquetOptions` is frozen; all fields must be set at construction time.
