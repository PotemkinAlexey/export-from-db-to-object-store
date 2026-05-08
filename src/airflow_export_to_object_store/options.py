"""Frozen dataclasses describing operator configuration."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ParquetOptions:
    compression: str = "zstd"
    row_group_size: int = 512_000
    coerce_timestamps: Optional[str] = "ms"
    write_statistics: bool = False
    use_dictionary: bool = True


@dataclass(frozen=True)
class RetryOptions:
    upload_retries: int = 3
    backoff_base: float = 1.5
    backoff_cap: float = 20.0


@dataclass(frozen=True)
class ShardOptions:
    max_workers: int = 6
    chunk_rows: int = 50_000
    memory_limit_mb: int = 1024
    timeout: Optional[float] = None


@dataclass(frozen=True)
class ShardResult:
    shard_index: int
    remote_uri: str
    rows: int
    bytes: int
    md5: Optional[str]
    elapsed_s: float
