"""Frozen dataclasses describing operator configuration."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

ExecutionMode = Literal["threads", "processes"]


@dataclass(frozen=True)
class ParquetOptions:
    compression: str = "zstd"
    row_group_size: int = 512_000
    coerce_timestamps: str | None = "ms"
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
    timeout: float | None = None
    # ``threads`` (default): shards run in a ThreadPoolExecutor. The hot path
    # (Arrow / Parquet / cloud-IO) releases the GIL so threads scale well and
    # share a single Airflow process.
    # ``processes``: shards run in a ProcessPoolExecutor. Use when shards must
    # be hard-isolated (leaky DB drivers, per-shard memory pressure, etc.).
    # Costs: 200–500 MB resident per worker and round-trip pickling of inputs
    # and ShardResult.
    execution_mode: ExecutionMode = "threads"


@dataclass(frozen=True)
class ShardResult:
    shard_index: int
    remote_uri: str
    rows: int
    bytes: int
    md5: str | None
    elapsed_s: float
    # True when ``skip_if_exists`` matched a pre-existing remote object and the
    # shard was not re-uploaded. Rows/bytes/md5 then describe the **local**
    # parquet that we would have uploaded; remote_uri is the canonical URI of
    # the existing object.
    skipped: bool = False
