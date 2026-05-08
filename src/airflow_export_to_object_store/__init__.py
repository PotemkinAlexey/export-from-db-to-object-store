"""airflow-export-to-object-store: streaming SQL → Parquet → Azure/S3 Airflow operator."""
from __future__ import annotations

from .db_adapter import UniversalDbAdapter
from .metrics import ExportMetrics
from .operator import ExportFromDBToObjectStoreOperator
from .options import ParquetOptions, RetryOptions, ShardOptions, ShardResult
from .retry import with_retries

__version__ = "0.1.0"

__all__ = [
    "ExportFromDBToObjectStoreOperator",
    "UniversalDbAdapter",
    "ExportMetrics",
    "ParquetOptions",
    "RetryOptions",
    "ShardOptions",
    "ShardResult",
    "with_retries",
    "__version__",
]
