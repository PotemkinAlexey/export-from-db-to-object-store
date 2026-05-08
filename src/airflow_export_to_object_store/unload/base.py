"""Native unload strategy protocol.

A native unload strategy bypasses the streaming fetch→Parquet→upload
pipeline and asks the database to write directly to the object store.
For warehouses that support this (Snowflake ``COPY INTO``, BigQuery
``EXPORT DATA``, Redshift ``UNLOAD``) it is typically **10–50× faster**
than client-side fetching for medium-to-large exports because:

* the data never crosses our network at all;
* the warehouse parallelises the write across its own compute nodes;
* per-row Python overhead disappears entirely.

The strategy receives the rendered SQL plus a small options object and
returns a list of :class:`~..options.ShardResult` describing what
actually landed in the bucket — so the operator's existing manifest
writer keeps working unchanged.
"""
from __future__ import annotations

import logging
from typing import Any, Protocol, runtime_checkable

from ..options import ShardResult


@runtime_checkable
class UnloadStrategy(Protocol):
    """Bulk-export strategy that runs server-side on the warehouse.

    Implementations are matched against ``(db_hook, storage_hook)`` pairs.
    The first strategy whose :meth:`matches` returns ``True`` runs
    :meth:`unload`; if no strategy matches, native unload is unavailable
    for this combination and the caller surfaces a clear error.
    """

    name: str

    def matches(self, db_hook: Any, storage_hook: Any) -> bool:
        """True iff this strategy can unload from ``db_hook`` to ``storage_hook``."""

    def unload(
        self,
        *,
        db_hook: Any,
        storage_hook: Any,
        sql: str,
        remote_dir: str,
        container: str | None,
        bucket: str | None,
        log: logging.Logger,
    ) -> list[ShardResult]:
        """Execute the warehouse-native bulk export and return one
        :class:`ShardResult` per file the warehouse produced."""
