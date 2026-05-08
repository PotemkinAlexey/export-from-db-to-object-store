"""Pure helpers used by the operator: MD5 computation and timestamp coercion."""
from __future__ import annotations

import hashlib
import os
from typing import Callable, Optional

import pyarrow as pa


def compute_md5_eff(
    file_path: str,
    *,
    log_fn: Optional[Callable[[str], None]] = None,
    skip_threshold_gb: int = 10,
) -> Optional[str]:
    """Compute MD5 of a file with a single 8 MB buffer.

    Returns the hex digest, or ``None`` if the file exceeds ``skip_threshold_gb``
    (very large files are skipped because hashing them serially can dominate
    runtime; cloud-side checksums are usually a better signal).
    """
    size = os.path.getsize(file_path)
    size_gb = size / (1024**3)

    if size_gb > skip_threshold_gb:
        if log_fn:
            log_fn(f"Skipping MD5 for very large file (size: {size_gb:.1f} GB)")
        return None

    h = hashlib.md5()
    buf = bytearray(8 * 1024 * 1024)
    mv = memoryview(buf)

    with open(file_path, "rb", buffering=0) as f:
        while True:
            n = f.readinto(buf)
            if not n:
                break
            h.update(mv[:n])

    return h.hexdigest()


def coerce_ts_table(tbl: pa.Table, target_unit: str) -> pa.Table:
    """Cast every timestamp column in ``tbl`` to ``target_unit``.

    Avoids ParquetWriter schema drift between batches when the source emits
    mixed timestamp resolutions. ``target_unit`` outside ``{s, ms, us, ns}``
    is treated as a no-op.
    """
    if target_unit not in ("s", "ms", "us", "ns"):
        return tbl

    new_fields = []
    for field in tbl.schema:
        if pa.types.is_timestamp(field.type):
            new_fields.append(pa.field(field.name, pa.timestamp(target_unit, field.type.tz)))
        else:
            new_fields.append(field)

    return tbl.cast(pa.schema(new_fields), safe=False)
