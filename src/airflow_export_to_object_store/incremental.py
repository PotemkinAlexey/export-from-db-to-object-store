"""Incremental export: watermark-based change capture.

Most "export this table" jobs are really "export the rows that changed
since the last run". The :class:`IncrementalConfig` plugged into the
operator gives you exactly that without manual XCom plumbing:

1. **Before** the export the operator reads the previous run's
   watermark from XCom (with ``include_prior_dates=True``) — or falls
   back to the configured default on the very first run.
2. It then computes a fresh watermark for *this* run, either by:
   * running the user-provided ``watermark_query`` against the source
     (recommended — captures a single consistent snapshot regardless
     of how long the export takes), or
   * deferring to the rendered ``watermark_now_template`` (e.g.
     ``"{{ ts }}"``) when querying the source is undesirable.
3. Both watermarks are exposed to the SQL template as
   ``watermark_prev`` and ``watermark_now``, so the user writes the
   typical ``WHERE col > '{{ watermark_prev }}' AND col <= '{{ watermark_now }}'``.
4. On successful export the new watermark is pushed back to XCom under
   the configured key — ready to become ``watermark_prev`` next time.

This module owns only the dataclass and the pure helpers that need
testing in isolation; the operator does the actual XCom + DB calls.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class IncrementalConfig:
    """Configure watermark-based incremental exports.

    Exactly one of ``watermark_query`` / ``watermark_now_template`` must
    be set:

    * ``watermark_query`` — SQL that returns a single scalar (rendered
      with the same Jinja context as the main query). Run against the
      DB **before** the export, so the watermark snapshots a single
      consistent moment in time even when the export itself takes
      hours. Typical: ``"SELECT MAX(updated_at) FROM orders"``.
    * ``watermark_now_template`` — Jinja string evaluated locally; use
      when DB-side ``MAX()`` is too expensive or you'd rather pin to
      a logical timestamp. Typical: ``"{{ ts }}"`` or
      ``"{{ data_interval_end }}"``.
    """

    watermark_query: str | None = None
    watermark_now_template: str | None = None
    xcom_key: str = "watermark"
    default_value: str = "1970-01-01 00:00:00"

    def __post_init__(self) -> None:
        if bool(self.watermark_query) == bool(self.watermark_now_template):
            raise ValueError("IncrementalConfig requires exactly one of watermark_query or watermark_now_template")


def coerce_watermark(value: object) -> str:
    """Render a watermark value to the canonical string the SQL sees.

    DB drivers return rich types (``datetime``, ``date``, ``Decimal``)
    for ``MAX()`` queries; we keep them comparable downstream by
    serialising via :func:`str`. ``None`` becomes the literal ``""`` so
    the operator can distinguish "no row found" from a real value.
    """
    if value is None:
        return ""
    return str(value)
