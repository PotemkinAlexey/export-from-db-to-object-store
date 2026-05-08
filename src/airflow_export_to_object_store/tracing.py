"""Optional OpenTelemetry tracing.

The operator emits spans for the export, each shard, and each major
phase inside a shard (fetch / write / validate / upload). When the
caller's Airflow already has an OTel exporter configured, these light
up automatically. Without the ``opentelemetry-api`` package installed
the helpers are no-ops with negligible overhead.

Exporting traces is the operator's user's responsibility — Airflow 2.10+
ships a built-in OTel integration; older versions need a small
:mod:`opentelemetry-sdk` setup in the DAG bootstrap.
"""

from __future__ import annotations

from contextlib import contextmanager, suppress
from typing import Any

try:
    from opentelemetry import trace as _otel_trace  # type: ignore[import-not-found]

    _TRACER = _otel_trace.get_tracer("airflow_export_to_object_store")
    _AVAILABLE = True
except Exception:  # pragma: no cover - optional dep
    _TRACER = None
    _AVAILABLE = False


@contextmanager
def span(name: str, /, **attributes: Any):
    """Open a span called ``name`` with the given attributes.

    Falls back to a no-op context manager when OTel is not installed.
    Attributes whose values are ``None`` are silently dropped so callers
    can pass optional fields directly (e.g. ``md5=result.md5``).
    """
    if not _AVAILABLE or _TRACER is None:
        yield None
        return

    cleaned = {k: v for k, v in attributes.items() if v is not None}
    with _TRACER.start_as_current_span(name, attributes=cleaned) as s:
        yield s


def set_attribute(span_obj: Any, key: str, value: Any) -> None:
    """Set an attribute on an open span if tracing is enabled."""
    if span_obj is None or value is None:
        return
    with suppress(Exception):  # defensive — span backends vary
        span_obj.set_attribute(key, value)


def is_available() -> bool:
    """True when the OpenTelemetry API is importable."""
    return _AVAILABLE
