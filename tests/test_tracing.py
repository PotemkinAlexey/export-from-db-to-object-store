"""Tracing helper has no observable effect when OTel is missing,
and emits a real span when it is installed."""
from __future__ import annotations

import pytest

from airflow_export_to_object_store import tracing


def test_span_is_no_op_without_otel():
    """The default test environment doesn't bring opentelemetry-api in,
    so the helper must produce a usable context manager that yields None."""
    if tracing.is_available():
        pytest.skip("OTel is installed; covered by test_span_with_otel")
    with tracing.span("test.noop", foo="bar") as s:
        assert s is None
        # set_attribute on None must be a no-op rather than raise.
        tracing.set_attribute(s, "answer", 42)


def test_span_drops_none_attributes(monkeypatch):
    """Attributes whose values are None must be filtered before reaching
    the OTel SDK (which rejects them)."""
    if not tracing.is_available():
        pytest.skip("OTel not installed in this environment")

    captured = {}

    class _Span:
        def set_attribute(self, k, v):
            captured[k] = v

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    class _Tracer:
        def start_as_current_span(self, name, attributes=None):
            captured["__name"] = name
            captured["__attrs"] = attributes
            return _Span()

    monkeypatch.setattr(tracing, "_TRACER", _Tracer())

    with tracing.span("test.attrs", a=1, b=None, c="x"):
        pass

    assert captured["__name"] == "test.attrs"
    assert captured["__attrs"] == {"a": 1, "c": "x"}
