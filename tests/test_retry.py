"""Tests for the with_retries decorator."""
from __future__ import annotations

import logging

import pytest

from airflow_export_to_object_store.options import RetryOptions
from airflow_export_to_object_store.retry import with_retries


class _Subject:
    def __init__(self, fail_times: int):
        self.fail_times = fail_times
        self.calls = 0
        self.retry_options = RetryOptions(upload_retries=3, backoff_base=1.0, backoff_cap=0.0)
        self.log = logging.getLogger("test")

    @with_retries
    def do(self):
        self.calls += 1
        if self.calls <= self.fail_times:
            raise RuntimeError(f"boom {self.calls}")
        return "ok"


def test_succeeds_first_try(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda _s: None)
    s = _Subject(fail_times=0)
    assert s.do() == "ok"
    assert s.calls == 1


def test_retries_then_succeeds(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda _s: None)
    s = _Subject(fail_times=2)
    assert s.do() == "ok"
    assert s.calls == 3


def test_raises_after_exhaustion(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda _s: None)
    s = _Subject(fail_times=10)
    with pytest.raises(RuntimeError):
        s.do()
    # initial + upload_retries
    assert s.calls == 1 + s.retry_options.upload_retries
