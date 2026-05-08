"""Third-party uploader registration via importlib.metadata entry points."""
from __future__ import annotations

import logging

import pytest

from airflow_export_to_object_store.uploaders import base as base_mod


class _FakeHook:
    pass


class _CustomUploader:
    name = "custom"

    def matches(self, hook):
        return isinstance(hook, _FakeHook)

    def network_targets(self):
        return []

    def health_check(self, *a, **kw):
        return None

    def exists(self, *a, **kw):
        return False

    def upload(self, *a, **kw):
        return "custom://result"


class _NotAnUploader:
    """Doesn't satisfy the Uploader protocol — registry must skip with a warn."""


class _FakeEntryPoint:
    def __init__(self, name, target):
        self.name = name
        self._target = target

    def load(self):
        return self._target


def test_get_registry_includes_entry_points(monkeypatch):
    monkeypatch.setattr(
        base_mod,
        "_entry_point_uploaders",
        lambda: [_CustomUploader()],
    )
    registry = base_mod.get_registry()
    names = [u.name for u in registry]
    # Built-ins still come first, plugin appended.
    assert names[-1] == "custom"
    assert "azure" in names and "s3" in names and "gcs" in names


def test_resolver_picks_plugin_when_builtin_does_not_match(monkeypatch):
    monkeypatch.setattr(
        base_mod,
        "_entry_point_uploaders",
        lambda: [_CustomUploader()],
    )
    uploader = base_mod.resolve_uploader(_FakeHook())
    assert uploader.name == "custom"


def test_bad_entry_point_is_skipped_not_fatal(monkeypatch, caplog):
    """A plugin that fails to load or doesn't satisfy the protocol must NOT
    take down the operator — log and move on."""

    def _raise(_):
        raise ImportError("simulated bad plugin")

    bad = _FakeEntryPoint("broken", lambda: None)
    bad.load = _raise  # type: ignore[assignment]
    not_an_uploader = _FakeEntryPoint("wrong-shape", _NotAnUploader)

    real_eps = lambda: [bad, not_an_uploader]  # noqa: E731

    def _fake_entry_points(*, group=None):
        return real_eps() if group == "airflow_export_to_object_store.uploaders" else []

    import importlib.metadata as md

    monkeypatch.setattr(md, "entry_points", _fake_entry_points)

    with caplog.at_level(logging.WARNING):
        # Force re-evaluation of entry points.
        result = base_mod._entry_point_uploaders()

    assert result == []
    msgs = " ".join(r.getMessage() for r in caplog.records)
    assert "broken" in msgs
    assert "wrong-shape" in msgs
