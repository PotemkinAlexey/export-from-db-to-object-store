"""Tests for flatten_and_render_params from the templating module."""

from __future__ import annotations

import pytest

from airflow_export_to_object_store.templating import flatten_and_render_params as flatten


def test_flat_dict_passthrough():
    out = flatten({"a": 1, "b": "x"}, {})
    assert out == {"a": 1, "b": "x"}


def test_nested_dict_uses_underscore_join():
    out = flatten({"db": {"host": "h", "port": 5432}}, {})
    assert out == {"db_host": "h", "db_port": 5432}


def test_list_uses_index_keys():
    out = flatten({"items": [10, 20]}, {})
    assert out == {"items_0": 10, "items_1": 20}


def test_jinja_values_rendered():
    out = flatten({"date": "{{ ds }}"}, {"ds": "2026-05-08"})
    assert out == {"date": "2026-05-08"}


def test_top_level_scalar_rejected():
    with pytest.raises(TypeError):
        flatten("not a dict", {})  # type: ignore[arg-type]


def test_too_deep_nesting_raises():
    nested = {"a": "leaf"}
    for _ in range(15):
        nested = {"x": nested}
    with pytest.raises(ValueError, match="nesting too deep"):
        flatten(nested, {})
