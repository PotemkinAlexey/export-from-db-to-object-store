"""Jinja rendering for SQL queries and remote/local paths."""
from __future__ import annotations

import logging
from typing import Any, Dict, Mapping

from airflow import macros
from jinja2 import StrictUndefined, Template


def flatten_and_render_params(data: Mapping[str, Any], ctx: Mapping[str, Any]) -> Dict[str, Any]:
    """Flatten nested dict/list and render Jinja templates in string leaves.

    Top-level input must be a dict; scalars at the very top would collide
    on a single synthetic key, so we reject them explicitly. Duplicate
    flattened keys are also rejected to surface ambiguous input.
    """
    if not isinstance(data, Mapping):
        raise TypeError(f"flatten_and_render_params expects a Mapping, got {type(data).__name__}")

    flat: Dict[str, Any] = {}

    def _flatten(prefix: str, value: Any, depth: int = 0) -> None:
        if depth > 10:
            raise ValueError("Params nesting too deep — recursion loop?")

        if isinstance(value, Mapping):
            for k, v in value.items():
                key = f"{prefix}_{k}" if prefix else k
                _flatten(key, v, depth + 1)
        elif isinstance(value, list):
            for i, v in enumerate(value):
                key = f"{prefix}_{i}" if prefix else f"list_{i}"
                _flatten(key, v, depth + 1)
        else:
            if not prefix:
                raise ValueError("Unexpected scalar at top level during flattening")
            if prefix in flat:
                raise ValueError(f"Duplicate flattened key: {prefix!r}")
            flat[prefix] = value

    _flatten("", data)

    rendered: Dict[str, Any] = {}
    for k, v in flat.items():
        if isinstance(v, str) and "{{" in v and "}}" in v:
            rendered[k] = Template(v, undefined=StrictUndefined).render(**ctx)
        else:
            rendered[k] = v
    return rendered


def render_template(
    template_str: str,
    ctx: Mapping[str, Any],
    sql_params: Mapping[str, Any],
    log: logging.Logger,
    label: str = "template",
) -> str:
    """Render a Jinja template with Airflow macros + sql_params + flattened ctx.

    For `label="SQL"` the rendered text is logged at DEBUG (not INFO) because
    SQL bodies may carry secrets after parameter substitution. A SELECT/WITH
    prefix sanity check is logged as a warning when violated.
    """
    flat_params = flatten_and_render_params(
        {
            **(ctx.get("params", {}) if isinstance(ctx, Mapping) else {}),
            **sql_params,
            **ctx,
            "macros": macros,
        },
        ctx,
    )

    full_ctx = {**ctx, "macros": macros, **flat_params}
    rendered = Template(template_str, undefined=StrictUndefined).render(**full_ctx)

    if label == "SQL":
        stripped = rendered.lstrip().upper()
        if not stripped.startswith(("SELECT", "WITH")):
            log.warning("SQL does not start with SELECT/WITH (first 100 chars): %s", stripped[:100])

    log.debug("Rendered %s:\n%s", label, rendered)
    return rendered


def render_path_template(
    template_str: str,
    ctx: Mapping[str, Any],
    sql_params: Mapping[str, Any],
    log: logging.Logger,
) -> str:
    """Render a path template with traversal protection."""
    rendered = render_template(template_str, ctx, sql_params, log, label="string")

    if ".." in rendered or rendered.startswith("/"):
        raise ValueError(f"Invalid path: {rendered}")

    return rendered.lstrip("/")
