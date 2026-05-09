"""Shared end-to-end fixtures: a sqlite source DB, a filesystem-backed
storage backend, and stubs for ``BaseHook.get_hook`` /
``resolve_uploader`` / ``task_instance``.

Imported via ``conftest.py``; the fixtures are plain functions so test
files import them explicitly.
"""

from __future__ import annotations

import shutil
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from airflow.hooks.base import BaseHook

from airflow_export_to_object_store import operator as operator_mod
from airflow_export_to_object_store import shard_task as shard_task_mod


# ----------------------------------------------------------------------
# Fakes
# ----------------------------------------------------------------------
class FakeDbHook:
    """A DB-API hook backed by a file sqlite database.

    ``get_conn()`` opens a *new* connection every time, mirroring how
    real Airflow hooks behave: each shard's :class:`UniversalDbAdapter`
    closes its own connection at the end of run() without affecting
    sibling shards. Backing the DB by a file (instead of ``:memory:``)
    lets all those connections see the same data.
    """

    def __init__(self, db_path: Path):
        self.db_path = db_path

    def get_conn(self) -> sqlite3.Connection:
        return sqlite3.connect(str(self.db_path), check_same_thread=False)

    def get_first(self, sql: str):
        conn = self.get_conn()
        try:
            return conn.execute(sql).fetchone()
        finally:
            conn.close()

    def get_records(self, sql: str):
        conn = self.get_conn()
        try:
            return conn.execute(sql).fetchall()
        finally:
            conn.close()


class FakeStorageHook:
    """Marker class — the fake uploader recognises it via ``isinstance``
    semantics that we bypass with monkeypatched ``resolve_uploader``."""


@dataclass
class _UploadCall:
    remote_path: str
    size: int
    encryption: Any | None
    tags: dict[str, str] | None


@dataclass
class FakeUploader:
    """Filesystem-backed Uploader. Writes uploads under ``root`` so
    tests can assert byte-equality against the produced Parquet."""

    root: Path
    bucket_or_container: str = "test-bucket"
    fail_remote_paths: set[str] = field(default_factory=set)
    uploads: list[_UploadCall] = field(default_factory=list)
    exists_paths: set[str] = field(default_factory=set)
    name: str = "fake"

    def matches(self, hook: Any) -> bool:
        return isinstance(hook, FakeStorageHook)

    def network_targets(self):
        return []

    def health_check(self, hook: Any, *, container, bucket, log) -> None:
        return None

    def exists(self, hook: Any, *, container, bucket, remote_path: str) -> bool:
        return remote_path in self.exists_paths or (self.root / remote_path).exists()

    def upload(
        self,
        hook: Any,
        local_path: str,
        remote_path: str,
        *,
        container,
        bucket,
        overwrite: bool,
        storage_hook_id: str,
        log,
        encryption=None,
        tags=None,
    ) -> str:
        if remote_path in self.fail_remote_paths:
            raise RuntimeError(f"forced upload failure on {remote_path}")
        target = self.root / remote_path
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(local_path, target)
        self.uploads.append(
            _UploadCall(
                remote_path=remote_path,
                size=target.stat().st_size,
                encryption=encryption,
                tags=dict(tags) if tags else None,
            )
        )
        return f"fake://{self.bucket_or_container}/{remote_path}"


class FakeTI:
    """Minimal ``task_instance`` stub: pull/push operate on a dict."""

    def __init__(self, prior: dict[tuple[str, str], Any] | None = None):
        self._prior = prior or {}
        self.pushed: dict[str, Any] = {}

    def xcom_pull(self, task_ids: str, key: str, include_prior_dates: bool = False):
        return self._prior.get((task_ids, key))

    def xcom_push(self, key: str, value: Any) -> None:
        self.pushed[key] = value


# ----------------------------------------------------------------------
# Wiring
# ----------------------------------------------------------------------
def install_fakes(
    monkeypatch,
    *,
    rows: list[tuple],
    schema: str = "CREATE TABLE orders (id INTEGER, amount REAL, updated_at TEXT)",
    insert_sql: str = "INSERT INTO orders VALUES (?, ?, ?)",
    storage_root: Path,
    db_conn_id: str = "test_db",
    storage_conn_id: str = "test_storage",
) -> tuple[FakeDbHook, FakeStorageHook, FakeUploader]:
    """Build the sqlite-backed DB, the filesystem-backed storage, and the
    fake uploader, then patch :class:`BaseHook` and ``resolve_uploader``
    so the operator under test sees them.
    """
    db_path = storage_root.parent / "_source.sqlite"
    seed = sqlite3.connect(str(db_path))
    try:
        seed.execute(schema)
        if rows:
            seed.executemany(insert_sql, rows)
        seed.commit()
    finally:
        seed.close()

    db_hook = FakeDbHook(db_path)
    storage_hook = FakeStorageHook()
    uploader = FakeUploader(root=storage_root)

    def _fake_get_hook(conn_id, *args, **kwargs):
        if conn_id == db_conn_id:
            return db_hook
        if conn_id == storage_conn_id:
            return storage_hook
        raise KeyError(f"Unknown conn_id in fake: {conn_id}")

    # ``BaseHook.get_hook`` is a classmethod; replace it with a staticmethod
    # that ignores the cls argument.
    monkeypatch.setattr(BaseHook, "get_hook", staticmethod(_fake_get_hook))

    def _fake_resolve(_hook, registry=None):
        return uploader

    monkeypatch.setattr(operator_mod, "resolve_uploader", _fake_resolve)
    monkeypatch.setattr(shard_task_mod, "resolve_uploader", _fake_resolve)

    return db_hook, storage_hook, uploader


def make_context(
    *,
    ti: FakeTI | None = None,
    ds: str = "2026-05-08",
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Bare Airflow render-context with ``ti``, ``task_instance``, ``ds``,
    plus a default empty ``params`` dict (the operator's templating reads it)."""
    ti = ti or FakeTI()
    ctx: dict[str, Any] = {
        "ti": ti,
        "task_instance": ti,
        "ds": ds,
        "ts": f"{ds}T00:00:00+00:00",
        "ts_nodash": f"{ds.replace('-', '')}T000000",
        "params": {},
    }
    if extra:
        ctx.update(extra)
    return ctx


# Convenience alias so individual tests can import a single name.
def make_orders_rows(n: int) -> list[tuple]:
    """Deterministic test data: id, amount, updated_at."""
    return [(i, i * 10.0, f"2026-05-08 {i:02d}:00:00") for i in range(n)]


__all__ = [
    "FakeDbHook",
    "FakeStorageHook",
    "FakeUploader",
    "FakeTI",
    "install_fakes",
    "make_context",
    "make_orders_rows",
]
