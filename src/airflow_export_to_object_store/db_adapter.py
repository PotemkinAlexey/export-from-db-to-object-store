"""Universal DB-API / Airflow connection adapter."""

from __future__ import annotations

import logging

from airflow.hooks.base import BaseHook
from airflow.models import Connection


class UniversalDbAdapter:
    """
    Smart wrapper over any supported Airflow connection type:
    • Postgres
    • Snowflake
    • Databricks SQL
    • Teradata
    • Anything implementing Python DB-API
    """

    def __init__(self, conn_id: str):
        self.conn_id = conn_id
        self.log = logging.getLogger(f"UniversalDbAdapter[{conn_id}]")
        self._hook = None
        self.conn = self._resolve_connection()

    # ------------------------------------------------------------------
    # Resolve connection via hook or manually construct driver
    # ------------------------------------------------------------------
    def _resolve_connection(self):
        try:
            self._hook = BaseHook.get_hook(self.conn_id)
            hook = self._hook
        except Exception as e:
            self.log.warning("Could not load hook for %s: %s", self.conn_id, e)
            hook = None

        if hook and hasattr(hook, "get_conn"):
            try:
                conn = hook.get_conn()
                if hasattr(conn, "cursor") or hasattr(conn, "execute"):
                    self.log.debug("Using connection from Airflow hook: %s", type(conn))
                    return conn
            except Exception as e:
                self.log.warning("Hook.get_conn() failed: %s", e)

        # Fallback — read Airflow connection fields
        airflow_conn: Connection = BaseHook.get_connection(self.conn_id)
        return self._build_manual_conn(airflow_conn)

    # ------------------------------------------------------------------
    # Manual driver construction
    # ------------------------------------------------------------------
    def _build_manual_conn(self, c):
        """
        Construct a real DB connection when hook returned only metadata.
        This method is called when the hook returned only a metadata object
        (e.g. `airflow.models.Connection`), and we must manually construct a
        connection using the driver.

        :param c: The `airflow.models.Connection` object returned by the hook.
        :return: A usable connection object (e.g. `psycopg2.extensions.connection`, etc.)
        :raises: `RuntimeError` if the connection type is unsupported or unconnectable.
        """
        extra = c.extra_dejson or {}
        ctype = (c.conn_type or "").lower()

        try:
            # ----------------------- Databricks -----------------------
            if ctype in ("databricks", "databricks_sql"):
                from databricks import sql as dbsql

                return dbsql.connect(
                    server_hostname=c.host,
                    http_path=extra.get("http_path"),
                    access_token=c.password,
                )

            # ----------------------- Postgres -------------------------
            if ctype in ("postgres", "postgresql"):
                import psycopg2

                return psycopg2.connect(
                    host=c.host,
                    port=c.port or 5432,
                    dbname=c.schema,
                    user=c.login,
                    password=c.password,
                )

            # ----------------------- Snowflake ------------------------
            if ctype == "snowflake":
                import snowflake.connector

                account = extra.get("account") or (c.host.split(".")[0] if c.host else None)
                return snowflake.connector.connect(
                    account=account,
                    user=c.login,
                    password=c.password,
                    database=c.schema,
                    warehouse=extra.get("warehouse"),
                    role=extra.get("role"),
                )

            # ----------------------- Teradata -------------------------
            if ctype in ("teradata", "teradatasql"):
                import teradatasql

                host = c.host or ""
                host_base = host.split("/", 1)[0]
                conn_args = {
                    "host": host_base,
                    "user": c.login,
                    "password": c.password,
                }
                if c.schema:
                    conn_args["database"] = c.schema
                return teradatasql.connect(**conn_args)

        except Exception as e:
            self.log.warning("Manual driver connection failed: %s", e)

        raise RuntimeError(f"Unsupported connection type '{ctype}'")

    # ------------------------------------------------------------------
    # Cursor interface
    # ------------------------------------------------------------------
    def cursor(self):
        # Create cursor once per adapter instance
        if not hasattr(self, "_cursor") or self._cursor is None:
            if hasattr(self.conn, "cursor"):
                self._cursor = self.conn.cursor()
            elif hasattr(self.conn, "execute"):
                self._cursor = self.conn
            else:
                raise RuntimeError(f"Connection {type(self.conn)} has no usable cursor")
        return self._cursor

    # ------------------------------------------------------------------
    # Simple wrappers
    # ------------------------------------------------------------------
    def execute(self, sql, params=None):
        cur = self.cursor()
        return cur.execute(sql, params) if params else cur.execute(sql)

    def fetchmany(self, n: int):
        cur = self.cursor()
        return cur.fetchmany(n) if hasattr(cur, "fetchmany") else []

    def fetchall(self):
        cur = self.cursor()
        return cur.fetchall() if hasattr(cur, "fetchall") else []

    # ------------------------------------------------------------------
    # Safe close
    # ------------------------------------------------------------------
    def close(self):
        """Safely close all resources. Idempotent."""

        # 1. Close cursor
        if hasattr(self, "_cursor") and self._cursor is not None:
            try:
                self._cursor.close()
            except Exception:
                # Log only in debug mode to avoid noise
                self.log.debug("Cursor close failed", exc_info=True)
            finally:
                self._cursor = None

        # 2. Close connection
        if self.conn is not None and hasattr(self.conn, "close"):
            try:
                self.conn.close()
            except Exception:
                self.log.debug("Connection close failed", exc_info=True)
            finally:
                self.conn = None

        # 3. Close hook (if exists)
        if self._hook is not None and hasattr(self._hook, "close"):
            try:
                self._hook.close()
            except Exception:
                self.log.debug("Hook close failed", exc_info=True)
            finally:
                self._hook = None

        # Optional: mark as closed
        self._closed = True
