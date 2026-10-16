"""Ad-hoc, read-only SQL over the data eyetable has ingested.

The query window on the source management page sends SQL here. Because the
API can be public, a query must never change anything:

  * Only one SELECT-style statement runs (SELECT, WITH, VALUES, TABLE). On
    Postgres it runs through a server-side cursor (DECLARE ... CURSOR FOR),
    which accepts exactly one read-only query; SQLite runs one statement.
  * It runs in a read-only transaction that is always rolled back.
  * On Postgres, point EYETABLE_QUERY_DATABASE_URL at a role that can only
    SELECT from the data schema (scripts/setup-postgres.ps1 creates one).
  * A statement timeout stops runaway queries, and only `limit` rows are
    fetched, so a huge table never lands in memory.

Each file source can be queried by its own name: before the query runs, a
temporary view named after the source (e.g. `parts_list`) is created over its
table (`parts_list__data`). Every table in the data schema is also reachable by
its plain name, since the data schema is the search path.
"""

import re
import sqlite3
import time
from dataclasses import dataclass
from typing import Any

import sqlalchemy as sa
from sqlalchemy.pool import NullPool

from eyetable.store import CoreStore

READ_KEYWORDS = ("select", "with", "values", "table")
MAX_LIMIT = 10_000

_LEADING_NOISE = re.compile(r"^(?:\s+|--[^\n]*(?:\n|$)|/\*.*?\*/|\()*", re.S)


class QueryError(Exception):
    pass


@dataclass
class QueryResult:
    columns: list[str]
    rows: list[list[Any]]
    truncated: bool  # more rows existed beyond the limit
    elapsed_ms: int


def first_keyword(sql: str) -> str:
    match = re.match(r"[A-Za-z]+", _LEADING_NOISE.sub("", sql, count=1))
    return match.group(0).lower() if match else ""


def clean(sql: str) -> str:
    sql = sql.strip()
    while sql.endswith(";"):
        sql = sql[:-1].rstrip()
    return sql


def _message(exc: Exception) -> str:
    text = str(exc).strip() or type(exc).__name__
    return re.sub(r"\n\(Background on this error.*", "", text, flags=re.S)


class QueryRunner:
    def __init__(self, store: CoreStore, url: str | None = None, timeout_s: float = 30):
        self.store = store
        self.dedicated = bool(url)
        # A fresh connection per query: temp views and session settings never leak.
        self.db = sa.create_engine(url, poolclass=NullPool) if url else store.db
        self.timeout_ms = int(timeout_s * 1000)

    def _meta_tables(self) -> set[str]:
        """eyetable's own tables, when they share a schema with the data (SQLite)."""
        if self.store.meta_schema != self.store.data_schema:
            return set()
        return {t.name for t in self.store.metadata.tables.values()}

    def tables(self) -> list[dict[str, Any]]:
        """Every table in the data schema with its columns, for the query window's sidebar."""
        insp = sa.inspect(self.store.db)
        hidden = self._meta_tables()
        out = []
        for name in sorted(insp.get_table_names(schema=self.store.data_schema)):
            if name.endswith("__stg") or name in hidden:
                continue
            cols = insp.get_columns(name, schema=self.store.data_schema)
            out.append({"table": name, "columns": [{"name": c["name"], "type": str(c["type"])} for c in cols]})
        return out

    def run(self, sql: str, limit: int = 1000, aliases: dict[str, str] | None = None) -> QueryResult:
        sql = clean(sql)
        if not sql:
            raise QueryError("write a query first")
        keyword = first_keyword(sql)
        if keyword not in READ_KEYWORDS:
            raise QueryError(
                "only read-only queries can run here: start with SELECT, WITH, VALUES or TABLE"
                + (f" (this one starts with {keyword.upper()})" if keyword else "")
            )
        limit = max(1, min(int(limit), MAX_LIMIT))
        aliases = aliases or {}

        start = time.perf_counter()
        conn = self.db.connect()
        raw = conn.connection.dbapi_connection
        try:
            if self.db.dialect.name == "postgresql":
                columns, rows = self._postgres(raw, sql, limit, aliases)
            elif self.db.dialect.name == "sqlite":
                columns, rows = self._sqlite(raw, sql, limit, aliases)
            else:
                raise QueryError(f"queries are not supported on {self.db.dialect.name}")
        except QueryError:
            raise
        except Exception as exc:
            raise QueryError(_message(exc)) from None
        finally:
            try:
                raw.rollback()
            except Exception:
                pass
            if not self.dedicated:
                conn.invalidate()  # shared pool: drop this connection rather than reuse its session state
            conn.close()
        elapsed = int((time.perf_counter() - start) * 1000)
        return QueryResult(columns, [list(r) for r in rows[:limit]], len(rows) > limit, elapsed)

    def _view_statements(self, aliases: dict[str, str], existing: set[str]) -> list[str]:
        prep = self.db.dialect.identifier_preparer
        return [
            f"CREATE TEMP VIEW {prep.quote(alias)} AS SELECT * FROM {self.store.qualified(table)}"
            for alias, table in sorted(aliases.items())
            if table in existing and alias not in existing  # never shadow a real table
        ]

    def _postgres(self, raw, sql: str, limit: int, aliases: dict[str, str]):
        schema = self.store.data_schema
        prep = self.db.dialect.identifier_preparer
        # The reader role defaults to read-only sessions, which would refuse even the
        # temp views below; they are harmless (session-only, over SELECT-only tables).
        raw.read_only = False
        with raw.cursor() as cur:
            cur.execute(f"SET statement_timeout = {self.timeout_ms}")
            if schema:
                cur.execute(f"SET search_path TO {prep.quote_schema(schema)}")
            cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = %s", (schema or "public",))
            existing = {r[0] for r in cur.fetchall()}
            for statement in self._view_statements(aliases, existing):
                cur.execute(statement)
        raw.commit()
        raw.read_only = True
        with raw.cursor(name="eyetable_query") as cur:  # DECLARE ... CURSOR FOR <one read-only query>
            cur.execute(sql)
            columns = [d.name for d in cur.description or []]
            rows = cur.fetchmany(limit + 1)
        return columns, rows

    def _sqlite(self, raw, sql: str, limit: int, aliases: dict[str, str]):
        cur = raw.cursor()
        try:
            existing = {r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
            for statement in self._view_statements(aliases, existing):
                cur.execute(statement)
            raw.commit()
            cur.execute("PRAGMA query_only = ON")
            deadline = time.perf_counter() + self.timeout_ms / 1000
            raw.set_progress_handler(lambda: time.perf_counter() > deadline, 10_000)
            hidden = self._meta_tables()

            def authorizer(action, arg1, arg2, dbname, source):
                # Postgres keeps eyetable's own tables in another schema the reader role
                # cannot see; in SQLite they sit next to the data, so refuse them here.
                if action == sqlite3.SQLITE_READ and arg1 in hidden:
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

            raw.set_authorizer(authorizer)
            try:
                cur.execute(sql)
                columns = [d[0] for d in cur.description or []]
                rows = cur.fetchmany(limit + 1)
            finally:
                raw.set_authorizer(None)
                raw.set_progress_handler(None, 0)
                cur.execute("PRAGMA query_only = OFF")
        finally:
            cur.close()
        return columns, rows
